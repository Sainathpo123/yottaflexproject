"""SQLite storage. One connection helper, explicit transactions, schema version 1."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from ..contracts.exceptions import InvariantError, StorageError

SCHEMA_VERSION = "1"

SCHEMA_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS schema_meta (
        meta_key TEXT PRIMARY KEY,
        meta_value TEXT NOT NULL
    )
    """,
    """
    INSERT OR IGNORE INTO schema_meta (meta_key, meta_value)
    VALUES ('schema_version', '1')
    """,
    """
    CREATE TABLE IF NOT EXISTS pipeline_runs (
        run_id TEXT PRIMARY KEY,
        dataset TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed', 'interrupted')),
        started_at TEXT NOT NULL,
        ended_at TEXT,
        duration_ms INTEGER,
        source_file TEXT NOT NULL,
        input_sha256 TEXT,
        input_bytes INTEGER,
        input_count INTEGER NOT NULL DEFAULT 0,
        accepted_count INTEGER NOT NULL DEFAULT 0,
        rejected_count INTEGER NOT NULL DEFAULT 0,
        inserted_count INTEGER NOT NULL DEFAULT 0,
        updated_count INTEGER NOT NULL DEFAULT 0,
        unchanged_count INTEGER NOT NULL DEFAULT 0,
        failure_reason TEXT,
        reject_breakdown_json TEXT NOT NULL DEFAULT '{}',
        pipeline_version TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS rejected_records (
        reject_id TEXT PRIMARY KEY,
        run_id TEXT NOT NULL,
        dataset TEXT NOT NULL,
        source_file TEXT NOT NULL,
        source_line INTEGER NOT NULL,
        record_key TEXT,
        reason_code TEXT NOT NULL,
        reason_detail TEXT NOT NULL,
        raw_values_json TEXT NOT NULL,
        rejected_at TEXT NOT NULL,
        FOREIGN KEY (run_id) REFERENCES pipeline_runs (run_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS customers (
        customer_id TEXT PRIMARY KEY,
        full_name TEXT NOT NULL,
        email TEXT NOT NULL,
        email_domain TEXT NOT NULL,
        country_code TEXT NOT NULL CHECK (length(country_code) = 2),
        status TEXT NOT NULL CHECK (status IN ('active', 'inactive', 'suspended')),
        signup_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        record_hash TEXT NOT NULL,
        current_history_id TEXT NOT NULL,
        source_file TEXT NOT NULL,
        source_line INTEGER NOT NULL,
        run_id TEXT NOT NULL,
        loaded_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS customer_history (
        history_id TEXT PRIMARY KEY,
        customer_id TEXT NOT NULL,
        full_name TEXT NOT NULL,
        email TEXT NOT NULL,
        email_domain TEXT NOT NULL,
        country_code TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('active', 'inactive', 'suspended')),
        signup_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        valid_from TEXT NOT NULL,
        valid_to TEXT,
        is_current INTEGER NOT NULL CHECK (is_current IN (0, 1)),
        record_hash TEXT NOT NULL,
        source_file TEXT NOT NULL,
        source_line INTEGER NOT NULL,
        run_id TEXT NOT NULL,
        CHECK (
            (is_current = 1 AND valid_to IS NULL)
            OR (is_current = 0 AND valid_to IS NOT NULL)
        )
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_customer_history_current
    ON customer_history (customer_id)
    WHERE is_current = 1
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_customer_history_customer
    ON customer_history (customer_id, valid_from)
    """,
    """
    CREATE TABLE IF NOT EXISTS orders (
        order_id TEXT PRIMARY KEY,
        customer_id TEXT NOT NULL,
        order_status TEXT NOT NULL CHECK (order_status IN ('placed', 'paid', 'shipped', 'cancelled')),
        currency TEXT NOT NULL CHECK (currency = 'USD'),
        amount_cents INTEGER NOT NULL CHECK (amount_cents >= 0 AND amount_cents <= 100000000),
        ordered_at TEXT NOT NULL,
        event_at TEXT NOT NULL,
        record_hash TEXT NOT NULL,
        current_history_id TEXT NOT NULL,
        source_file TEXT NOT NULL,
        source_line INTEGER NOT NULL,
        run_id TEXT NOT NULL,
        loaded_at TEXT NOT NULL,
        FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS order_history (
        history_id TEXT PRIMARY KEY,
        order_id TEXT NOT NULL,
        customer_id TEXT NOT NULL,
        order_status TEXT NOT NULL CHECK (order_status IN ('placed', 'paid', 'shipped', 'cancelled')),
        currency TEXT NOT NULL CHECK (currency = 'USD'),
        amount_cents INTEGER NOT NULL CHECK (amount_cents >= 0 AND amount_cents <= 100000000),
        ordered_at TEXT NOT NULL,
        event_at TEXT NOT NULL,
        valid_from TEXT NOT NULL,
        valid_to TEXT,
        is_current INTEGER NOT NULL CHECK (is_current IN (0, 1)),
        record_hash TEXT NOT NULL,
        source_file TEXT NOT NULL,
        source_line INTEGER NOT NULL,
        run_id TEXT NOT NULL,
        CHECK (
            (is_current = 1 AND valid_to IS NULL)
            OR (is_current = 0 AND valid_to IS NOT NULL)
        )
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_order_history_current
    ON order_history (order_id)
    WHERE is_current = 1
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_orders_customer ON orders (customer_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_order_history_order ON order_history (order_id, valid_from)
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_rejected_records_run ON rejected_records (run_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_pipeline_runs_status ON pipeline_runs (status)
    """,
)


def connect(db_path: Path) -> sqlite3.Connection:
    """Open a database in autocommit mode so transactions are explicit."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 5000")
    return connection


def ensure_schema(db_path: Path) -> None:
    connection = connect(db_path)
    try:
        for statement in SCHEMA_STATEMENTS:
            connection.execute(statement)
        row = connection.execute(
            "SELECT meta_value FROM schema_meta WHERE meta_key = 'schema_version'"
        ).fetchone()
        if row is None or row[0] != SCHEMA_VERSION:
            found = None if row is None else row[0]
            raise StorageError(
                f"database schema version is {found}, this pipeline requires {SCHEMA_VERSION}"
            )
    finally:
        connection.close()


def mark_interrupted(db_path: Path, ended_at: str) -> int:
    connection = connect(db_path)
    try:
        cursor = connection.execute(
            """
            UPDATE pipeline_runs
            SET status = 'interrupted',
                ended_at = ?,
                failure_reason = 'process ended before the run was finalized'
            WHERE status = 'running'
            """,
            (ended_at,),
        )
        return int(cursor.rowcount)
    finally:
        connection.close()


def start_run(
    db_path: Path,
    *,
    run_id: str,
    dataset: str,
    started_at: str,
    source_file: str,
    input_sha256: str | None,
    input_bytes: int | None,
    pipeline_version: str,
) -> None:
    connection = connect(db_path)
    try:
        connection.execute(
            """
            INSERT INTO pipeline_runs (
                run_id, dataset, status, started_at, source_file,
                input_sha256, input_bytes, pipeline_version
            ) VALUES (?, ?, 'running', ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                dataset,
                started_at,
                source_file,
                input_sha256,
                input_bytes,
                pipeline_version,
            ),
        )
    finally:
        connection.close()


def mark_succeeded(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    ended_at: str,
    duration_ms: int,
    input_count: int,
    accepted_count: int,
    rejected_count: int,
    inserted_count: int,
    updated_count: int,
    unchanged_count: int,
    reject_breakdown_json: str,
) -> None:
    cursor = connection.execute(
        """
        UPDATE pipeline_runs
        SET status = 'succeeded',
            ended_at = ?,
            duration_ms = ?,
            input_count = ?,
            accepted_count = ?,
            rejected_count = ?,
            inserted_count = ?,
            updated_count = ?,
            unchanged_count = ?,
            reject_breakdown_json = ?,
            failure_reason = NULL
        WHERE run_id = ? AND status = 'running'
        """,
        (
            ended_at,
            duration_ms,
            input_count,
            accepted_count,
            rejected_count,
            inserted_count,
            updated_count,
            unchanged_count,
            reject_breakdown_json,
            run_id,
        ),
    )
    if cursor.rowcount != 1:
        raise InvariantError(f"could not finalize run {run_id}")


def mark_failed(
    db_path: Path,
    *,
    run_id: str,
    ended_at: str,
    duration_ms: int,
    failure_reason: str,
) -> None:
    connection = connect(db_path)
    try:
        connection.execute(
            """
            UPDATE pipeline_runs
            SET status = 'failed',
                ended_at = ?,
                duration_ms = ?,
                failure_reason = ?
            WHERE run_id = ? AND status = 'running'
            """,
            (ended_at, duration_ms, failure_reason, run_id),
        )
    finally:
        connection.close()


class Transaction:
    """Write transaction that commits on a clean exit and rolls back otherwise."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.connection: sqlite3.Connection | None = None

    def __enter__(self) -> sqlite3.Connection:
        self.connection = connect(self.db_path)
        self.connection.execute("BEGIN IMMEDIATE")
        return self.connection

    def __exit__(self, exc_type, exc, traceback) -> None:
        connection = self.connection
        self.connection = None
        if connection is None:
            return
        try:
            if exc_type is None:
                connection.execute("COMMIT")
            else:
                connection.execute("ROLLBACK")
        finally:
            connection.close()
