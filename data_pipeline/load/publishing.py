"""Versioned publish: current row plus append-only history, batched inserts."""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime

from ..contracts.exceptions import InvariantError
from ..contracts.models import Candidate, PublishStats, RejectDecision, freeze_strings
from ..contracts.reasons import CONFLICTING_VERSION, OUT_OF_ORDER
from ..transformation.timefmt import format_utc
from ..validation.parsing import history_key
from ..validation.registry import DatasetSpec

_IDENT = re.compile(r"^[a-z_]+$")
_INSERT_CHUNK = 5000
_VERIFY_CHUNK = 400


def ident(name: str) -> str:
    if _IDENT.fullmatch(name) is None:
        raise InvariantError(f"refusing to interpolate identifier {name!r}")
    return name


@dataclass(frozen=True)
class CurrentVersion:
    version_at: datetime
    record_hash: str
    history_id: str


def publish_versions(
    connection: sqlite3.Connection,
    dataset: DatasetSpec,
    candidates: list[Candidate],
    *,
    source_file: str,
    run_id: str,
    loaded_at: str,
) -> PublishStats:
    """Apply a unique-key batch. Callers must already have collapsed the file."""
    stats = PublishStats()
    if not candidates:
        return stats

    existing = _load_current(connection, dataset)
    inserts: list[Candidate] = []
    updates: list[tuple[Candidate, CurrentVersion]] = []

    for candidate in candidates:
        current = existing.get(candidate.key)
        if current is None:
            inserts.append(candidate)
            continue
        if current.record_hash == candidate.record_hash:
            stats.unchanged += 1
            stats.accepted_lines.append(candidate.source_line)
            continue
        if candidate.version_at < current.version_at:
            stats.rejects.append(
                _outcome(
                    candidate,
                    OUT_OF_ORDER,
                    "incoming_version_older_than_current",
                )
            )
            continue
        if candidate.version_at == current.version_at:
            stats.rejects.append(
                _outcome(
                    candidate,
                    CONFLICTING_VERSION,
                    "same_version_timestamp_different_payload",
                )
            )
            continue
        updates.append((candidate, current))

    _apply_updates(connection, dataset, updates, source_file, run_id, loaded_at)
    _apply_inserts(connection, dataset, inserts, source_file, run_id, loaded_at)
    stats.inserted = len(inserts)
    stats.updated = len(updates)
    stats.accepted_lines.extend(candidate.source_line for candidate in inserts)
    stats.accepted_lines.extend(candidate.source_line for candidate, _current in updates)
    return stats


def _outcome(candidate: Candidate, reason_code: str, detail: str) -> RejectDecision:
    return RejectDecision(
        source_line=candidate.source_line,
        raw=freeze_strings(candidate.raw),
        record_key=candidate.key,
        reason_code=reason_code,
        reason_detail=detail,
    )


def _load_current(
    connection: sqlite3.Connection,
    dataset: DatasetSpec,
) -> dict[str, CurrentVersion]:
    table = ident(dataset.current_table)
    key = ident(dataset.key_column)
    version = ident(dataset.version_column)
    found: dict[str, CurrentVersion] = {}
    rows = connection.execute(
        f"SELECT {key}, {version}, record_hash, current_history_id FROM {table}"
    )
    for row in rows:
        found[row[0]] = CurrentVersion(
            version_at=datetime.fromisoformat(row[1]),
            record_hash=row[2],
            history_id=row[3],
        )
    return found


def _history_id(dataset: DatasetSpec, candidate: Candidate) -> str:
    version_text = str(candidate.values[dataset.version_column])
    return history_key(dataset.history_prefix, candidate.key, version_text)


def _apply_inserts(
    connection: sqlite3.Connection,
    dataset: DatasetSpec,
    candidates: list[Candidate],
    source_file: str,
    run_id: str,
    loaded_at: str,
) -> None:
    if not candidates:
        return
    current_sql = _insert_sql(
        dataset.current_table,
        dataset.business_columns
        + ("record_hash", "current_history_id", "source_file", "source_line", "run_id", "loaded_at"),
    )
    history_sql = _insert_sql(dataset.history_table, _history_columns(dataset))
    current_rows = [
        _current_tuple(dataset, candidate, source_file, run_id, loaded_at) for candidate in candidates
    ]
    history_rows = [
        _history_tuple(dataset, candidate, source_file, run_id, valid_to=None, is_current=1)
        for candidate in candidates
    ]
    _executemany(connection, history_sql, history_rows)
    _executemany(connection, current_sql, current_rows)


def _apply_updates(
    connection: sqlite3.Connection,
    dataset: DatasetSpec,
    updates: list[tuple[Candidate, CurrentVersion]],
    source_file: str,
    run_id: str,
    loaded_at: str,
) -> None:
    if not updates:
        return
    history_table = ident(dataset.history_table)
    close_sql = (
        f"UPDATE {history_table} SET is_current = 0, valid_to = ? "
        "WHERE history_id = ? AND is_current = 1"
    )
    close_params = [
        (str(candidate.values[dataset.version_column]), previous.history_id)
        for candidate, previous in updates
    ]
    _executemany(connection, close_sql, close_params)
    _verify_closed(connection, dataset, [previous.history_id for _candidate, previous in updates])

    history_sql = _insert_sql(dataset.history_table, _history_columns(dataset))
    history_rows = [
        _history_tuple(dataset, candidate, source_file, run_id, valid_to=None, is_current=1)
        for candidate, _previous in updates
    ]
    _executemany(connection, history_sql, history_rows)

    assignments = [column for column in dataset.business_columns if column != dataset.key_column]
    assignments.extend(
        ["record_hash", "current_history_id", "source_file", "source_line", "run_id", "loaded_at"]
    )
    set_clause = ", ".join(f"{ident(column)} = ?" for column in assignments)
    update_sql = (
        f"UPDATE {ident(dataset.current_table)} SET {set_clause} "
        f"WHERE {ident(dataset.key_column)} = ?"
    )
    update_rows = []
    for candidate, _previous in updates:
        values = [candidate.values[column] for column in dataset.business_columns if column != dataset.key_column]
        values.extend(
            [
                candidate.record_hash,
                _history_id(dataset, candidate),
                source_file,
                candidate.source_line,
                run_id,
                loaded_at,
                candidate.key,
            ]
        )
        update_rows.append(tuple(values))
    _executemany(connection, update_sql, update_rows)


def _verify_closed(connection: sqlite3.Connection, dataset: DatasetSpec, history_ids: list[str]) -> None:
    table = ident(dataset.history_table)
    confirmed = 0
    for offset in range(0, len(history_ids), _VERIFY_CHUNK):
        chunk = history_ids[offset : offset + _VERIFY_CHUNK]
        placeholders = ", ".join("?" for _ in chunk)
        row = connection.execute(
            f"""
            SELECT COUNT(*) FROM {table}
            WHERE is_current = 0 AND valid_to IS NOT NULL AND history_id IN ({placeholders})
            """,
            chunk,
        ).fetchone()
        confirmed += int(row[0])
    if confirmed != len(history_ids):
        raise InvariantError("failed to close the previous history version")


def _history_columns(dataset: DatasetSpec) -> tuple[str, ...]:
    return (
        ("history_id",)
        + dataset.business_columns
        + (
            "valid_from",
            "valid_to",
            "is_current",
            "record_hash",
            "source_file",
            "source_line",
            "run_id",
        )
    )


def _current_tuple(
    dataset: DatasetSpec,
    candidate: Candidate,
    source_file: str,
    run_id: str,
    loaded_at: str,
) -> tuple[object, ...]:
    return tuple(candidate.values[column] for column in dataset.business_columns) + (
        candidate.record_hash,
        _history_id(dataset, candidate),
        source_file,
        candidate.source_line,
        run_id,
        loaded_at,
    )


def _history_tuple(
    dataset: DatasetSpec,
    candidate: Candidate,
    source_file: str,
    run_id: str,
    *,
    valid_to: str | None,
    is_current: int,
) -> tuple[object, ...]:
    version_text = str(candidate.values[dataset.version_column])
    return (
        (
            _history_id(dataset, candidate),
        )
        + tuple(candidate.values[column] for column in dataset.business_columns)
        + (
            version_text,
            valid_to,
            is_current,
            candidate.record_hash,
            source_file,
            candidate.source_line,
            run_id,
        )
    )


def _insert_sql(table: str, columns: tuple[str, ...]) -> str:
    names = ", ".join(ident(column) for column in columns)
    placeholders = ", ".join("?" for _ in columns)
    return f"INSERT INTO {ident(table)} ({names}) VALUES ({placeholders})"


def _executemany(connection: sqlite3.Connection, sql: str, rows: list[tuple[object, ...]]) -> None:
    for offset in range(0, len(rows), _INSERT_CHUNK):
        connection.executemany(sql, rows[offset : offset + _INSERT_CHUNK])


def format_loaded_at(value: datetime) -> str:
    return format_utc(value)
