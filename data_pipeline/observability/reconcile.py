"""Reconciliation and structural controls over a published database."""

from __future__ import annotations

import json
from pathlib import Path

from ..contracts.models import RunReport
from ..load.storage import connect

# Independent of the generator. These describe the hand-written sample only.
SAMPLE_CUSTOMERS = 5
SAMPLE_ORDERS = 5
SAMPLE_ORDER_AMOUNT_CENTS = 100_006_799
SAMPLE_ORDER_CUSTOMERS = 5
_ROW_LIMIT = 100
_REJECT_LIMIT = 500


def build_reconciliation(
    db_path: Path,
    reports: list[RunReport],
    *,
    expect_sample: bool,
) -> dict[str, object]:
    connection = connect(db_path)
    try:
        checks = _structural_checks(connection)
        checks.extend(_run_checks(connection))
        if expect_sample:
            checks.extend(_sample_checks(connection))
        aggregates = _aggregates(connection)
        targets = {
            "customers": _maybe_rows(connection, "customers", "customer_id"),
            "orders": _maybe_rows(connection, "orders", "order_id"),
        }
        rejected = _maybe_rejects(connection)
    finally:
        connection.close()
    passed = all(check["passed"] for check in checks) and all(
        report.status == "succeeded" for report in reports
    )
    return {
        "passed": passed,
        "runs": [report.to_dict() for report in reports],
        "checks": checks,
        "aggregates": aggregates,
        "targets": targets,
        "rejected_records": rejected,
    }


def _structural_checks(connection) -> list[dict[str, object]]:
    checks = [
        _check(
            "no_orphan_orders",
            _count(
                connection,
                """
                SELECT COUNT(*) FROM orders AS o
                LEFT JOIN customers AS c ON c.customer_id = o.customer_id
                WHERE c.customer_id IS NULL
                """,
            )
            == 0,
            "every order references a published customer",
        ),
        _check(
            "foreign_key_check",
            connection.execute("PRAGMA foreign_key_check").fetchall() == [],
            "SQLite foreign_key_check returns no violations",
        ),
        _check(
            "customer_current_matches_history",
            _count(
                connection,
                """
                SELECT COUNT(*) FROM customers AS c
                LEFT JOIN customer_history AS h ON h.history_id = c.current_history_id
                WHERE h.history_id IS NULL
                   OR h.is_current != 1
                   OR h.record_hash != c.record_hash
                   OR h.customer_id != c.customer_id
                   OR h.valid_from != c.updated_at
                """,
            )
            == 0,
            "each current customer points at its open history version",
        ),
        _check(
            "order_current_matches_history",
            _count(
                connection,
                """
                SELECT COUNT(*) FROM orders AS o
                LEFT JOIN order_history AS h ON h.history_id = o.current_history_id
                WHERE h.history_id IS NULL
                   OR h.is_current != 1
                   OR h.record_hash != o.record_hash
                   OR h.order_id != o.order_id
                   OR h.valid_from != o.event_at
                """,
            )
            == 0,
            "each current order points at its open history version",
        ),
        _check(
            "one_open_customer_version",
            _count(
                connection,
                """
                SELECT COUNT(*) FROM (
                    SELECT customer_id FROM customer_history
                    WHERE is_current = 1
                    GROUP BY customer_id
                    HAVING COUNT(*) > 1
                )
                """,
            )
            == 0,
            "a customer has at most one open history row",
        ),
        _check(
            "one_open_order_version",
            _count(
                connection,
                """
                SELECT COUNT(*) FROM (
                    SELECT order_id FROM order_history
                    WHERE is_current = 1
                    GROUP BY order_id
                    HAVING COUNT(*) > 1
                )
                """,
            )
            == 0,
            "an order has at most one open history row",
        ),
        _check(
            "customer_history_bounds",
            _count(
                connection,
                """
                SELECT COUNT(*) FROM customer_history
                WHERE valid_from != updated_at
                   OR (is_current = 1 AND valid_to IS NOT NULL)
                   OR (is_current = 0 AND valid_to IS NULL)
                """,
            )
            == 0,
            "customer history uses a half-open validity window",
        ),
        _check(
            "order_history_bounds",
            _count(
                connection,
                """
                SELECT COUNT(*) FROM order_history
                WHERE valid_from != event_at
                   OR (is_current = 1 AND valid_to IS NOT NULL)
                   OR (is_current = 0 AND valid_to IS NULL)
                """,
            )
            == 0,
            "order history uses a half-open validity window",
        ),
    ]
    return checks


def _run_checks(connection) -> list[dict[str, object]]:
    mismatched = 0
    unbalanced = 0
    for row in connection.execute("SELECT * FROM pipeline_runs WHERE status = 'succeeded'"):
        reject_rows = _count(
            connection,
            "SELECT COUNT(*) FROM rejected_records WHERE run_id = ?",
            (row["run_id"],),
        )
        if reject_rows != row["rejected_count"]:
            mismatched += 1
        if row["input_count"] != row["accepted_count"] + row["rejected_count"]:
            unbalanced += 1
        if row["accepted_count"] != (
            row["inserted_count"] + row["updated_count"] + row["unchanged_count"]
        ):
            unbalanced += 1
    unpublished = _count(
        connection,
        """
        SELECT COUNT(*) FROM rejected_records AS r
        JOIN pipeline_runs AS p ON p.run_id = r.run_id
        WHERE p.status != 'succeeded'
        """,
    )
    return [
        _check(
            "succeeded_runs_balance",
            unbalanced == 0,
            "every succeeded run satisfies input = accepted + rejected",
        ),
        _check(
            "succeeded_reject_rows_match",
            mismatched == 0,
            "quarantine rows match the run ledger",
        ),
        _check(
            "failed_runs_publish_nothing",
            unpublished == 0,
            "a rolled-back run leaves no quarantine rows",
        ),
    ]


def _sample_checks(connection) -> list[dict[str, object]]:
    customers = _count(connection, "SELECT COUNT(*) FROM customers")
    orders = _count(connection, "SELECT COUNT(*) FROM orders")
    customer_history = _count(connection, "SELECT COUNT(*) FROM customer_history")
    order_history = _count(connection, "SELECT COUNT(*) FROM order_history")
    amount = _count(connection, "SELECT COALESCE(SUM(amount_cents), 0) FROM orders")
    distinct_customers = _count(connection, "SELECT COUNT(DISTINCT customer_id) FROM orders")
    alice = connection.execute(
        "SELECT full_name, email FROM customers WHERE customer_id = 'C000101'"
    ).fetchone()
    paid = connection.execute(
        "SELECT order_status FROM orders WHERE order_id = 'O000201'"
    ).fetchone()
    return [
        _check("sample_customer_count", customers == SAMPLE_CUSTOMERS, f"customers={customers}"),
        _check("sample_order_count", orders == SAMPLE_ORDERS, f"orders={orders}"),
        _check(
            "sample_history_is_one_version",
            customer_history == SAMPLE_CUSTOMERS and order_history == SAMPLE_ORDERS,
            f"customer_history={customer_history} order_history={order_history}",
        ),
        _check(
            "sample_order_amount_cents",
            amount == SAMPLE_ORDER_AMOUNT_CENTS,
            f"amount_cents={amount}",
        ),
        _check(
            "sample_distinct_order_customers",
            distinct_customers == SAMPLE_ORDER_CUSTOMERS,
            f"distinct_customers={distinct_customers}",
        ),
        _check(
            "sample_latest_customer_version",
            alice is not None and alice["full_name"] == "Alice Duplicate" and alice["email"] == "alice@example.com",
            "C000101 keeps the later in-file version",
        ),
        _check(
            "sample_latest_order_version",
            paid is not None and paid["order_status"] == "paid",
            "O000201 keeps the later in-file event",
        ),
    ]


def _aggregates(connection) -> dict[str, object]:
    order_totals = connection.execute(
        """
        SELECT COUNT(*) AS orders,
               COUNT(DISTINCT customer_id) AS customers,
               COALESCE(SUM(amount_cents), 0) AS amount_cents
        FROM orders
        """
    ).fetchone()
    by_status = [
        dict(row)
        for row in connection.execute(
            """
            SELECT order_status, COUNT(*) AS orders, COALESCE(SUM(amount_cents), 0) AS amount_cents
            FROM orders
            GROUP BY order_status
            ORDER BY order_status
            """
        )
    ]
    by_country = [
        dict(row)
        for row in connection.execute(
            """
            SELECT country_code, COUNT(*) AS customers
            FROM customers
            GROUP BY country_code
            ORDER BY country_code
            """
        )
    ]
    by_reason = [
        dict(row)
        for row in connection.execute(
            """
            SELECT dataset, reason_code, COUNT(*) AS rejected
            FROM rejected_records
            GROUP BY dataset, reason_code
            ORDER BY dataset, reason_code
            """
        )
    ]
    return {
        "customers": _count(connection, "SELECT COUNT(*) FROM customers"),
        "customer_history": _count(connection, "SELECT COUNT(*) FROM customer_history"),
        "orders": int(order_totals["orders"]),
        "order_customers": int(order_totals["customers"]),
        "order_amount_cents": int(order_totals["amount_cents"]),
        "order_history": _count(connection, "SELECT COUNT(*) FROM order_history"),
        "orders_by_status": by_status,
        "customers_by_country": by_country,
        "rejects_by_reason": by_reason,
    }


def _maybe_rows(connection, table: str, key: str) -> dict[str, object]:
    count = _count(connection, f"SELECT COUNT(*) FROM {table}")
    if count > _ROW_LIMIT:
        return {"omitted": True, "row_count": count}
    rows = [
        dict(row)
        for row in connection.execute(f"SELECT * FROM {table} ORDER BY {key}")
    ]
    return {"omitted": False, "row_count": count, "rows": rows}


def _maybe_rejects(connection) -> dict[str, object]:
    count = _count(connection, "SELECT COUNT(*) FROM rejected_records")
    if count > _REJECT_LIMIT:
        return {"omitted": True, "row_count": count}
    rows = [
        {
            "dataset": row["dataset"],
            "source_file": row["source_file"],
            "source_line": row["source_line"],
            "record_key": row["record_key"],
            "reason_code": row["reason_code"],
            "reason_detail": row["reason_detail"],
            "raw_values": json.loads(row["raw_values_json"]),
        }
        for row in connection.execute(
            """
            SELECT dataset, source_file, source_line, record_key, reason_code,
                   reason_detail, raw_values_json
            FROM rejected_records
            ORDER BY dataset, source_line, reason_code
            """
        )
    ]
    return {"omitted": False, "row_count": count, "rows": rows}


def _count(connection, sql: str, params: tuple = ()) -> int:
    return int(connection.execute(sql, params).fetchone()[0])


def _check(name: str, passed: bool, detail: str) -> dict[str, object]:
    return {"name": name, "passed": passed, "detail": detail}
