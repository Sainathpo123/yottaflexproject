"""Demo and benchmark entry points."""

from __future__ import annotations

import json
import time
from pathlib import Path

from ..contracts.models import RunReport
from ..extraction.generate import generate
from ..observability.reconcile import build_reconciliation
from .pipeline import run_dataset


def remove_database(db_path: Path) -> None:
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(str(db_path) + suffix)
        candidate.unlink(missing_ok=True)


def run_demo(
    db_path: Path,
    report_path: Path,
    customers_path: Path,
    orders_path: Path,
) -> int:
    """Load the sample files into a fresh database and write the reconciliation."""
    remove_database(db_path)
    reports = [
        run_dataset(db_path, "customers", customers_path),
    ]
    if reports[0].status == "succeeded":
        reports.append(run_dataset(db_path, "orders", orders_path))
    return _write_report(db_path, report_path, reports, expect_sample=True)


def run_benchmark(rows: int, seed: int, output_dir: Path, db_path: Path, report_path: Path) -> int:
    """Generate a seeded dataset, load it into a fresh database, and time the work."""
    remove_database(db_path)
    started = time.perf_counter()
    generated = generate(rows, seed, output_dir)
    generate_seconds = time.perf_counter() - started
    customer_started = time.perf_counter()
    customers = run_dataset(db_path, "customers", output_dir / "customers.csv")
    customer_seconds = time.perf_counter() - customer_started
    order_started = time.perf_counter()
    orders = run_dataset(db_path, "orders", output_dir / "orders.csv")
    order_seconds = time.perf_counter() - order_started
    document = build_reconciliation(db_path, [customers, orders], expect_sample=False)
    payload = {
        "rows_requested": rows,
        "seed": seed,
        "generate_seconds": round(generate_seconds, 3),
        "customers_load_seconds": round(customer_seconds, 3),
        "orders_load_seconds": round(order_seconds, 3),
        "customers_rows_per_second": _rate(customers.input_count, customer_seconds),
        "orders_rows_per_second": _rate(orders.input_count, order_seconds),
        "generated": generated.to_dict(),
        "reconciliation_passed": document["passed"],
        "aggregates": document["aggregates"],
        "runs": document["runs"],
    }
    _dump(report_path, payload)
    return 0 if document["passed"] else 2


def _write_report(
    db_path: Path,
    report_path: Path,
    reports: list[RunReport],
    *,
    expect_sample: bool,
) -> int:
    document = build_reconciliation(db_path, reports, expect_sample=expect_sample)
    _dump(report_path, document)
    return 0 if document["passed"] else 2


def _dump(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _rate(rows: int, seconds: float) -> int:
    if seconds <= 0:
        return rows
    return int(rows / seconds)
