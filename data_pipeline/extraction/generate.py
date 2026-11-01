"""Deterministic synthetic customers and orders, plus a fixed defect trailer."""

from __future__ import annotations

import csv
import hashlib
import random
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ..transformation.timefmt import format_utc
from ..validation.registry import CUSTOMERS, ORDERS

MIN_ROWS = 8
MAX_ROWS = 2_000_000
CUSTOMER_TRAILER = 8
ORDER_TRAILER = 10

_COUNTRIES = ("US", "CA", "GB", "IN", "DE", "AU", "FR", "SG")
_CUSTOMER_STATUSES = ("active", "inactive", "suspended")
_NAMES = ("Avery", "Blake", "Casey", "Devon", "Emery", "Finley", "Gray", "Harper")
_ORDER_STATUSES = ("placed", "paid", "shipped", "cancelled")
_IST = timezone(timedelta(hours=5, minutes=30))
_EPOCH = datetime(2024, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True)
class GeneratedFiles:
    customers_path: str
    orders_path: str
    customers_sha256: str
    orders_sha256: str
    customers_data_rows: int
    orders_data_rows: int
    base_rows: int
    seed: int

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def generate(rows: int, seed: int, output_dir: Path) -> GeneratedFiles:
    """Write ``rows`` base records per dataset, then a fixed edge-case trailer.

    ``rows`` must be at least 8. The seed changes order amounts only. Keys,
    defect positions, and the customer file do not depend on the seed.
    """
    if rows < MIN_ROWS or rows > MAX_ROWS:
        raise ValueError(f"rows must be between {MIN_ROWS} and {MAX_ROWS}")
    output_dir.mkdir(parents=True, exist_ok=True)
    customer_rows = _customer_rows(rows)
    order_rows, short_line = _order_rows(rows, seed)
    customers_path = output_dir / "customers.csv"
    orders_path = output_dir / "orders.csv"
    _write_csv(customers_path, CUSTOMERS.columns, customer_rows)
    _write_csv(orders_path, ORDERS.columns, order_rows)
    with orders_path.open("a", encoding="utf-8", newline="") as handle:
        handle.write(short_line)
    return GeneratedFiles(
        customers_path=customers_path.name,
        orders_path=orders_path.name,
        customers_sha256=_sha256(customers_path),
        orders_sha256=_sha256(orders_path),
        customers_data_rows=len(customer_rows),
        orders_data_rows=len(order_rows) + 1,
        base_rows=rows,
        seed=seed,
    )


def _customer_rows(rows: int) -> list[list[str]]:
    base: list[dict[str, object]] = []
    for index in range(rows):
        signup = _EPOCH + timedelta(minutes=index)
        updated = signup + timedelta(days=1)
        if index == 4:
            signup_text = signup.astimezone(_IST).isoformat()
            updated_text = updated.astimezone(_IST).isoformat()
        else:
            signup_text = format_utc(signup)
            updated_text = format_utc(updated)
        country = _COUNTRIES[index % len(_COUNTRIES)]
        if index == 2:
            country = country.lower()
        email = f"user{index + 1}@example.com"
        if index == 1:
            email = f"User{index + 1}@Example.com"
        base.append(
            {
                "signup": signup,
                "updated": updated,
                "cells": [
                    f"C{index + 1:07d}",
                    f"{_NAMES[index % len(_NAMES)]} {index + 1}",
                    email,
                    country,
                    _CUSTOMER_STATUSES[index % len(_CUSTOMER_STATUSES)],
                    signup_text,
                    updated_text,
                ],
            }
        )
    first = base[0]
    second = base[1]
    newer_updated = first["updated"] + timedelta(days=2)
    newer = list(first["cells"])
    newer[1] = "Updated 1"
    newer[6] = format_utc(newer_updated)
    older = list(second["cells"])
    older[6] = str(second["cells"][5])
    missing_email = ["C9000001", "Missing Email", "", "US", "active", format_utc(_EPOCH), format_utc(_EPOCH)]
    bad_country = ["C9000002", "Bad Country", "bad.country@example.com", "ZZ", "active", format_utc(_EPOCH), format_utc(_EPOCH)]
    time_travel = [
        "C9000003",
        "Time Travel",
        "time.travel@example.com",
        "FR",
        "active",
        format_utc(_EPOCH + timedelta(days=10)),
        format_utc(_EPOCH),
    ]
    naive = ["C9000004", "Naive Time", "naive.time@example.com", "AU", "active", "2024-01-01T00:00:00", "2024-01-02T00:00:00"]
    blank_id = ["", "Blank Id", "blank.id@example.com", "SG", "active", format_utc(_EPOCH), format_utc(_EPOCH)]
    records = [list(item["cells"]) for item in base]
    records.extend([newer, list(newer), older, missing_email, bad_country, time_travel, naive, blank_id])
    return records


def _order_rows(rows: int, seed: int) -> tuple[list[list[str]], str]:
    rng = random.Random(seed)
    base: list[dict[str, object]] = []
    for index in range(rows):
        cents = rng.randrange(1, 250_000)
        amount = f"{cents // 100}.{cents % 100:02d}"
        status = _ORDER_STATUSES[index % len(_ORDER_STATUSES)]
        if index == 0:
            status = "placed"
            amount = "0.01"
        elif index == 3:
            status = "cancelled"
            amount = "0.00"
        elif index == rows - 1:
            status = "shipped"
            amount = "999999.99"
        ordered = _EPOCH + timedelta(days=30, minutes=index)
        event = ordered + timedelta(hours=1)
        base.append(
            {
                "ordered": ordered,
                "event": event,
                "amount": amount,
                "cells": [
                    f"O{index + 1:07d}",
                    f"C{index + 1:07d}",
                    status,
                    "USD",
                    amount,
                    format_utc(ordered),
                    format_utc(event),
                ],
            }
        )
    first = base[0]
    newer = list(first["cells"])
    newer[2] = "shipped"
    newer[6] = format_utc(first["event"] + timedelta(days=1))
    older = list(first["cells"])
    older[2] = "paid"
    older[6] = format_utc(first["ordered"])
    bad_fk = ["O9000002", "C9999999", "placed", "USD", "10.00", format_utc(_EPOCH), format_utc(_EPOCH)]
    negative = ["O9000003", "C0000001", "placed", "USD", "-1.00", format_utc(_EPOCH), format_utc(_EPOCH)]
    zero_placed = ["O9000004", "C0000001", "placed", "USD", "0.00", format_utc(_EPOCH), format_utc(_EPOCH)]
    zero_cancelled = ["O9000005", "C0000001", "cancelled", "USD", "0.00", format_utc(_EPOCH), format_utc(_EPOCH)]
    early_event = [
        "O9000006",
        "C0000001",
        "placed",
        "USD",
        "12.50",
        format_utc(_EPOCH + timedelta(days=2)),
        format_utc(_EPOCH + timedelta(days=1)),
    ]
    bad_status = ["O9000007", "C0000001", "refunded", "USD", "5.00", format_utc(_EPOCH), format_utc(_EPOCH)]
    precise = ["O9000008", "C0000001", "placed", "USD", "10.999", format_utc(_EPOCH), format_utc(_EPOCH)]
    records = [list(item["cells"]) for item in base]
    records.extend(
        [newer, older, bad_fk, negative, zero_placed, zero_cancelled, early_event, bad_status, precise]
    )
    return records, "O9000099,C0000001,placed\n"


def _write_csv(path: Path, header: tuple[str, ...], rows: list[list[str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
