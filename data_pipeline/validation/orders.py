"""Order file contract, row validation, and customer reference checks."""

from __future__ import annotations

from types import MappingProxyType

import sqlite3

from ..contracts.exceptions import ParseIssue
from ..contracts.models import Candidate, RejectDecision, freeze_strings
from ..contracts.reasons import BUSINESS_RULE, INVALID_REFERENCE
from .parsing import (
    CURRENCIES,
    MAX_AMOUNT_CENTS,
    ORDER_STATUSES,
    as_utc_text,
    business_hash,
    parse_choice,
    parse_customer_id,
    parse_money_cents,
    parse_order_id,
    parse_timestamp,
    require_text,
)

COLUMNS = (
    "order_id",
    "customer_id",
    "order_status",
    "currency",
    "amount",
    "ordered_at",
    "event_at",
)
BUSINESS_COLUMNS = (
    "order_id",
    "customer_id",
    "order_status",
    "currency",
    "amount_cents",
    "ordered_at",
    "event_at",
)
KEY_COLUMN = "order_id"
VERSION_COLUMN = "event_at"
CURRENT_TABLE = "orders"
HISTORY_TABLE = "order_history"
HISTORY_PREFIX = "order"


def parse_row(raw: dict[str, str], source_line: int) -> Candidate | RejectDecision:
    try:
        order_id = parse_order_id(require_text(raw, "order_id"))
        customer_id = parse_customer_id(require_text(raw, "customer_id"))
        status = parse_choice(
            require_text(raw, "order_status"),
            field="order_status",
            allowed=ORDER_STATUSES,
        )
        currency = parse_choice(
            require_text(raw, "currency").upper(),
            field="currency",
            allowed=CURRENCIES,
        )
        amount_text = require_text(raw, "amount")
        cents = parse_money_cents(amount_text)
        if cents < 0:
            raise ParseIssue(BUSINESS_RULE, "amount_negative")
        if cents == 0 and status != "cancelled":
            raise ParseIssue(BUSINESS_RULE, "amount_zero_requires_cancelled")
        if cents > MAX_AMOUNT_CENTS:
            raise ParseIssue(BUSINESS_RULE, "amount_above_maximum")
        ordered_at = parse_timestamp(require_text(raw, "ordered_at"), field="ordered_at")
        event_at = parse_timestamp(require_text(raw, "event_at"), field="event_at")
        if event_at < ordered_at:
            raise ParseIssue(BUSINESS_RULE, "event_at_before_ordered_at")
        values: dict[str, object] = {
            "order_id": order_id,
            "customer_id": customer_id,
            "order_status": status,
            "currency": currency,
            "amount_cents": cents,
            "ordered_at": as_utc_text(ordered_at),
            "event_at": as_utc_text(event_at),
        }
        return Candidate(
            key=order_id,
            version_at=event_at,
            values=MappingProxyType(values),
            source_line=source_line,
            raw=freeze_strings(raw),
            record_hash=business_hash(values, BUSINESS_COLUMNS),
        )
    except ParseIssue as exc:
        return _reject(raw, source_line, exc.reason_code, exc.detail)


def screen_references(
    connection: sqlite3.Connection,
    candidates: list[Candidate],
) -> tuple[list[Candidate], list[RejectDecision]]:
    """Drop orders whose customer is not already published."""
    known = {
        row[0]
        for row in connection.execute("SELECT customer_id FROM customers")
    }
    kept: list[Candidate] = []
    rejects: list[RejectDecision] = []
    for candidate in candidates:
        customer_id = str(candidate.values["customer_id"])
        if customer_id in known:
            kept.append(candidate)
            continue
        rejects.append(
            RejectDecision(
                source_line=candidate.source_line,
                raw=freeze_strings(candidate.raw),
                record_key=candidate.key,
                reason_code=INVALID_REFERENCE,
                reason_detail=f"customer_id not found: {customer_id}",
            )
        )
    return kept, rejects


def _reject(raw: dict[str, str], source_line: int, reason_code: str, detail: str) -> RejectDecision:
    key = raw.get("order_id", "").strip() or None
    return RejectDecision(
        source_line=source_line,
        raw=freeze_strings(raw),
        record_key=key,
        reason_code=reason_code,
        reason_detail=detail,
    )
