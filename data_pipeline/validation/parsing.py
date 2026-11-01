"""Shared field parsers. Dataset modules decide which rules apply."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from decimal import Decimal

from ..contracts.exceptions import ParseIssue
from ..contracts.reasons import INVALID_ENUM, INVALID_FORMAT, MISSING_REQUIRED
from ..transformation.timefmt import format_utc

CUSTOMER_ID_RE = re.compile(r"^C[0-9]{6,9}$")
ORDER_ID_RE = re.compile(r"^O[0-9]{6,9}$")
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
MONEY_RE = re.compile(r"^[+-]?\d+(\.\d{1,2})?$")

MAX_NAME_LENGTH = 200
MAX_AMOUNT = Decimal("1000000.00")
MAX_AMOUNT_CENTS = 100_000_000

COUNTRIES = frozenset({"US", "CA", "GB", "IN", "DE", "AU", "FR", "SG"})
CUSTOMER_STATUSES = frozenset({"active", "inactive", "suspended"})
ORDER_STATUSES = frozenset({"placed", "paid", "shipped", "cancelled"})
CURRENCIES = frozenset({"USD"})


def require_text(raw: dict[str, str], field: str) -> str:
    value = raw[field].strip()
    if value == "":
        raise ParseIssue(MISSING_REQUIRED, field)
    return value


def parse_customer_id(value: str) -> str:
    if CUSTOMER_ID_RE.fullmatch(value) is None:
        raise ParseIssue(INVALID_FORMAT, "customer_id must match C followed by 6 to 9 digits")
    return value


def parse_order_id(value: str) -> str:
    if ORDER_ID_RE.fullmatch(value) is None:
        raise ParseIssue(INVALID_FORMAT, "order_id must match O followed by 6 to 9 digits")
    return value


def parse_name(value: str) -> str:
    if any(ord(char) < 32 for char in value):
        raise ParseIssue(INVALID_FORMAT, "full_name contains a control character")
    if not 1 <= len(value) <= MAX_NAME_LENGTH:
        raise ParseIssue(INVALID_FORMAT, f"full_name length must be 1..{MAX_NAME_LENGTH}")
    return value


def parse_email(value: str) -> str:
    normalized = value.lower()
    if EMAIL_RE.fullmatch(normalized) is None:
        raise ParseIssue(INVALID_FORMAT, "email")
    return normalized


def email_domain(email: str) -> str:
    return email.split("@", 1)[1]


def parse_country(value: str) -> str:
    normalized = value.upper()
    if normalized not in COUNTRIES:
        raise ParseIssue(INVALID_ENUM, "country_code not in operating set")
    return normalized


def parse_choice(value: str, *, field: str, allowed: frozenset[str]) -> str:
    if value not in allowed:
        raise ParseIssue(INVALID_ENUM, field)
    return value


def parse_timestamp(value: str, *, field: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ParseIssue(INVALID_FORMAT, f"{field} must be ISO-8601 with a timezone") from exc
    if parsed.tzinfo is None or parsed.tzinfo.utcoffset(parsed) is None:
        raise ParseIssue(INVALID_FORMAT, f"{field} must include a timezone offset")
    return parsed.astimezone(timezone.utc)


def parse_money_cents(value: str) -> int:
    if MONEY_RE.fullmatch(value) is None:
        raise ParseIssue(INVALID_FORMAT, "amount must be a decimal with at most 2 fraction digits")
    quantized = Decimal(value).quantize(Decimal("0.01"))
    cents = int(quantized * 100)
    return cents


def business_hash(values: dict[str, object], columns: tuple[str, ...]) -> str:
    payload = {column: values[column] for column in columns}
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def history_key(prefix: str, business_key: str, version_text: str) -> str:
    return f"{prefix}|{business_key}|{version_text}"


def as_utc_text(value: datetime) -> str:
    return format_utc(value)
