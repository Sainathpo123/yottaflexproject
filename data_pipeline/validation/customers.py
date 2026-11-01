"""Customer file contract and row validation."""

from __future__ import annotations

from types import MappingProxyType

from ..contracts.exceptions import ParseIssue
from ..contracts.models import Candidate, RejectDecision, freeze_strings
from ..contracts.reasons import BUSINESS_RULE
from .parsing import (
    CUSTOMER_STATUSES,
    as_utc_text,
    business_hash,
    email_domain,
    parse_choice,
    parse_country,
    parse_customer_id,
    parse_email,
    parse_name,
    parse_timestamp,
    require_text,
)

COLUMNS = (
    "customer_id",
    "full_name",
    "email",
    "country_code",
    "status",
    "signup_at",
    "updated_at",
)
BUSINESS_COLUMNS = (
    "customer_id",
    "full_name",
    "email",
    "email_domain",
    "country_code",
    "status",
    "signup_at",
    "updated_at",
)
KEY_COLUMN = "customer_id"
VERSION_COLUMN = "updated_at"
CURRENT_TABLE = "customers"
HISTORY_TABLE = "customer_history"
HISTORY_PREFIX = "customer"


def parse_row(raw: dict[str, str], source_line: int) -> Candidate | RejectDecision:
    try:
        customer_id = parse_customer_id(require_text(raw, "customer_id"))
        full_name = parse_name(require_text(raw, "full_name"))
        email = parse_email(require_text(raw, "email"))
        country = parse_country(require_text(raw, "country_code"))
        status = parse_choice(
            require_text(raw, "status"),
            field="status",
            allowed=CUSTOMER_STATUSES,
        )
        signup_at = parse_timestamp(require_text(raw, "signup_at"), field="signup_at")
        updated_at = parse_timestamp(require_text(raw, "updated_at"), field="updated_at")
        if updated_at < signup_at:
            raise ParseIssue(BUSINESS_RULE, "updated_at_before_signup_at")
        values = {
            "customer_id": customer_id,
            "full_name": full_name,
            "email": email,
            "email_domain": email_domain(email),
            "country_code": country,
            "status": status,
            "signup_at": as_utc_text(signup_at),
            "updated_at": as_utc_text(updated_at),
        }
        return Candidate(
            key=customer_id,
            version_at=updated_at,
            values=MappingProxyType(values),
            source_line=source_line,
            raw=freeze_strings(raw),
            record_hash=business_hash(values, BUSINESS_COLUMNS),
        )
    except ParseIssue as exc:
        return _reject(raw, source_line, exc.reason_code, exc.detail)


def _reject(raw: dict[str, str], source_line: int, reason_code: str, detail: str) -> RejectDecision:
    key = raw.get("customer_id", "").strip() or None
    return RejectDecision(
        source_line=source_line,
        raw=freeze_strings(raw),
        record_key=key,
        reason_code=reason_code,
        reason_detail=detail,
    )
