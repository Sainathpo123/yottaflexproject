"""UTC timestamp formatting shared by the pipeline."""

from __future__ import annotations

from datetime import datetime, timezone


def utc_now() -> datetime:
    """Return the current time as an aware UTC datetime."""
    return datetime.now(timezone.utc)


def format_utc(value: datetime) -> str:
    """Format an aware datetime as ISO-8601 UTC."""
    if value.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat()
