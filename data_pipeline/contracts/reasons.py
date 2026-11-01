"""Stable reason codes stored on quarantined records and failed runs."""

from __future__ import annotations

MALFORMED_INPUT = "malformed_input"
MALFORMED_ROW = "malformed_row"
UNEXPECTED_SCHEMA = "unexpected_schema"
SOURCE_UNREADABLE = "source_unreadable"
MISSING_REQUIRED = "missing_required_field"
INVALID_FORMAT = "invalid_format"
INVALID_ENUM = "invalid_enum"
DUPLICATE_KEY = "duplicate_key_in_batch"
CONFLICTING_VERSION = "conflicting_version"
INVALID_REFERENCE = "invalid_reference"
OUT_OF_ORDER = "out_of_order_event"
BUSINESS_RULE = "business_rule_violation"
REJECT_THRESHOLD = "reject_threshold_exceeded"
