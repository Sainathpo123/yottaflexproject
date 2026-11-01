"""Structured JSON logs for pipeline runs."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from ..contracts.models import RunReport

_CONFIGURED = False


class JsonFormatter(logging.Formatter):
    """One JSON object per line, safe for a log collector."""

    _FIELDS = ("run_id", "dataset", "status")

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key in self._FIELDS:
            value = getattr(record, key, None)
            if value is not None:
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=True)


def configure_logging(level: int = logging.INFO) -> None:
    global _CONFIGURED
    logger = logging.getLogger("yottaflex")
    logger.setLevel(level)
    if _CONFIGURED:
        return
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.propagate = False
    _CONFIGURED = True


def log_report(report: RunReport) -> None:
    logger = logging.getLogger("yottaflex.pipeline")
    logger.info(
        "status=%s input=%s accepted=%s rejected=%s inserted=%s updated=%s unchanged=%s",
        report.status,
        report.input_count,
        report.accepted_count,
        report.rejected_count,
        report.inserted_count,
        report.updated_count,
        report.unchanged_count,
        extra={
            "run_id": report.run_id,
            "dataset": report.dataset,
            "status": report.status,
        },
    )
