"""CSV extraction with a strict header contract and a full-file checksum."""

from __future__ import annotations

import csv
import hashlib
import io
from pathlib import Path

from ..contracts.exceptions import BatchRejected
from ..contracts.models import ExtractedBatch, SourceRow
from ..contracts.reasons import MALFORMED_INPUT, SOURCE_UNREADABLE, UNEXPECTED_SCHEMA


def extract_csv(path: Path, columns: tuple[str, ...]) -> ExtractedBatch:
    source_name = path.name
    try:
        payload = path.read_bytes()
    except FileNotFoundError as exc:
        raise BatchRejected(SOURCE_UNREADABLE, f"file not found: {source_name}") from exc
    except OSError as exc:
        raise BatchRejected(SOURCE_UNREADABLE, f"file could not be read: {source_name}") from exc

    digest = hashlib.sha256(payload).hexdigest()
    try:
        text = payload.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise BatchRejected(
            MALFORMED_INPUT,
            "file is not valid UTF-8",
            sha256=digest,
            input_bytes=len(payload),
        ) from exc
    if text.strip() == "":
        raise BatchRejected(
            MALFORMED_INPUT,
            "file is empty",
            sha256=digest,
            input_bytes=len(payload),
        )

    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration as exc:
        raise BatchRejected(
            MALFORMED_INPUT,
            "file has no header",
            sha256=digest,
            input_bytes=len(payload),
        ) from exc
    except csv.Error as exc:
        raise BatchRejected(
            MALFORMED_INPUT,
            "header row is not valid CSV",
            sha256=digest,
            input_bytes=len(payload),
        ) from exc

    if list(header) != list(columns):
        raise BatchRejected(
            UNEXPECTED_SCHEMA,
            f"expected columns {list(columns)} but found {list(header)}",
            sha256=digest,
            input_bytes=len(payload),
        )

    rows: list[SourceRow] = []
    try:
        for record in reader:
            if record == []:
                rows.append(SourceRow(source_line=reader.line_num, cells=tuple()))
                continue
            rows.append(SourceRow(source_line=reader.line_num, cells=tuple(record)))
    except csv.Error as exc:
        raise BatchRejected(
            MALFORMED_INPUT,
            "file is not valid CSV",
            sha256=digest,
            input_bytes=len(payload),
        ) from exc

    return ExtractedBatch(
        source_name=source_name,
        sha256=digest,
        input_bytes=len(payload),
        rows=tuple(rows),
    )
