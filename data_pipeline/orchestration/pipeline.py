"""Orchestrate extract, validate, publish, quarantine, and the run ledger."""

from __future__ import annotations

import json
import uuid
from pathlib import Path

from ..contracts.exceptions import BatchRejected, InvariantError
from ..contracts.models import ExtractedBatch, RejectDecision, RunReport, freeze_strings
from ..contracts.reasons import MALFORMED_ROW, REJECT_THRESHOLD
from ..extraction.extract import extract_csv
from ..load.publishing import publish_versions
from ..load.storage import Transaction, ensure_schema, mark_failed, mark_interrupted, mark_succeeded, start_run
from ..observability.logs import log_report
from ..transformation.timefmt import format_utc, utc_now
from ..validation.collapse import collapse
from ..validation.registry import DatasetSpec, get_dataset
from ..version import __version__


def run_dataset(
    db_path: Path | str,
    dataset_name: str,
    input_path: Path | str,
    *,
    max_reject_rate: float | None = None,
) -> RunReport:
    """Load one CSV into the local database.

    Row-level problems are quarantined and the rest of the file is published.
    File-level problems and a reject rate above ``max_reject_rate`` roll back
    the whole attempt. ``max_reject_rate`` is a fraction from 0 to 1; 0 fails
    the batch when any row is rejected.
    """
    dataset = get_dataset(dataset_name)
    database = Path(db_path)
    source = Path(input_path)
    ensure_schema(database)
    run_id = str(uuid.uuid4())
    started = utc_now()
    try:
        extracted = extract_csv(source, dataset.columns)
    except BatchRejected as exc:
        return _fail_before_publish(
            database,
            dataset_name=dataset.name,
            run_id=run_id,
            started=started,
            source_file=source.name,
            sha256=exc.sha256,
            input_bytes=exc.input_bytes,
            reason=str(exc),
        )

    _open_run(
        database,
        run_id=run_id,
        dataset_name=dataset.name,
        started_at=format_utc(started),
        source_file=extracted.source_name,
        sha256=extracted.sha256,
        input_bytes=extracted.input_bytes,
    )
    try:
        report = _publish(
            database,
            dataset,
            extracted,
            run_id=run_id,
            started=started,
            max_reject_rate=max_reject_rate,
        )
    except BatchRejected as exc:
        return _fail_open_run(
            database,
            dataset_name=dataset.name,
            run_id=run_id,
            started=started,
            source_file=extracted.source_name,
            sha256=extracted.sha256,
            input_bytes=extracted.input_bytes,
            input_count=len(extracted.rows),
            reason=str(exc),
        )
    except Exception as exc:
        _fail_open_run(
            database,
            dataset_name=dataset.name,
            run_id=run_id,
            started=started,
            source_file=extracted.source_name,
            sha256=extracted.sha256,
            input_bytes=extracted.input_bytes,
            input_count=len(extracted.rows),
            reason=f"unexpected: {exc.__class__.__name__}: {exc}",
        )
        raise
    log_report(report)
    return report


def _publish(
    database: Path,
    dataset: DatasetSpec,
    extracted: ExtractedBatch,
    *,
    run_id: str,
    started,
    max_reject_rate: float | None,
) -> RunReport:
    winners, rejects = _classify(dataset, extracted)
    with Transaction(database) as connection:
        winners, reference_rejects = dataset.screen(connection, winners)
        loaded_at = format_utc(utc_now())
        stats = publish_versions(
            connection,
            dataset,
            winners,
            source_file=extracted.source_name,
            run_id=run_id,
            loaded_at=loaded_at,
        )
        rejects.extend(reference_rejects)
        rejects.extend(stats.rejects)
        _assert_partition(extracted, rejects, stats.accepted_lines)
        input_count = len(extracted.rows)
        rejected_count = len(rejects)
        accepted_count = stats.accepted
        if accepted_count + rejected_count != input_count:
            raise InvariantError("accepted and rejected counts do not cover the input")
        if _over_reject_threshold(input_count, rejected_count, max_reject_rate):
            raise BatchRejected(
                REJECT_THRESHOLD,
                (
                    f"rejected {rejected_count} of {input_count} records, "
                    f"limit {max_reject_rate}"
                ),
            )
        _insert_rejects(
            connection,
            dataset_name=dataset.name,
            run_id=run_id,
            source_file=extracted.source_name,
            rejected_at=loaded_at,
            rejects=rejects,
        )
        ended = utc_now()
        duration_ms = _duration_ms(started, ended)
        breakdown = _breakdown(rejects)
        mark_succeeded(
            connection,
            run_id=run_id,
            ended_at=format_utc(ended),
            duration_ms=duration_ms,
            input_count=input_count,
            accepted_count=accepted_count,
            rejected_count=rejected_count,
            inserted_count=stats.inserted,
            updated_count=stats.updated,
            unchanged_count=stats.unchanged,
            reject_breakdown_json=json.dumps(breakdown, sort_keys=True),
        )
    return RunReport(
        run_id=run_id,
        dataset=dataset.name,
        status="succeeded",
        started_at=format_utc(started),
        ended_at=format_utc(ended),
        duration_ms=duration_ms,
        source_file=extracted.source_name,
        input_sha256=extracted.sha256,
        input_bytes=extracted.input_bytes,
        input_count=input_count,
        accepted_count=accepted_count,
        rejected_count=rejected_count,
        inserted_count=stats.inserted,
        updated_count=stats.updated,
        unchanged_count=stats.unchanged,
        failure_reason=None,
        reject_breakdown=breakdown,
        pipeline_version=__version__,
    )


def _classify(dataset: DatasetSpec, extracted: ExtractedBatch):
    from ..contracts.models import Candidate

    candidates: list[Candidate] = []
    rejects: list[RejectDecision] = []
    width = len(dataset.columns)
    for row in extracted.rows:
        if len(row.cells) != width:
            raw = _malformed_raw(dataset, row.cells)
            key = raw.get(dataset.key_column, "").strip() or None
            rejects.append(
                RejectDecision(
                    source_line=row.source_line,
                    raw=freeze_strings(raw),
                    record_key=key,
                    reason_code=MALFORMED_ROW,
                    reason_detail=f"expected {width} columns, found {len(row.cells)}",
                )
            )
            continue
        raw = dict(zip(dataset.columns, row.cells, strict=True))
        outcome = dataset.parse(raw, row.source_line)
        if isinstance(outcome, RejectDecision):
            rejects.append(outcome)
        else:
            candidates.append(outcome)
    winners, collapsed = collapse(candidates)
    rejects.extend(collapsed)
    return winners, rejects


def _malformed_raw(dataset: DatasetSpec, cells: tuple[str, ...]) -> dict[str, str]:
    raw: dict[str, str] = {}
    for index, column in enumerate(dataset.columns):
        if index < len(cells):
            raw[column] = cells[index]
    if len(cells) > len(dataset.columns):
        raw["_extra"] = "|".join(cells[len(dataset.columns) :])
    return raw


def _assert_partition(extracted: ExtractedBatch, rejects: list[RejectDecision], accepted_lines: list[int]) -> None:
    actual = sorted([item.source_line for item in rejects] + list(accepted_lines))
    expected = sorted(row.source_line for row in extracted.rows)
    if actual != expected:
        raise InvariantError("each input record must have exactly one outcome")


def _over_reject_threshold(input_count: int, rejected_count: int, max_reject_rate: float | None) -> bool:
    if max_reject_rate is None or input_count == 0:
        return False
    return (rejected_count / input_count) > max_reject_rate


def _insert_rejects(
    connection,
    *,
    dataset_name: str,
    run_id: str,
    source_file: str,
    rejected_at: str,
    rejects: list[RejectDecision],
) -> None:
    if not rejects:
        return
    ordered = sorted(rejects, key=lambda item: (item.source_line, item.reason_code))
    rows = []
    for item in ordered:
        raw_json = json.dumps(dict(item.raw), sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        rows.append(
            (
                f"{run_id}:{item.source_line}:{item.reason_code}",
                run_id,
                dataset_name,
                source_file,
                item.source_line,
                item.record_key,
                item.reason_code,
                item.reason_detail,
                raw_json,
                rejected_at,
            )
        )
    connection.executemany(
        """
        INSERT INTO rejected_records (
            reject_id, run_id, dataset, source_file, source_line, record_key,
            reason_code, reason_detail, raw_values_json, rejected_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def _breakdown(rejects: list[RejectDecision]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in rejects:
        counts[item.reason_code] = counts.get(item.reason_code, 0) + 1
    return dict(sorted(counts.items()))


def _open_run(
    database: Path,
    *,
    run_id: str,
    dataset_name: str,
    started_at: str,
    source_file: str,
    sha256: str | None,
    input_bytes: int | None,
) -> None:
    mark_interrupted(database, started_at)
    start_run(
        database,
        run_id=run_id,
        dataset=dataset_name,
        started_at=started_at,
        source_file=source_file,
        input_sha256=sha256,
        input_bytes=input_bytes,
        pipeline_version=__version__,
    )


def _fail_before_publish(
    database: Path,
    *,
    dataset_name: str,
    run_id: str,
    started,
    source_file: str,
    sha256: str | None,
    input_bytes: int | None,
    reason: str,
) -> RunReport:
    _open_run(
        database,
        run_id=run_id,
        dataset_name=dataset_name,
        started_at=format_utc(started),
        source_file=source_file,
        sha256=sha256,
        input_bytes=input_bytes,
    )
    return _fail_open_run(
        database,
        dataset_name=dataset_name,
        run_id=run_id,
        started=started,
        source_file=source_file,
        sha256=sha256,
        input_bytes=input_bytes,
        input_count=0,
        reason=reason,
    )


def _fail_open_run(
    database: Path,
    *,
    dataset_name: str,
    run_id: str,
    started,
    source_file: str,
    sha256: str | None,
    input_bytes: int | None,
    input_count: int,
    reason: str,
) -> RunReport:
    ended = utc_now()
    duration_ms = _duration_ms(started, ended)
    mark_failed(
        database,
        run_id=run_id,
        ended_at=format_utc(ended),
        duration_ms=duration_ms,
        failure_reason=reason,
    )
    report = RunReport(
        run_id=run_id,
        dataset=dataset_name,
        status="failed",
        started_at=format_utc(started),
        ended_at=format_utc(ended),
        duration_ms=duration_ms,
        source_file=source_file,
        input_sha256=sha256,
        input_bytes=input_bytes,
        input_count=input_count,
        accepted_count=0,
        rejected_count=0,
        inserted_count=0,
        updated_count=0,
        unchanged_count=0,
        failure_reason=reason,
        reject_breakdown={},
        pipeline_version=__version__,
    )
    log_report(report)
    return report


def _duration_ms(started, ended) -> int:
    return max(0, int((ended - started).total_seconds() * 1000))
