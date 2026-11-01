"""Value objects passed between extract, validate, publish, and report."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from types import MappingProxyType
from typing import Mapping


def freeze_strings(values: Mapping[str, str]) -> Mapping[str, str]:
    return MappingProxyType(dict(values))


@dataclass(frozen=True)
class SourceRow:
    source_line: int
    cells: tuple[str, ...]


@dataclass(frozen=True)
class ExtractedBatch:
    source_name: str
    sha256: str
    input_bytes: int
    rows: tuple[SourceRow, ...]


@dataclass(frozen=True)
class Candidate:
    """One structurally valid record, ready for collapse and publishing."""

    key: str
    version_at: datetime
    values: Mapping[str, object]
    source_line: int
    raw: Mapping[str, str]
    record_hash: str


@dataclass(frozen=True)
class RejectDecision:
    source_line: int
    raw: Mapping[str, str]
    record_key: str | None
    reason_code: str
    reason_detail: str


@dataclass
class PublishStats:
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    rejects: list[RejectDecision] = field(default_factory=list)
    accepted_lines: list[int] = field(default_factory=list)

    @property
    def accepted(self) -> int:
        return self.inserted + self.updated + self.unchanged


@dataclass(frozen=True)
class RunReport:
    run_id: str
    dataset: str
    status: str
    started_at: str
    ended_at: str | None
    duration_ms: int | None
    source_file: str
    input_sha256: str | None
    input_bytes: int | None
    input_count: int
    accepted_count: int
    rejected_count: int
    inserted_count: int
    updated_count: int
    unchanged_count: int
    failure_reason: str | None
    reject_breakdown: dict[str, int]
    pipeline_version: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)
