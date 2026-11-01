"""Pipeline errors that callers can handle without parsing message text."""

from __future__ import annotations


class PipelineError(Exception):
    """Base error for expected pipeline failures and broken invariants."""


class UnknownDatasetError(PipelineError):
    """Raised when the caller asks for a dataset the registry does not define."""

    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(f"unknown dataset: {name}")


class BatchRejected(PipelineError):
    """A file-level failure. No target rows from this attempt are published."""

    def __init__(
        self,
        reason_code: str,
        detail: str,
        *,
        sha256: str | None = None,
        input_bytes: int | None = None,
    ) -> None:
        self.reason_code = reason_code
        self.detail = detail
        self.sha256 = sha256
        self.input_bytes = input_bytes
        super().__init__(f"{reason_code}: {detail}")


class InvariantError(PipelineError):
    """Raised when an internal accounting check fails. The batch is rolled back."""


class StorageError(PipelineError):
    """Raised when the local database cannot honor the pipeline contract."""


class ParseIssue(Exception):
    """Internal signal used while validating one field. Not a public error."""

    def __init__(self, reason_code: str, detail: str) -> None:
        self.reason_code = reason_code
        self.detail = detail
        super().__init__(detail)
