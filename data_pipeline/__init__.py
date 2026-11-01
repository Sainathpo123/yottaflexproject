"""Local idempotent pipeline for customers and orders."""

from __future__ import annotations

from .orchestration.pipeline import run_dataset
from .version import __version__

__all__ = ["__version__", "run_dataset"]
