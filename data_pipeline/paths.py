"""Default locations relative to the project root, not the process cwd."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SAMPLE_CUSTOMERS = ROOT / "data" / "sample" / "customers.csv"
SAMPLE_ORDERS = ROOT / "data" / "sample" / "orders.csv"
DEFAULT_DB = ROOT / "outputs" / "pipeline.db"
DEFAULT_REPORT = ROOT / "outputs" / "reconciliation.json"
DEFAULT_BENCHMARK_DB = ROOT / "outputs" / "large.db"
DEFAULT_BENCHMARK_REPORT = ROOT / "outputs" / "performance.json"
DEFAULT_GENERATED = ROOT / "data" / "generated"
