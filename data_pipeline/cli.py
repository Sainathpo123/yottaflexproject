"""Command line for demo, single-dataset runs, generation, and checks."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from .contracts.exceptions import InvariantError, PipelineError
from .extraction.generate import MAX_ROWS, MIN_ROWS, generate
from .observability.logs import configure_logging
from .observability.reconcile import build_reconciliation
from .orchestration.jobs import run_benchmark, run_demo
from .orchestration.pipeline import run_dataset
from .paths import (
    DEFAULT_BENCHMARK_DB,
    DEFAULT_BENCHMARK_REPORT,
    DEFAULT_DB,
    DEFAULT_GENERATED,
    DEFAULT_REPORT,
    SAMPLE_CUSTOMERS,
    SAMPLE_ORDERS,
)
from .validation.registry import dataset_names
from .version import __version__


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        return _dispatch(args)
    except InvariantError:
        logging.getLogger("yottaflex.pipeline").exception("pipeline invariant failed")
        return 1
    except PipelineError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="data_pipeline")
    parser.add_argument("--version", action="version", version=__version__)
    commands = parser.add_subparsers(dest="command", required=True)

    demo = commands.add_parser("demo", help="load the sample customers and orders into a fresh database")
    demo.add_argument("--db", type=Path, default=DEFAULT_DB)
    demo.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    demo.add_argument("--customers", type=Path, default=SAMPLE_CUSTOMERS)
    demo.add_argument("--orders", type=Path, default=SAMPLE_ORDERS)

    run = commands.add_parser("run", help="load one CSV")
    run.add_argument("--dataset", required=True, choices=dataset_names())
    run.add_argument("--input", type=Path, required=True)
    run.add_argument("--db", type=Path, default=DEFAULT_DB)
    run.add_argument("--max-reject-rate", type=float, default=None)

    produced = commands.add_parser("generate", help="write a seeded customers file and orders file")
    produced.add_argument("--rows", type=int, required=True)
    produced.add_argument("--seed", type=int, default=42)
    produced.add_argument("--output-dir", type=Path, default=DEFAULT_GENERATED)

    check = commands.add_parser("check", help="run reconciliation controls against an existing database")
    check.add_argument("--db", type=Path, default=DEFAULT_DB)

    bench = commands.add_parser("benchmark", help="generate and load a larger seeded dataset")
    bench.add_argument("--rows", type=int, default=100_000)
    bench.add_argument("--seed", type=int, default=42)
    bench.add_argument("--output-dir", type=Path, default=DEFAULT_GENERATED)
    bench.add_argument("--db", type=Path, default=DEFAULT_BENCHMARK_DB)
    bench.add_argument("--report", type=Path, default=DEFAULT_BENCHMARK_REPORT)
    return parser


def _dispatch(args: argparse.Namespace) -> int:
    if args.command == "demo":
        code = run_demo(args.db, args.report, args.customers, args.orders)
        print(args.report.read_text(encoding="utf-8"), end="")
        return code
    if args.command == "run":
        if args.max_reject_rate is not None and not 0 <= args.max_reject_rate <= 1:
            raise ValueError("max reject rate must be between 0 and 1")
        report = run_dataset(
            args.db,
            args.dataset,
            args.input,
            max_reject_rate=args.max_reject_rate,
        )
        print(json.dumps(report.to_dict(), indent=2))
        return 0 if report.status == "succeeded" else 2
    if args.command == "generate":
        if not MIN_ROWS <= args.rows <= MAX_ROWS:
            raise ValueError(f"rows must be between {MIN_ROWS} and {MAX_ROWS}")
        print(json.dumps(generate(args.rows, args.seed, args.output_dir).to_dict(), indent=2))
        return 0
    if args.command == "check":
        document = build_reconciliation(args.db, [], expect_sample=False)
        print(json.dumps(document, indent=2))
        return 0 if document["passed"] else 2
    if args.command == "benchmark":
        code = run_benchmark(args.rows, args.seed, args.output_dir, args.db, args.report)
        print(args.report.read_text(encoding="utf-8"), end="")
        return code
    raise ValueError(f"unknown command {args.command}")
