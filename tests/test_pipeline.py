"""Behavioral tests for extraction, data quality, history, reruns, and recovery."""

from __future__ import annotations

import csv
import contextlib
import hashlib
import io
import json
import logging
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from data_pipeline.cli import main
from data_pipeline.contracts.exceptions import StorageError
from data_pipeline.extraction.generate import generate
from data_pipeline.load.storage import connect, ensure_schema, start_run
from data_pipeline.observability.reconcile import build_reconciliation
from data_pipeline.orchestration.jobs import run_demo
from data_pipeline.orchestration.pipeline import run_dataset
from data_pipeline.paths import SAMPLE_CUSTOMERS, SAMPLE_ORDERS
from data_pipeline.validation.customers import COLUMNS as CUSTOMER_COLUMNS
from data_pipeline.validation.orders import COLUMNS as ORDER_COLUMNS
from data_pipeline.version import __version__

T0 = "2024-01-01T00:00:00+00:00"
T1 = "2024-06-01T00:00:00+00:00"
T2 = "2024-08-01T00:00:00+00:00"


def write_csv(directory: Path, name: str, header: tuple[str, ...], rows: list[list[str]]) -> Path:
    path = directory / name
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)
    return path


def customer(
    customer_id: str = "C000201",
    full_name: str = "Ann Example",
    email: str = "ann@example.com",
    country: str = "US",
    status: str = "active",
    signup_at: str = T0,
    updated_at: str = T0,
) -> list[str]:
    return [customer_id, full_name, email, country, status, signup_at, updated_at]


def order(
    order_id: str = "O000201",
    customer_id: str = "C000201",
    status: str = "placed",
    currency: str = "USD",
    amount: str = "10.00",
    ordered_at: str = T1,
    event_at: str = T1,
) -> list[str]:
    return [order_id, customer_id, status, currency, amount, ordered_at, event_at]


class PipelineCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = self.root / "pipeline.db"

    def tearDown(self) -> None:
        self.temp.cleanup()

    def load_customers(self, rows: list[list[str]], **kwargs):
        path = write_csv(self.root, "customers.csv", CUSTOMER_COLUMNS, rows)
        return run_dataset(self.db, "customers", path, **kwargs)

    def load_orders(self, rows: list[list[str]], **kwargs):
        path = write_csv(self.root, "orders.csv", ORDER_COLUMNS, rows)
        return run_dataset(self.db, "orders", path, **kwargs)

    def assert_balanced(self, report) -> None:
        self.assertEqual(report.pipeline_version, __version__)
        self.assertIsNotNone(report.run_id)
        self.assertTrue(str(report.started_at).endswith("+00:00"))
        if report.status != "succeeded":
            self.assertEqual(report.accepted_count, 0)
            return
        self.assertEqual(report.input_count, report.accepted_count + report.rejected_count)
        self.assertEqual(
            report.accepted_count,
            report.inserted_count + report.updated_count + report.unchanged_count,
        )

    def count(self, sql: str, params: tuple = ()) -> int:
        connection = connect(self.db)
        try:
            return int(connection.execute(sql, params).fetchone()[0])
        finally:
            connection.close()

    def one(self, sql: str, params: tuple = ()):
        connection = connect(self.db)
        try:
            return connection.execute(sql, params).fetchone()
        finally:
            connection.close()


class HappyPathTests(PipelineCase):
    def test_insert_normalizes_and_writes_history(self) -> None:
        report = self.load_customers(
            [customer(email=" Ann@Example.com ", customer_id="C000201", full_name="  Ann Example  ")]
        )
        self.assertEqual(report.status, "succeeded")
        self.assert_balanced(report)
        self.assertEqual(report.inserted_count, 1)
        row = self.one("SELECT * FROM customers")
        self.assertEqual(row["email"], "ann@example.com")
        self.assertEqual(row["email_domain"], "example.com")
        self.assertEqual(row["full_name"], "Ann Example")
        self.assertEqual(row["source_file"], "customers.csv")
        self.assertEqual(self.count("SELECT COUNT(*) FROM customer_history WHERE is_current = 1"), 1)
        history = self.one("SELECT history_id, valid_from, valid_to FROM customer_history")
        self.assertEqual(history["history_id"], f"customer|C000201|{T0}")
        self.assertEqual(history["valid_from"], T0)
        self.assertIsNone(history["valid_to"])

    def test_zulu_timestamp_and_maximum_amount(self) -> None:
        self.load_customers([customer()])
        report = self.load_orders([order(amount="1000000.00", ordered_at="2024-06-01T00:00:00Z", event_at=T1)])
        self.assertEqual(report.status, "succeeded")
        self.assertEqual(self.one("SELECT amount_cents, ordered_at FROM orders")["amount_cents"], 100_000_000)
        self.assertEqual(self.one("SELECT ordered_at FROM orders")["ordered_at"], T1)

    def test_header_only_file_is_an_empty_success(self) -> None:
        report = self.load_customers([])
        self.assertEqual(report.status, "succeeded")
        self.assertEqual(report.input_count, 0)
        self.assertEqual(self.count("SELECT COUNT(*) FROM customers"), 0)

    def test_orders_for_inactive_customers_are_allowed(self) -> None:
        self.load_customers([customer(status="inactive")])
        report = self.load_orders([order()])
        self.assertEqual(report.inserted_count, 1)
        self.assert_balanced(report)


class RejectionTests(PipelineCase):
    def test_missing_required_malformed_and_bad_values(self) -> None:
        report = self.load_customers(
            [
                customer(),
                customer(customer_id="", email="missing@example.com"),
                customer(customer_id="C000202", full_name=""),
                customer(customer_id="C000203", email="not-an-email"),
                customer(customer_id="C000204", country="ZZ"),
                customer(customer_id="C000205", status="ACTIVE"),
                customer(customer_id="C000206", signup_at="2024-01-01T00:00:00", updated_at="2024-01-02T00:00:00+00:00"),
                customer(customer_id="C000207", signup_at=T1, updated_at=T0),
                ["C000208", "Short"],
            ]
        )
        self.assertEqual(report.status, "succeeded")
        self.assert_balanced(report)
        self.assertEqual(report.inserted_count, 1)
        self.assertEqual(report.rejected_count, 8)
        self.assertEqual(self.count("SELECT COUNT(*) FROM customers"), 1)
        reasons = {row[0] for row in self._reasons("customers")}
        self.assertTrue(
            {
                "missing_required_field",
                "invalid_format",
                "invalid_enum",
                "business_rule_violation",
                "malformed_row",
            }.issubset(reasons)
        )

    def test_domain_rules_for_amounts_and_events(self) -> None:
        self.load_customers([customer()])
        report = self.load_orders(
            [
                order(order_id="O000301", amount="0.00", status="cancelled"),
                order(order_id="O000302", amount="0.00", status="placed"),
                order(order_id="O000303", amount="-1.00"),
                order(order_id="O000304", amount="10.999"),
                order(order_id="O000305", amount="1000000.01"),
                order(order_id="O000306", ordered_at=T2, event_at=T1),
                order(order_id="O000307", customer_id="C000999"),
            ]
        )
        self.assert_balanced(report)
        self.assertEqual(report.inserted_count, 1)
        self.assertEqual(self.one("SELECT order_id FROM orders")["order_id"], "O000301")
        details = {row[1] for row in self._reasons("orders")}
        self.assertIn("amount_zero_requires_cancelled", details)
        self.assertIn("amount_negative", details)
        self.assertIn("amount_above_maximum", details)
        self.assertIn("event_at_before_ordered_at", details)

    def test_duplicate_key_keeps_latest_version_only(self) -> None:
        report = self.load_customers(
            [
                customer(full_name="Early Ann", updated_at=T0),
                customer(full_name="Later Ann", updated_at=T2, email="later@example.com"),
            ]
        )
        self.assertEqual(report.inserted_count, 1)
        self.assertEqual(report.rejected_count, 1)
        self.assertEqual(report.reject_breakdown["duplicate_key_in_batch"], 1)
        self.assertEqual(self.one("SELECT full_name FROM customers")["full_name"], "Later Ann")
        self.assertEqual(self.count("SELECT COUNT(*) FROM customer_history"), 1)

    def test_conflicting_versions_in_one_batch_publish_nothing_for_that_key(self) -> None:
        report = self.load_customers(
            [
                customer(customer_id="C000201", full_name="Ann", email="ann@example.com"),
                customer(customer_id="C000201", full_name="Other", email="other@example.com"),
                customer(customer_id="C000202", email="ben@example.com"),
            ]
        )
        self.assertEqual(report.inserted_count, 1)
        self.assertEqual(self.one("SELECT customer_id FROM customers")["customer_id"], "C000202")
        self.assertEqual(report.reject_breakdown["conflicting_version"], 2)

    def test_exact_duplicate_is_quarantined(self) -> None:
        row = customer()
        report = self.load_customers([row, list(row)])
        self.assertEqual(report.inserted_count, 1)
        self.assertEqual(self.one("SELECT reason_detail FROM rejected_records")["reason_detail"], "exact_duplicate_in_batch")

    def test_reject_keeps_the_raw_source_value(self) -> None:
        self.load_customers([customer(customer_id="C000201", email="Not An Email")])
        raw = json.loads(self.one("SELECT raw_values_json FROM rejected_records")["raw_values_json"])
        self.assertEqual(raw["email"], "Not An Email")
        self.assertEqual(self.one("SELECT source_line FROM rejected_records")["source_line"], 2)

    def _reasons(self, dataset: str) -> list[tuple]:
        connection = connect(self.db)
        try:
            return connection.execute(
                "SELECT reason_code, reason_detail FROM rejected_records WHERE dataset = ?",
                (dataset,),
            ).fetchall()
        finally:
            connection.close()


class HistoryAndRerunTests(PipelineCase):
    def test_newer_version_keeps_prior_history(self) -> None:
        first = self.load_customers([customer(full_name="Version One", updated_at=T0)])
        second = self.load_customers([customer(full_name="Version Two", email="two@example.com", updated_at=T2)])
        self.assertEqual(first.inserted_count, 1)
        self.assertEqual(second.updated_count, 1)
        self.assertEqual(self.count("SELECT COUNT(*) FROM customers"), 1)
        self.assertEqual(self.count("SELECT COUNT(*) FROM customer_history"), 2)
        self.assertEqual(self.one("SELECT full_name FROM customers")["full_name"], "Version Two")
        closed = self.one(
            "SELECT valid_to FROM customer_history WHERE is_current = 0"
        )
        self.assertEqual(closed["valid_to"], T2)
        as_of_old = self.one(
            """
            SELECT full_name FROM customer_history
            WHERE customer_id = 'C000201'
              AND valid_from <= ?
              AND (valid_to IS NULL OR valid_to > ?)
            """,
            (T0, T0),
        )
        as_of_new = self.one(
            """
            SELECT full_name FROM customer_history
            WHERE customer_id = 'C000201'
              AND valid_from <= ?
              AND (valid_to IS NULL OR valid_to > ?)
            """,
            (T2, T2),
        )
        self.assertEqual(as_of_old["full_name"], "Version One")
        self.assertEqual(as_of_new["full_name"], "Version Two")

    def test_rerun_is_unchanged_and_stale_event_does_not_overwrite(self) -> None:
        self.load_customers([customer(full_name="Current", updated_at=T2)])
        rerun = self.load_customers([customer(full_name="Current", updated_at=T2)])
        self.assertEqual(rerun.unchanged_count, 1)
        self.assertEqual(rerun.inserted_count, 0)
        self.assertEqual(self.count("SELECT COUNT(*) FROM customer_history"), 1)
        stale = self.load_customers([customer(full_name="Old", email="old@example.com", updated_at=T0)])
        self.assertEqual(stale.rejected_count, 1)
        self.assertEqual(stale.reject_breakdown["out_of_order_event"], 1)
        self.assertEqual(self.one("SELECT full_name FROM customers")["full_name"], "Current")
        self.assertEqual(self.count("SELECT COUNT(*) FROM customer_history"), 1)

    def test_same_timestamp_different_payload_conflicts_with_current(self) -> None:
        self.load_customers([customer(full_name="Ann", email="ann@example.com", updated_at=T1)])
        report = self.load_customers([customer(full_name="Ann", email="other@example.com", updated_at=T1)])
        self.assertEqual(report.reject_breakdown["conflicting_version"], 1)
        self.assertEqual(self.one("SELECT email FROM customers")["email"], "ann@example.com")

    def test_order_rerun_and_late_event(self) -> None:
        self.load_customers([customer()])
        self.load_orders([order(status="placed", event_at=T2, ordered_at=T1)])
        newer = self.load_orders([order(status="shipped", event_at="2024-09-01T00:00:00+00:00", ordered_at=T1)])
        self.assertEqual(newer.updated_count, 1)
        self.assertEqual(self.count("SELECT COUNT(*) FROM order_history"), 2)
        stale = self.load_orders([order(status="cancelled", amount="0.00", event_at=T1, ordered_at=T1)])
        self.assertEqual(stale.reject_breakdown["out_of_order_event"], 1)
        self.assertEqual(self.one("SELECT order_status FROM orders")["order_status"], "shipped")
        replay = self.load_orders([order(status="shipped", event_at="2024-09-01T00:00:00+00:00", ordered_at=T1)])
        self.assertEqual(replay.unchanged_count, 1)
        self.assertEqual(self.count("SELECT COUNT(*) FROM order_history"), 2)

    def test_corrected_reject_can_be_retried_without_duplicating_the_good_row(self) -> None:
        first = self.load_customers(
            [
                customer(customer_id="C000301", email="good@example.com"),
                customer(customer_id="C000302", email="broken"),
            ]
        )
        self.assertEqual(first.inserted_count, 1)
        self.assertEqual(first.rejected_count, 1)
        second = self.load_customers(
            [
                customer(customer_id="C000301", email="good@example.com"),
                customer(customer_id="C000302", email="fixed@example.com"),
            ]
        )
        self.assertEqual(second.unchanged_count, 1)
        self.assertEqual(second.inserted_count, 1)
        self.assertEqual(self.count("SELECT COUNT(*) FROM customers"), 2)
        self.assertEqual(self.count("SELECT COUNT(*) FROM customer_history"), 2)


class FailureAndRecoveryTests(PipelineCase):
    def test_schema_drift_publishes_nothing(self) -> None:
        self.load_customers([customer()])
        drifted = write_csv(
            self.root,
            "drift.csv",
            (*CUSTOMER_COLUMNS, "extra"),
            [customer() + ["nope"]],
        )
        report = run_dataset(self.db, "customers", drifted)
        self.assertEqual(report.status, "failed")
        self.assertIn("unexpected_schema", report.failure_reason or "")
        self.assertEqual(self.count("SELECT COUNT(*) FROM customers"), 1)
        self.assertEqual(self.count("SELECT COUNT(*) FROM rejected_records"), 0)

    def test_reordered_columns_are_rejected(self) -> None:
        header = tuple(reversed(CUSTOMER_COLUMNS))
        path = write_csv(self.root, "reordered.csv", header, [list(reversed(customer()))])
        report = run_dataset(self.db, "customers", path)
        self.assertEqual(report.status, "failed")
        self.assertEqual(self.count("SELECT COUNT(*) FROM customers"), 0)

    def test_empty_file_malformed_utf8_and_missing_file(self) -> None:
        empty = self.root / "empty.csv"
        empty.write_bytes(b"")
        report = run_dataset(self.db, "customers", empty)
        self.assertIn("malformed_input", report.failure_reason or "")
        binary = self.root / "binary.csv"
        binary.write_bytes(b"\xff\xfe\x00")
        report = run_dataset(self.db, "customers", binary)
        self.assertIn("malformed_input", report.failure_reason or "")
        report = run_dataset(self.db, "customers", self.root / "missing.csv")
        self.assertIn("source_unreadable", report.failure_reason or "")
        self.assertEqual(self.count("SELECT COUNT(*) FROM customers"), 0)

    def test_bom_is_accepted(self) -> None:
        payload = ("\ufeff" + ",".join(CUSTOMER_COLUMNS) + "\n" + ",".join(customer()) + "\n").encode("utf-8")
        path = self.root / "bom.csv"
        path.write_bytes(payload)
        report = run_dataset(self.db, "customers", path)
        self.assertEqual(report.inserted_count, 1)
        self.assertEqual(report.input_sha256, hashlib.sha256(payload).hexdigest())

    def test_reject_threshold_rolls_back_the_good_rows(self) -> None:
        report = self.load_customers(
            [customer(), customer(customer_id="C000202", email="broken")],
            max_reject_rate=0,
        )
        self.assertEqual(report.status, "failed")
        self.assertIn("reject_threshold_exceeded", report.failure_reason or "")
        self.assertEqual(self.count("SELECT COUNT(*) FROM customers"), 0)
        self.assertEqual(self.count("SELECT COUNT(*) FROM rejected_records"), 0)
        self.assertEqual(self.count("SELECT COUNT(*) FROM customer_history"), 0)
        retry = self.load_customers([customer()])
        self.assertEqual(retry.inserted_count, 1)

    def test_unexpected_error_marks_the_run_failed_and_rolls_back(self) -> None:
        with patch("data_pipeline.orchestration.pipeline.publish_versions", side_effect=RuntimeError("boom")):
            with self.assertRaises(RuntimeError):
                self.load_customers([customer()])
        self.assertEqual(self.one("SELECT status, failure_reason FROM pipeline_runs")["status"], "failed")
        self.assertIn("boom", self.one("SELECT failure_reason FROM pipeline_runs")["failure_reason"])
        self.assertEqual(self.count("SELECT COUNT(*) FROM customers"), 0)

    def test_interrupted_run_is_closed_by_the_next_attempt(self) -> None:
        ensure_schema(self.db)
        start_run(
            self.db,
            run_id="stuck-run",
            dataset="customers",
            started_at=T0,
            source_file="old.csv",
            input_sha256=None,
            input_bytes=None,
            pipeline_version=__version__,
        )
        report = self.load_customers([customer()])
        self.assertEqual(report.status, "succeeded")
        stuck = self.one("SELECT status, failure_reason FROM pipeline_runs WHERE run_id = 'stuck-run'")
        self.assertEqual(stuck["status"], "interrupted")
        self.assertIn("finalized", stuck["failure_reason"])

    def test_schema_version_mismatch_stops_before_a_new_run(self) -> None:
        self.load_customers([customer()])
        connection = connect(self.db)
        try:
            connection.execute("UPDATE schema_meta SET meta_value = '9' WHERE meta_key = 'schema_version'")
        finally:
            connection.close()
        with self.assertRaises(StorageError):
            self.load_customers([customer(customer_id="C000202", email="other@example.com")])
        self.assertEqual(self.count("SELECT COUNT(*) FROM customers"), 1)

    def test_orders_without_customers_are_reference_rejects(self) -> None:
        report = self.load_orders([order(), order(order_id="O000202")])
        self.assertEqual(report.status, "succeeded")
        self.assert_balanced(report)
        self.assertEqual(report.rejected_count, 2)
        self.assertEqual(report.reject_breakdown["invalid_reference"], 2)
        self.assertEqual(self.count("SELECT COUNT(*) FROM orders"), 0)

    def test_control_character_in_name_is_rejected(self) -> None:
        report = self.load_customers([customer(full_name="Ann\nExample")])
        self.assertEqual(report.rejected_count, 1)
        self.assertEqual(report.reject_breakdown["invalid_format"], 1)


class SampleAndGeneratorTests(PipelineCase):
    def test_sample_files_match_the_published_contract(self) -> None:
        customers = run_dataset(self.db, "customers", SAMPLE_CUSTOMERS)
        orders = run_dataset(self.db, "orders", SAMPLE_ORDERS)
        self.assertEqual(customers.status, "succeeded")
        self.assertEqual(orders.status, "succeeded")
        self.assert_balanced(customers)
        self.assert_balanced(orders)
        self.assertEqual(customers.input_count, 14)
        self.assertEqual(orders.input_count, 14)
        self.assertEqual(customers.inserted_count, 5)
        self.assertEqual(orders.inserted_count, 5)
        self.assertEqual(
            customers.reject_breakdown,
            {
                "business_rule_violation": 1,
                "duplicate_key_in_batch": 1,
                "invalid_enum": 2,
                "invalid_format": 2,
                "malformed_row": 1,
                "missing_required_field": 2,
            },
        )
        self.assertEqual(
            orders.reject_breakdown,
            {
                "business_rule_violation": 3,
                "duplicate_key_in_batch": 1,
                "invalid_enum": 1,
                "invalid_format": 1,
                "invalid_reference": 1,
                "malformed_row": 1,
                "missing_required_field": 1,
            },
        )
        alice = self.one("SELECT full_name, email, email_domain FROM customers WHERE customer_id = 'C000101'")
        self.assertEqual(tuple(alice), ("Alice Duplicate", "alice@example.com", "example.com"))
        bob = self.one("SELECT signup_at, updated_at FROM customers WHERE customer_id = 'C000102'")
        self.assertEqual(bob["signup_at"], "2024-02-02T08:04:05+00:00")
        self.assertEqual(bob["updated_at"], "2024-03-01T05:00:00+00:00")
        priya = self.one("SELECT email, country_code, status, signup_at FROM customers WHERE customer_id = 'C000111'")
        self.assertEqual(priya["email"], "priya.shah@example.com")
        self.assertEqual(priya["country_code"], "IN")
        self.assertEqual(priya["status"], "suspended")
        self.assertEqual(priya["signup_at"], "2024-06-30T18:30:00+00:00")
        self.assertEqual(self.one("SELECT order_status, amount_cents FROM orders WHERE order_id = 'O000201'")["order_status"], "paid")
        self.assertEqual(self.count("SELECT COALESCE(SUM(amount_cents), 0) FROM orders"), 100_006_799)
        self.assertEqual(self.count("SELECT COUNT(DISTINCT customer_id) FROM orders"), 5)
        malformed_key = self.one(
            "SELECT reason_code FROM rejected_records WHERE dataset = 'customers' AND record_key = 'C000112'"
        )
        self.assertEqual(malformed_key["reason_code"], "malformed_row")
        malformed = self.one(
            "SELECT source_line FROM rejected_records WHERE dataset = 'customers' AND reason_code = 'malformed_row'"
        )
        self.assertEqual(malformed["source_line"], 15)
        digest = hashlib.sha256(SAMPLE_CUSTOMERS.read_bytes()).hexdigest()
        self.assertEqual(customers.input_sha256, digest)
        replay = run_dataset(self.db, "customers", SAMPLE_CUSTOMERS)
        self.assertEqual(replay.unchanged_count, 5)
        self.assertEqual(replay.inserted_count, 0)
        self.assertEqual(self.count("SELECT COUNT(*) FROM customer_history"), 5)
        document = build_reconciliation(self.db, [customers, orders, replay], expect_sample=False)
        self.assertTrue(document["passed"])
        self.assertEqual(document["aggregates"]["order_amount_cents"], 100_006_799)

    def test_generator_is_deterministic_and_reconciles(self) -> None:
        first = generate(30, 42, self.root / "a")
        second = generate(30, 42, self.root / "b")
        other = generate(30, 7, self.root / "c")
        self.assertEqual(first.customers_sha256, second.customers_sha256)
        self.assertEqual(first.orders_sha256, second.orders_sha256)
        self.assertEqual(first.customers_sha256, other.customers_sha256)
        self.assertNotEqual(first.orders_sha256, other.orders_sha256)
        self.assertEqual(first.customers_data_rows, 38)
        self.assertEqual(first.orders_data_rows, 40)
        customers = run_dataset(self.db, "customers", self.root / "a" / "customers.csv")
        orders = run_dataset(self.db, "orders", self.root / "a" / "orders.csv")
        self.assert_balanced(customers)
        self.assert_balanced(orders)
        self.assertEqual(self.count("SELECT COUNT(*) FROM customers"), 30)
        self.assertEqual(self.count("SELECT COUNT(*) FROM orders"), 31)
        self.assertEqual(self.one("SELECT full_name FROM customers WHERE customer_id = 'C0000001'")["full_name"], "Updated 1")
        self.assertEqual(self.one("SELECT email, country_code FROM customers WHERE customer_id = 'C0000002'")["email"], "user2@example.com")
        self.assertEqual(self.one("SELECT country_code FROM customers WHERE customer_id = 'C0000003'")["country_code"], "GB")
        self.assertEqual(self.one("SELECT order_status FROM orders WHERE order_id = 'O0000001'")["order_status"], "shipped")
        self.assertEqual(self.one("SELECT amount_cents, order_status FROM orders WHERE order_id = 'O9000005'")["amount_cents"], 0)
        replay = run_dataset(self.db, "customers", self.root / "a" / "customers.csv")
        self.assertEqual(replay.unchanged_count, 30)
        self.assertEqual(replay.inserted_count, 0)
        self.assertEqual(self.count("SELECT COUNT(*) FROM customer_history"), 30)
        self.assertTrue(build_reconciliation(self.db, [customers, orders, replay], expect_sample=False)["passed"])

    def test_demo_and_cli_write_a_passing_report(self) -> None:
        report_path = self.root / "reconciliation.json"
        code = run_demo(self.db, report_path, SAMPLE_CUSTOMERS, SAMPLE_ORDERS)
        self.assertEqual(code, 0)
        document = json.loads(report_path.read_text(encoding="utf-8"))
        self.assertTrue(document["passed"])
        self.assertTrue(all(check["passed"] for check in document["checks"]))
        self.assertEqual(document["aggregates"]["orders"], 5)
        with contextlib.redirect_stdout(io.StringIO()):
            cli_code = main(["check", "--db", str(self.db)])
        self.assertEqual(cli_code, 0)

    def test_run_log_contains_the_run_id(self) -> None:
        records: list[logging.LogRecord] = []

        class Capture(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                records.append(record)

        logger = logging.getLogger("yottaflex.pipeline")
        handler = Capture()
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        try:
            report = self.load_customers([customer()])
        finally:
            logger.removeHandler(handler)
        self.assertTrue(any(getattr(record, "run_id", None) == report.run_id for record in records))


if __name__ == "__main__":
    unittest.main()
