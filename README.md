# YottaFlex data pipeline

Local, idempotent pipeline that lands two related datasets, `customers` and `orders`, into SQLite. It is a finished vertical slice: reusable extract, validation, transform, load, quarantine, logging, and reconciliation. Python 3.10 or newer is the only requirement. There are no third-party packages, credentials, or network calls.

Synthetic data only. Do not point this at a source you are not allowed to use.

## Architecture

```text
CSV file
  -> byte hash and strict header contract
  -> row validation (required fields, types, domains, business rules)
  -> in-batch collapse (latest version of each key)
  -> reference check (orders must cite a published customer)
  -> one SQLite transaction
       current table + history table
       quarantine rows for this run
       run ledger flipped to succeeded
  -> JSON log line and reconciliation report
```

A file-level failure (unreadable file, bad encoding, empty file, schema drift, reject-rate gate, or a broken internal check) rolls the transaction back. The run ledger still records the failure. Row-level data-quality problems do not fail the run: valid rows publish, and invalid rows are stored in `rejected_records` with the source file, line, raw values, and reason.

```text
customers.csv  ->  customers + customer_history
orders.csv     ->  orders + order_history
                         |
                         +-> rejected_records
                         +-> pipeline_runs
```

Load customers before orders. An order whose customer is not already in `customers` is quarantined as `invalid_reference`.

## Layout

Each pipeline stage is its own package under `data_pipeline/`. Orchestration calls the layers; it does not own their rules.

| Path | Layer |
| --- | --- |
| `data_pipeline/extraction/` | Read CSV, hash the file, and generate synthetic inputs |
| `data_pipeline/validation/` | Field rules, batch collapse, and the customer and order contracts |
| `data_pipeline/transformation/` | Shared normalization, including UTC timestamps |
| `data_pipeline/load/` | SQLite schema, transactions, and versioned publishing |
| `data_pipeline/observability/` | JSON logs, run metrics, and reconciliation |
| `data_pipeline/contracts/` | Shared models, reason codes, and errors |
| `data_pipeline/orchestration/` | Run lifecycle: extract, validate, load, quarantine |
| `data_pipeline/cli.py` | Command line |
| `data/sample/` | Small files with valid rows and deliberate defects |
| `data/generated/` | Output of `generate` (not source-controlled) |
| `tests/` | Automated checks |
| `docs/data_dictionary.md` | Grain, keys, nullability, rules |
| `docs/design_tradeoffs.md` | Choices and limits |
| `outputs/reconciliation.json` | Sample run evidence |
| `outputs/performance.json` | Seeded 100,000-row timing |

## Setup

From this directory:

```powershell
python --version
python -m unittest discover -v
python -m data_pipeline demo
```

`demo` deletes `outputs/pipeline.db`, loads the sample files, and writes `outputs/reconciliation.json`. The process exit code is 0 only when every reconciliation check passes.

## Commands

```powershell
python -m data_pipeline run --dataset customers --input data/sample/customers.csv --db outputs/pipeline.db
python -m data_pipeline run --dataset orders --input data/sample/orders.csv --db outputs/pipeline.db
python -m data_pipeline check --db outputs/pipeline.db
python -m data_pipeline generate --rows 1000 --seed 42 --output-dir data/generated
python -m data_pipeline benchmark --rows 100000 --seed 42
```

`--rows` is the number of valid base records per dataset and must be from 8 to 2,000,000. A fixed trailer of duplicate, missing, late, and invalid rows is appended so the large file still exercises quarantine. The seed changes order amounts only.

`--max-reject-rate` is an optional fraction from 0 to 1. The batch is rolled back when `rejected / input` is greater than this value. `0` fails the batch if any row is rejected. Omit it to publish the good rows and quarantine the rest.

## Naming

| Kind | Rule |
| --- | --- |
| Tables and columns | `snake_case` |
| Business keys | Producer ids. Customers match `C` plus 6 to 9 digits. Orders match `O` plus 6 to 9 digits. |
| History id | `{entity}\|{business_key}\|{version_timestamp_utc}` |
| Version time | `updated_at` for customers, `event_at` for orders. Stored as ISO-8601 UTC. |
| Run id | New UUID per invocation. A rerun is a new operation, so the run id is not derived from the file. |
| `source_file` | File name only, not a full path. |
| `input_sha256` | SHA-256 of the raw file bytes, recorded on the run. |

## Sample result

The sample load publishes 5 customers and 5 orders. Nine customer rows and nine order rows are quarantined. Order amount totals **100,006,799 cents**. `C000101` keeps the later name `Alice Duplicate`. `O000201` keeps the later status `paid`. Reloading either sample file inserts nothing and does not add history rows.

Details, including every rejected raw record, are in `outputs/reconciliation.json`.

## Recovery

| Situation | What is stored | What to do |
| --- | --- | --- |
| Bad rows, good rows | Good rows committed, bad rows quarantined, run `succeeded` | Fix the source and rerun the file. Unchanged keys are not duplicated. Corrected keys insert or update. |
| Reject rate over the gate | Nothing from that attempt, run `failed` | Fix the file or raise the gate, then rerun. |
| Schema drift, empty file, bad UTF-8 | Nothing published, run `failed` | Replace the file and rerun. |
| Process killed mid-transaction | Run left at `running`, uncommitted writes discarded by SQLite | Run the file again. The next run marks the stuck row `interrupted`. |
| Rebuild | Current database is wrong or the schema version does not match | Delete the database file and load customers, then orders. That is the backfill path for this slice. |

Rerunning a file is safe. An incoming version older than the stored version is quarantined as `out_of_order_event` and does not overwrite current state. The same payload is counted as `unchanged` and does not append history.

## Tests

`python -m unittest discover -v` covers the happy path, empty input, malformed rows, schema drift, encoding failures, missing files, duplicates, nulls, invalid references, late and out-of-order versions, conflicting versions, zero and negative amounts, idempotent reruns, reject-threshold rollback, unexpected errors, interrupted runs, the sample contract, and the seeded generator.

## Performance

Measured with `python -m data_pipeline benchmark --rows 100000 --seed 42` on this Windows machine (Python 3.12.0). These are one-machine timings, not a hardware-independent guarantee. See `outputs/performance.json`.

| Step | Result |
| --- | --- |
| Generate 100,000 base rows plus the defect trailer | 3.094 s |
| Load customers (100,008 input rows, 100,000 published) | 7.671 s, about 13,037 rows/s |
| Load orders (100,010 input rows, 100,001 published) | 6.543 s, about 15,284 rows/s |
| Reconciliation | passed |

The load path reads current keys once, writes with `executemany` in chunks of 5,000, and commits one transaction per dataset. The file is hashed from the bytes already read. SQLite stays on the default rollback journal rather than WAL so the database file is safe to keep on a synced folder.

## Limits

This is one process and one SQLite file, not a scheduler or a warehouse. The whole file is held in memory, which is appropriate for the 100,000-row run and not for unbounded files. The country list and `USD` are the operating set for this slice. Customer deletes are not supported. A suspended customer can still receive an order. Current tables are the latest accepted version; older accepted versions stay in the history tables.

## Possible next steps

- Stream files that do not fit in memory, with the same run and quarantine contract.
- Add a scheduler around `python -m data_pipeline run` rather than a second orchestration framework inside this process.
- Reject orders for suspended customers if the business wants that rule.
- Add currencies only with an explicit rate table, still stored as integer minor units.
- Promote the SQLite contract to a warehouse once the reconciliation checks are the same.
