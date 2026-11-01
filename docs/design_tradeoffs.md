# Design and trade-offs

This note explains the choices in the pipeline and the limits that follow from them. The goal was a finished, testable slice, not a distributed platform.

## What we optimized for

The brief asks for correctness, a clear design, safe reruns, and proof from tests. A local Python program meets that better than an unfinished Spark or cloud setup. Python 3.10+ and the standard library are the only requirements. There is no account, secret, or network call.

## Layers

| Layer | Responsibility |
| --- | --- |
| Extraction | Read a UTF-8 CSV, hash the file bytes, and reject a bad header before any row is saved |
| Validation | Check required fields, types, allowed values, and business rules. Collapse duplicate keys inside one file |
| Transformation | Normalize values the same way every time: trimmed text, lower-case email, upper-case country and currency, UTC timestamps, money as integer cents |
| Load | Write the current row and a history row in one SQLite transaction |
| Observability | Log one JSON line per run, store counts on the run, and check that the saved data still agrees with those counts |

`orchestration` calls these layers in that order. Dataset rules for customers and orders live in `validation`, so a third file can be added by registering a contract rather than copying the runner.

## Customers and orders

Two datasets are enough to show a real relationship. An order is saved only when its customer is already in `customers`. Load customers first. An unknown customer is quarantined as `invalid_reference`. It does not fail the whole file.

Within one file, the latest version of a key wins. Older copies of that key are quarantined, not written as history. If two different payloads share the newest timestamp, none of that key is published.

Across runs, a newer version updates the current row and appends history. The previous history row is closed with `valid_to` set to the new version time. A point-in-time read uses `valid_from <= t < valid_to`. An older incoming version is quarantined as `out_of_order_event` and does not overwrite current data. The same payload is counted as `unchanged` and does not add another history row.

## Where bad rows go

Row-level problems are saved in `rejected_records` with the file name, line number, raw values, and a stable reason code. The good rows in that file are still published. The run status is `succeeded`.

These problems fail the whole file and publish nothing:

- The file is missing, empty, or not UTF-8
- The header does not match the contract, including a reordered header
- The optional reject-rate gate is exceeded
- An internal check fails

The run row is still recorded as `failed`. SQLite rolls back the data transaction, so a failed attempt does not leave a partial table.

## Why SQLite

SQLite is one file, needs no server, and can enforce primary keys, checks, and the order-to-customer foreign key. Money is stored as integer cents so totals do not use binary floating point. The journal mode is the default rollback journal, not WAL, so the database file is safer on a synced folder such as OneDrive.

The cost is that this is one process and one machine. It is not a warehouse and it does not run on a schedule.

## Performance choices

For the 100,000-row run the whole file is read into memory, current keys are loaded once, and inserts use `executemany` in chunks of 5,000 inside a single transaction. That avoids one lookup and one commit per row.

The same choice does not fit unbounded files. Streaming, with the same quarantine and run contract, is the next step if files grow past memory.

Measured timings are in `outputs/performance.json`. They are one run on this machine, not a guarantee for other hardware.

## What this slice does not do

- Customer deletes. A removed customer is not represented.
- Blocking orders for suspended customers. A suspended customer can still have an order.
- More than one currency. Only `USD` is accepted. Amounts above 1,000,000.00 are rejected.
- A full ISO country list. The operating set is US, CA, GB, IN, DE, AU, FR, and SG.
- Schema migration. If `schema_meta` is not version `1`, the run stops. Rebuild by deleting the database and loading customers, then orders.
- A scheduler, a UI, or a cloud service. The command line is the operator interface.

## Retry, restart, and backfill

| Situation | Result | What to do |
| --- | --- | --- |
| Some rows are bad | Good rows are saved. Bad rows are quarantined | Fix the file and run it again. Unchanged keys stay as they are. Fixed keys insert or update |
| Reject-rate gate, bad header, or a crash inside the transaction | Nothing from that attempt is saved | Fix the cause and run the same command again |
| Process killed after the run row is written but before it is finished | The run stays `running`. SQLite discards the uncommitted writes | Run the file again. The next run marks the stuck row `interrupted` |
| The database is wrong, or the schema version does not match | The file cannot be patched in place | Delete the database and load customers, then orders |

`run_id` is a new UUID every time, because a rerun is a new operation. Business keys stay the producer ids. `source_file` stores the file name only, not a full path.
