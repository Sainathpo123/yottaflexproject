# How to run

This project reads two CSV files, customers and orders, checks every row, and saves the good rows in a local database. Bad rows are kept aside with the reason they failed.

You do not install anything. Python 3.10 or newer is enough. This machine already has Python 3.12.

Open a terminal in this folder:

```powershell
cd "C:\Users\RMudhike\OneDrive - Stryker\Desktop\abc\yottaflex-data-pipeline"
```

Run the steps below in this order.

## 1. Create the data

The challenge did not provide CSV files. This command creates them. `500` means 500 normal customers and 500 normal orders. A few extra bad rows are added on purpose so the pipeline has something to reject. `42` is the seed, so the same command always creates the same files.

```powershell
python -m data_pipeline generate --rows 500 --seed 42 --output-dir data/generated
```

Files created:

- `data/generated/customers.csv`
- `data/generated/orders.csv`

Small hand-written samples also exist in `data/sample/`. You do not need to generate those.

## 2. Run the automated tests

This checks the pipeline rules. It does not use the 500-row files. It uses its own temporary files and the small samples.

```powershell
python -m unittest discover -v
```

You want the last lines to say `OK`. A few JSON lines in the middle are normal. They are run logs, not errors.

## 3. Load the customers

Customers must be loaded first. Orders look up these customers.

```powershell
python -m data_pipeline run --dataset customers --input data/generated/customers.csv --db outputs/pipeline.db
```

A good run has `"status": "succeeded"`. `input_count` must equal `accepted_count` plus `rejected_count`.

## 4. Load the orders

```powershell
python -m data_pipeline run --dataset orders --input data/generated/orders.csv --db outputs/pipeline.db
```

Same check: status succeeded, and input equals accepted plus rejected.

## 5. Check the saved data

```powershell
python -m data_pipeline check --db outputs/pipeline.db
```

You want `"passed": true`. This confirms every order belongs to a real customer, counts still add up, and reruns did not create duplicate current rows.

## Where the results are

| File | What it is |
| --- | --- |
| `outputs/pipeline.db` | The database. Customers, orders, history, rejected rows, and run counts are stored here. |
| Terminal output | The JSON report from `run` and `check`. This is printed on screen only. |

`pipeline.db` is a database file. The editor cannot show it as text. Do not use Open Anyway.

To save the check report as a file you can open:

```powershell
python -m data_pipeline check --db outputs/pipeline.db > outputs/reconciliation.json
```

Then open `outputs/reconciliation.json`.

## Optional commands

Load the small sample files into a fresh database and write `outputs/reconciliation.json`. This deletes `outputs/pipeline.db` first, so run it only when you want to replace the 500-row database.

```powershell
python -m data_pipeline demo
```

Create and load about 100,000 rows. This uses a separate database, `outputs/large.db`, and writes `outputs/performance.json`.

```powershell
python -m data_pipeline benchmark --rows 100000 --seed 42
```

Fail the whole load if too many rows are bad. `0` means any rejected row fails the batch and nothing from that file is saved. Leave this off for the normal run.

```powershell
python -m data_pipeline run --dataset customers --input data/generated/customers.csv --db outputs/pipeline.db --max-reject-rate 0
```

## If you need to start over

Delete `outputs/pipeline.db`, then run step 3 and step 4 again. Always load customers before orders.
