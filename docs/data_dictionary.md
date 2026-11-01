# Data dictionary

Timestamps in the target are ISO-8601 UTC (`2024-01-01T00:00:00+00:00`). Money is integer cents. Empty source fields are missing, not nulls in the target: a rejected row never creates a target record, and every published business column is `NOT NULL`.

`source_file` is the file name. `run_id` joins to `pipeline_runs`. `source_line` is the CSV reader's line number (the header is line 1).

## Grain and relationships

| Table | Grain | Key |
| --- | --- | --- |
| `customers` | One current customer | `customer_id` |
| `customer_history` | One accepted customer version | `history_id` = `customer\|{customer_id}\|{updated_at}` |
| `orders` | One current order | `order_id` |
| `order_history` | One accepted order version | `history_id` = `order\|{order_id}\|{event_at}` |
| `pipeline_runs` | One pipeline invocation | `run_id` (UUID) |
| `rejected_records` | One rejected input record for one run | `reject_id` = `{run_id}:{source_line}:{reason_code}` |
| `schema_meta` | Schema contract | `meta_key` = `schema_version`, value `1` |

`orders.customer_id` references `customers.customer_id`. History tables do not cascade. There is at most one `is_current = 1` history row per business key.

History validity is half-open. `valid_from` equals the version timestamp. `valid_to` is null on the open version and, on a closed version, equals the next accepted version's timestamp. A row is effective at time `t` when `valid_from <= t` and (`valid_to` is null or `valid_to > t`). The instant of a change belongs to the newer version.

An older event that loses inside one file, or that arrives after a newer version is already stored, is quarantined. It is not written as history. History contains accepted versions only.

## customers.csv

| Column | Required | Rule |
| --- | --- | --- |
| `customer_id` | yes | `C` and 6 to 9 digits, after trim |
| `full_name` | yes | Trimmed, 1 to 200 characters, no control characters |
| `email` | yes | Trimmed, lowercased, must contain a domain |
| `country_code` | yes | Trimmed, uppercased, one of `US CA GB IN DE AU FR SG` |
| `status` | yes | Exactly `active`, `inactive`, or `suspended` |
| `signup_at` | yes | ISO-8601 with an offset. `Z` is accepted as UTC. Naive timestamps are rejected. |
| `updated_at` | yes | Same timestamp rule. Must be greater than or equal to `signup_at`. |

The header must match these names in this order. An extra, missing, or reordered column fails the file.

### customers

Published columns, all `NOT NULL`:

| Column | Source |
| --- | --- |
| `customer_id` | trimmed id |
| `full_name` | trimmed name |
| `email` | normalized email |
| `email_domain` | derived, the part after `@` |
| `country_code` | normalized code |
| `status` | source status |
| `signup_at` | `signup_at` in UTC |
| `updated_at` | `updated_at` in UTC. This is the version timestamp. |
| `record_hash` | SHA-256 of the business columns above |
| `current_history_id` | open history id |
| `source_file`, `source_line`, `run_id` | lineage of the publish that wrote this version |
| `loaded_at` | pipeline clock when this version was published |

`loaded_at` is not refreshed on an unchanged rerun. The run ledger is the record of every time the file was seen.

## orders.csv

| Column | Required | Rule |
| --- | --- | --- |
| `order_id` | yes | `O` and 6 to 9 digits |
| `customer_id` | yes | Same pattern as a customer id, and must already exist in `customers` |
| `order_status` | yes | Exactly `placed`, `paid`, `shipped`, or `cancelled` |
| `currency` | yes | Trimmed, uppercased, `USD` only |
| `amount` | yes | Decimal with at most 2 fraction digits. Stored as `amount_cents`. |
| `ordered_at` | yes | ISO-8601 with an offset |
| `event_at` | yes | ISO-8601 with an offset. This is the version timestamp. Must be greater than or equal to `ordered_at`. |

Amount rules, applied after the decimal parse:

- Negative amounts are rejected (`amount_negative`).
- Zero is allowed only when `order_status` is `cancelled` (`amount_zero_requires_cancelled`).
- The maximum is `1000000.00` inclusive, which is `100000000` cents (`amount_above_maximum`).

A cancelled order may keep a positive amount. An inactive or suspended customer may still have an order. Both are deliberate for this slice.

### orders

| Column | Notes |
| --- | --- |
| `order_id` | primary key |
| `customer_id` | foreign key to `customers` |
| `order_status` | normalized status |
| `currency` | `USD` |
| `amount_cents` | integer, `0` through `100000000` |
| `ordered_at` | UTC |
| `event_at` | UTC version timestamp |
| `record_hash`, `current_history_id`, `source_file`, `source_line`, `run_id`, `loaded_at` | same lineage rules as customers |

## History tables

`customer_history` carries the customer business columns plus:

| Column | Null | Meaning |
| --- | --- | --- |
| `history_id` | no | deterministic version id |
| `valid_from` | no | equals `updated_at` |
| `valid_to` | yes | null only while `is_current = 1` |
| `is_current` | no | `1` or `0` |
| `record_hash`, `source_file`, `source_line`, `run_id` | no | lineage of that version |

`order_history` is the same shape, with `valid_from` equal to `event_at`.

Point-in-time read:

```sql
SELECT *
FROM customer_history
WHERE customer_id = 'C000101'
  AND valid_from <= '2024-06-01T00:00:00+00:00'
  AND (valid_to IS NULL OR valid_to > '2024-06-01T00:00:00+00:00');
```

## pipeline_runs

| Column | Null | Meaning |
| --- | --- | --- |
| `run_id` | no | UUID |
| `dataset` | no | `customers` or `orders` |
| `status` | no | `running`, `succeeded`, `failed`, or `interrupted` |
| `started_at`, `ended_at` | `ended_at` null only while running | UTC |
| `duration_ms` | yes | wall time for the attempt |
| `source_file` | no | file name |
| `input_sha256`, `input_bytes` | yes | present once the bytes were read |
| `input_count`, `accepted_count`, `rejected_count` | no | for a failed run these stay 0, because nothing was published |
| `inserted_count`, `updated_count`, `unchanged_count` | no | `accepted = inserted + updated + unchanged` |
| `failure_reason` | yes | set when status is `failed` or `interrupted` |
| `reject_breakdown_json` | no | object of reason code to count |
| `pipeline_version` | no | package version |

For every succeeded run, `input_count = accepted_count + rejected_count`, and the number of `rejected_records` for that `run_id` equals `rejected_count`.

## rejected_records

| Column | Null | Meaning |
| --- | --- | --- |
| `reject_id` | no | stable within the run |
| `run_id` | no | foreign key to `pipeline_runs` |
| `dataset`, `source_file`, `source_line` | no | where the record came from |
| `record_key` | yes | business key when the row had one |
| `reason_code`, `reason_detail` | no | machine code and stable detail |
| `raw_values_json` | no | original field text, before normalization |
| `rejected_at` | no | UTC |

A failed or interrupted run has no quarantine rows. Those attempts did not publish.

### Reason codes

The first failing check wins, in this order: malformed width, missing required field in column order, format, enumeration, then cross-field business rules. Reference and version checks run after the row is otherwise valid.

| Code | When |
| --- | --- |
| `malformed_input` | Empty file, bad UTF-8, or CSV the parser cannot read. Fails the file. |
| `unexpected_schema` | Header does not match the contract. Fails the file. |
| `source_unreadable` | Missing or unreadable path. Fails the file. |
| `malformed_row` | Column count does not match the header. |
| `missing_required_field` | Required value is empty after trim. Detail is the column name. |
| `invalid_format` | Id, email, name, timestamp, or amount shape. |
| `invalid_enum` | Status, country, or currency outside the operating set. |
| `business_rule_violation` | `updated_at_before_signup_at`, `event_at_before_ordered_at`, `amount_negative`, `amount_zero_requires_cancelled`, `amount_above_maximum`. |
| `duplicate_key_in_batch` | `exact_duplicate_in_batch` or `superseded_by_later_record_in_batch`. |
| `conflicting_version` | Two different payloads share the newest timestamp, in the file or against the stored version. |
| `invalid_reference` | `customer_id` is well formed and not in `customers`. |
| `out_of_order_event` | Incoming version is older than the stored version. |
| `reject_threshold_exceeded` | Operator gate. Fails the file and rolls back. |

## In-batch version rule

Rows that pass validation are grouped by business key. The newest version timestamp wins and is the only candidate published. An extra copy of that same payload is `exact_duplicate_in_batch`. An older payload is `superseded_by_later_record_in_batch`. If two different payloads share that newest timestamp, every row for the key is `conflicting_version` and nothing for the key is published. A later version of a key must arrive in a later run if it was not the unique newest row in the file.

## Reconciliation queries

```sql
SELECT dataset, reason_code, COUNT(*) AS rejected
FROM rejected_records
GROUP BY dataset, reason_code
ORDER BY dataset, reason_code;

SELECT order_status, COUNT(*) AS orders, SUM(amount_cents) AS amount_cents
FROM orders
GROUP BY order_status
ORDER BY order_status;

SELECT country_code, COUNT(*) AS customers
FROM customers
GROUP BY country_code
ORDER BY country_code;
```

`python -m data_pipeline check --db outputs/pipeline.db` runs the structural controls: no orphan orders, `PRAGMA foreign_key_check`, current rows matched to their open history version, one open version per key, half-open history bounds, and the run equations above.
