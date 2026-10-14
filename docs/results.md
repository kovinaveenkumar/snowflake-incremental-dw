# Results

Every number here comes from an actual run. All data is synthetic.

## Step 1: Data generator (2026-10-14)

Command: `python generator/generate_files.py --out data/raw` (seed 42, 500 accounts, 30 business days). Runtime: 0.45 s.

| Metric | Value |
|---|---|
| Business days | 30 (2026-09-01 to 2026-10-12, weekends skipped, no holiday calendar) |
| Securities (reference) | 100 |
| Account rows sent | 893 (500 on day 1, then about 14 a day: changed, new and re-sent unchanged) |
| Distinct accounts | 538 (500 initial + 38 opened later) |
| Trade rows sent | 60,031 (1,906 to 2,104 per day, bad rows included) |
| Injected bad rows | 178 |
| ...missing account_id | 41 |
| ...negative quantity | 46 |
| ...unknown security | 45 |
| ...duplicate trade_id | 46 |
| Files written | 92 (30 accounts + 30 trades + 30 control + securities.csv + manifest.json), 6.7 MB |

Unit tests: `pytest tests/test_generator.py` → **17 passed** in 0.45 s.

## Step 2: Snowflake setup (2026-10-14)

Account: Snowflake trial, AWS us-east-2, Snowflake 10.36.101. Ran `00_setup.sql` (as ACCOUNTADMIN), `01_stage.sql`, `03_raw_tables.sql` (as DW_ROLE) with `python -m scripts.run_sql`. Every statement succeeded.

| Object | Verified state |
|---|---|
| Role `DW_ROLE` | Created, granted to the user and to SYSADMIN; owns `BROKER_DW`; has EXECUTE TASK |
| Warehouse `DW_WH` | X-Small, SUSPENDED, auto-suspend 60 s, resource monitor `DW_MONITOR` |
| Resource monitor `DW_MONITOR` | 20 credits/month, suspend at 100%; 0.00 credits used |
| Database `BROKER_DW` | Schemas AUDIT, CORE, MART, RAW, STAGE |
| File format `RAW.FF_CSV` | CSV, header skipped, empty field as NULL |
| Stage `RAW.LANDING` | Internal, server-side encrypted (SNOWFLAKE_SSE), directory table enabled |
| RAW tables | ACCOUNTS, TRADES, CONTROL_TOTALS, SECURITIES (business columns VARCHAR + load metadata) |

## Step 3: Snowpipe load of day 1 (2026-10-14)

Created `RAW.PIPE_ACCOUNTS`, `RAW.PIPE_TRADES`, `RAW.PIPE_CONTROL_TOTALS`, `RAW.PIPE_SECURITIES` (`02_pipes.sql`).
Ran `python -m generator.upload_to_stage --date 2026-09-01 --reference --wait`: 4 files PUT to `@RAW.LANDING` (gzip), 4 `insertFiles` REST calls returned `SUCCESS`.

Load history (from `INFORMATION_SCHEMA.COPY_HISTORY`):

| File | Table | Status | Rows | Errors | Received to loaded |
|---|---|---|---|---|---|
| reference/securities.csv.gz | RAW.SECURITIES | Loaded | 100 | 0 | 22.1 s |
| accounts/accounts_20260901.csv.gz | RAW.ACCOUNTS | Loaded | 500 | 0 | 24.5 s |
| trades/trades_20260901.csv.gz | RAW.TRADES | Loaded | 2,068 | 0 | 19.6 s |
| control/control_20260901.csv.gz | RAW.CONTROL_TOTALS | Loaded | 2 | 0 | 24.7 s |

Reconciliation, files vs RAW: all 4 row counts match. Trade control totals match the sender's control file exactly (2,068 rows, quantity 1,022,869, notional 253,469,756.56).
All 8 injected bad rows for the day are present in RAW, as intended (2 missing account_id, 2 negative quantity, 2 unknown security, 2 duplicate trade_id); they are rejected downstream, not at load.

Idempotency: re-running the same upload, PUT reported `SKIPPED` for all 4 files and the pipes loaded nothing new (row counts unchanged after 45 s; `SYSTEM$PIPE_STATUS` pendingFileCount 0).

## Step 4: Streams (2026-10-14)

Created 4 APPEND_ONLY streams with SHOW_INITIAL_ROWS = TRUE (`04_streams.sql`). Verified by reading them without consuming:

| Stream | Stream rows | Table rows | Inserts | Updates | Has data |
|---|---|---|---|---|---|
| RAW.STRM_SECURITIES | 100 | 100 | 100 | 0 | TRUE |
| RAW.STRM_ACCOUNTS | 500 | 500 | 500 | 0 | TRUE |
| RAW.STRM_TRADES | 2,068 | 2,068 | 2,068 | 0 | TRUE |
| RAW.STRM_CONTROL_TOTALS | 2 | 2 | 2 | 0 | TRUE |

The day 1 rows loaded before the streams existed are all visible, because of SHOW_INITIAL_ROWS. A second SELECT on STRM_TRADES still returned 2,068, which confirms that reading does not advance the offset. All streams are `stale = false`, and stale_after is 2026-10-28 (14 days out) unless they are consumed earlier.
