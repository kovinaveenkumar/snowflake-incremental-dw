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

## Step 5: SCD Type 2 account dimension (2026-10-14)

**SCD2 tests** (`RUN_SNOWFLAKE_TESTS=1 pytest tests/test_scd2_logic.py`, in a temporary transient `BROKER_DW_TEST`, dropped afterwards): **8 passed** in 47 s.
They cover: new, changed, unchanged, rejected (missing id, invalid status), a multi-day catch-up batch chained in date order, out-of-order rows rejected, identical replay ignored, an empty run, and the invariants (exactly one current row per account, contiguous non-overlapping ranges, unique surrogate keys).

Engineering note: with `SHOW_INITIAL_ROWS = TRUE`, the first consuming read returns only the rows present when the stream was created; rows inserted after creation arrive on the next read. The tests cover this (run 0) and `04_streams.sql` documents it. In BROKER_DW, day 1 was that initial snapshot.

**Day 1 merge on BROKER_DW** (`CALL CORE.SP_LOAD_DIM_ACCOUNT()`, 6.3 s round trip, 4.3 s inside the procedure):

| Metric | Value |
|---|---|
| Rows read from RAW.STRM_ACCOUNTS | 500 |
| Rejected | 0 |
| Versions inserted / closed | 500 / 0 |
| DIM_ACCOUNT rows / distinct accounts / current | 500 / 500 / 500 |
| Accounts with more than one current row | 0 |
| effective_from / effective_to | 2026-09-01 / 9999-12-31 for all |
| Stream after the run | 0 rows, SYSTEM$STREAM_HAS_DATA = FALSE |

Change history (closed versions) starts once days 2 and later are loaded.

## Step 6: DIM_SECURITY and FACT_TRADE (2026-10-14)

**Snowflake-backed tests** (SCD2 + fact, temporary `BROKER_DW_TEST`): **16 passed** in 131 s.
Testing identified that `SP_LOAD_DIM_ACCOUNT` inserted into `AUDIT.LOAD_LOG` by column position, which breaks when Step 6 adds the `rows_inserted`/`rows_updated` columns. The insert now names its columns and was redeployed before Step 6 (DIM_ACCOUNT data unchanged, 500 rows).

**Day 1 load on BROKER_DW**, in Task-tree order:

| Procedure | Time | Result |
|---|---|---|
| SP_LOAD_DIM_SECURITY | 4.9 s | 100 read, 100 inserted, 0 updated, 0 rejected |
| SP_LOAD_DIM_ACCOUNT | 4.8 s | 0 read (already loaded in Step 5) |
| SP_LOAD_FACT_TRADE | 6.6 s | 2,068 read, 2,060 inserted, 8 rejected |

**Checks:**
- Rejected trades match the generator's manifest exactly, by trade_id and reason: 2 MISSING_ACCOUNT_ID, 2 NEGATIVE_QUANTITY, 2 UNKNOWN_SECURITY, 2 DUPLICATE_TRADE_ID.
- Control totals reconcile: fact + rejected = 2,060 + 8 = 2,068 rows; quantity 1,021,133 + 1,736 = 1,022,869; notional 252,945,194.31 + 524,562.25 = 253,469,756.56. All three match the sender's control file.
- Trade IDs in FACT_TRADE are unique. There are 0 orphan keys, and 0 trades are linked to a non-active account version or to a version outside its date range.
- By side: BUY 1,018 trades (signed notional +124,553,716.87), SELL 1,042 trades (−128,391,477.44).

## Step 7: FACT_DAILY_POSITION snapshot (2026-10-14)

**Full test suite** (`RUN_SNOWFLAKE_TESTS=1 pytest`): **44 passed** in 268 s. That is 22 offline tests plus 22 Snowflake-backed (8 SCD2, 8 trade fact, 6 position snapshot). The position tests passed on the first run. They cover the close price from the last trade by time, short positions, a position closed to zero, carry-forward with no trades, the account version on the snapshot date, a late trade that rebuilds from its date, an empty run, and an independent running-sum check.

**Day 1 snapshot on BROKER_DW** (`CALL CORE.SP_LOAD_FACT_DAILY_POSITION()`, 4.9 s). The new stream `CORE.STRM_FACT_TRADE` delivered all 2,060 day 1 trades on its first read:

| Metric | Value |
|---|---|
| Snapshot rows (account x security) for 2026-09-01 | 2,014 |
| Accounts / securities with a position | 494 / 100 |
| Long / short / flat rows | 998 / 1,016 / 0 |
| Gross long / short market value | 122,146,020.87 / −125,970,094.66 |

**Checks against FACT_TRADE:**
- Trade count: 2,060 in the snapshot = 2,060 in the fact.
- Net quantity: −10,403 = −10,403.
- Net notional: −3,837,760.57 = −3,837,760.57.
- 0 rows differ from the independent running sum of trades, 0 duplicate keys, 0 missing close prices, and the stream is empty afterwards.

Positions are short-heavy because the synthetic generator lets accounts sell what they do not hold (documented in `07_fact_daily_position.sql`).

## Step 8: Scheduled Task graph (2026-10-14)

**Task tests** (`RUN_SNOWFLAKE_TESTS=1 pytest tests/test_tasks.py`): **3 passed** in 77 s. The real graph ran in `BROKER_DW_TEST`, all 4 tasks SUCCEEDED and every layer loaded. A second run with no new data was SKIPPED by the root's WHEN condition ("Conditional expression for task evaluated to false"), so no warehouse ran and no load happened.
Engineering note: SKIPPED runs are not listed in `INFORMATION_SCHEMA.COMPLETE_TASK_GRAPHS`, and task names can repeat across databases, so `scripts/tasks.py run-once --wait` reads the root task's final state from `TASK_HISTORY`, filtered to the current database.

**BROKER_DW:** 4 tasks created and resumed (`python -m scripts.tasks resume`): TASK_ROOT every 5 minutes, then DIM_ACCOUNT, FACT_TRADE, FACT_DAILY_POSITION. The schedule is left running.

**No-manual-steps proof, day 2:** the only action was `python -m generator.upload_to_stage --date 2026-09-02` at 14:01:25 (PUT + Snowpipe notify). No procedure was called by hand. The first scheduled tick after resume, at 14:06:12, ran the graph: all 4 tasks SUCCEEDED, complete at 14:06:41 (root 4.9 s, account 7.8 s, trades 8.3 s, positions 5.5 s).

| Layer | Day 2 result from the scheduled run |
|---|---|
| DIM_ACCOUNT (SCD2) | 9 rows read: 5 changed (5 versions closed, 5 inserted), 1 new account, 3 unchanged re-sends ignored. Totals: 506 rows, 501 current, 5 closed, 0 accounts with 2 current rows |
| FACT_TRADE | 1,992 read, 1,985 inserted, 7 rejected (2 duplicate, 1 missing account, 2 negative quantity, 2 unknown security). Rejects match the manifest exactly |
| Control totals | 1,985 + 7 = 1,992 rows, and notional 236,761,613.76 + (−48,630.78) = 236,712,982.98, matching the sender's control file |
| FACT_DAILY_POSITION | Rebuilt from 2026-09-02 only (0 day 1 rows replaced), 3,899 rows inserted. Total 5,913 rows over 2 days, 0 differences from the running sum of trades |
| Streams | All RAW streams empty afterwards |

Example SCD2 history: A000059 moved from Atlanta (2026-09-01 to 2026-09-01, closed) to Boston (2026-09-02 to 9999-12-31, current).

## Step 9: Reconciliation, control totals and DQ checks (2026-10-14)

**Tests:** `tests/test_data_quality.py` **5 passed** (181 s).
The DQ tests cover exact reconciliation with a reject-rate WARN, a sender control total $1.00 off (raises an error, results kept, file marked FAIL, LOAD_LOG `DQ_FAILED`), a control file arriving before its data (PENDING), data landed but not loaded (PENDING), then PASS, closed files not re-checked, and a deliberately corrupted DIM_ACCOUNT failing 2 global checks.

**Deployed to BROKER_DW:** schedule suspended, `08_tasks.sql` re-run (root now also wakes on `RAW.STRM_CONTROL_TOTALS`), `09_data_quality.sql` run, schedule resumed. The graph now has 5 tasks: ROOT → DIM_ACCOUNT → FACT_TRADE → FACT_DAILY_POSITION → RUN_DQ_CHECKS.

**Days 1 and 2 reconciled.** `CALL AUDIT.SP_RUN_DQ_CHECKS()` was run twice, because the control stream's first read returns only its initial snapshot (day 1) and day 2 arrives on the second read. Each run took 5-6 s and gave 14 PASS, 0 FAIL, 0 WARN, 0 PENDING.

| File | Sender control | RAW | FACT + rejected | Result |
|---|---|---|---|---|
| trades_20260901.csv | 2,068 rows / 1,022,869 qty / 253,469,756.56 | same | 2,060 + 8 rows / 1,021,133 + 1,736 / 252,945,194.31 + 524,562.25 | 7 checks PASS, reject rate 0.39 % |
| trades_20260902.csv | 1,992 / 980,965 / 236,712,982.98 | same | 1,985 + 7 / 980,800 + 165 / 236,761,613.76 + (−48,630.78) | 7 checks PASS, reject rate 0.35 % |
| accounts_20260901.csv | 500 rows | 500 | n/a | PASS |
| accounts_20260902.csv | 9 rows | 9 | n/a | PASS |

All 6 global checks passed on both runs: one current version per account, contiguous ranges, unique trade_id, no orphans, account version in force at trade date, positions equal to the running sum of trades. AUDIT.CONTROL_REGISTER shows all 4 files closed as PASS.

## Step 10: Snowpark data-quality check (2026-10-14)

`snowpark/dq_checks.py` flags price outliers ("fat-finger" trades) with the Snowpark DataFrame API. Each trade's price is compared with the leave-one-out VWAP of the other trades in the same security on the same day, and trades more than 5 % off are flagged (minimum 2 peers). The DataFrame code is pushed down to Snowflake as SQL.

**Tests:** `tests/test_snowpark_dq.py` gave **4 passed**: 3 offline in Snowpark local testing mode, plus 1 in `BROKER_DW_TEST` (69.5 s).
Engineering note: Snowpark local testing mode does not implement `round`, so rounding is applied by the `NUMBER(38,4)` output column.

**BROKER_DW:** created `AUDIT.PRICE_OUTLIERS` and ran `python -m snowpark.dq_checks` (5.8 s end to end):

| Metric | Value |
|---|---|
| Trades evaluated (all had at least 2 same-day peers) | 4,045 of 4,045 |
| Flagged at 5 % | 0 → DQ_RESULTS `SNOWPARK_PRICE_OUTLIERS` = PASS |
| Deviation from peer VWAP: max / p99 / mean | 0.6829 % / 0.5774 % / 0.2592 % |
| Trades that would be flagged at 0.5 % / 1 % / 2 % | 238 / 0 / 0 |

A 5 % threshold has about 7x headroom over normal price noise in this data (the generator spreads trade prices ±0.5 % around a daily price), so a real fat-finger error stands out. The check runs from the client; it is not in the Task graph.
