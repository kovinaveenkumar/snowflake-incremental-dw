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
