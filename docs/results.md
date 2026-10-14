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
