-- Step 4: change-tracking streams on the RAW tables. Each downstream load reads only rows that
-- arrived since its last successful run; the stream offset advances only when a DML statement
-- that reads the stream commits, so a failed load is simply retried on the next run.
--
-- APPEND_ONLY: Snowpipe only ever inserts into RAW, so insert-only streams are enough and cheaper.
-- SHOW_INITIAL_ROWS: day 1 was loaded before these streams existed; without this flag those rows
--   would never be seen downstream. The first consuming read returns ONLY that initial snapshot
--   (rows present when the stream was created); rows inserted after creation arrive on the next
--   read. Nothing is lost, it is one run later (verified in tests/test_scd2_logic.py, run 0).
-- One stream per consumer: a stream has a single offset, so each table that reads RAW gets its own.
-- Staleness: a stream must be consumed within the table's retention plus
--   MAX_DATA_EXTENSION_TIME_IN_DAYS (default 14 days); the Task tree in Step 8 runs far more often.

USE ROLE DW_ROLE;
USE DATABASE BROKER_DW;

CREATE STREAM IF NOT EXISTS RAW.STRM_ACCOUNTS
  ON TABLE RAW.ACCOUNTS
  APPEND_ONLY = TRUE
  SHOW_INITIAL_ROWS = TRUE
  COMMENT = 'New account rows -> CORE.DIM_ACCOUNT (SCD2 merge, Step 5)';

CREATE STREAM IF NOT EXISTS RAW.STRM_TRADES
  ON TABLE RAW.TRADES
  APPEND_ONLY = TRUE
  SHOW_INITIAL_ROWS = TRUE
  COMMENT = 'New trade rows -> validation, AUDIT.REJECTED_ROWS and CORE.FACT_TRADE (Step 6)';

CREATE STREAM IF NOT EXISTS RAW.STRM_CONTROL_TOTALS
  ON TABLE RAW.CONTROL_TOTALS
  APPEND_ONLY = TRUE
  SHOW_INITIAL_ROWS = TRUE
  COMMENT = 'New sender control totals -> reconciliation (Step 9)';

CREATE STREAM IF NOT EXISTS RAW.STRM_SECURITIES
  ON TABLE RAW.SECURITIES
  APPEND_ONLY = TRUE
  SHOW_INITIAL_ROWS = TRUE
  COMMENT = 'Security master changes -> CORE.DIM_SECURITY (Step 6)';

SHOW STREAMS IN SCHEMA RAW;
