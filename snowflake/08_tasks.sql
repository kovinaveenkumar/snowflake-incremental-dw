-- Step 8: Task graph that runs the whole load on a schedule, with no manual steps.
--
--   CORE.TASK_ROOT                      every 5 minutes, only WHEN a RAW stream has data
--     -> CORE.TASK_LOAD_DIM_ACCOUNT     (root itself loads DIM_SECURITY)
--       -> CORE.TASK_LOAD_FACT_TRADE
--         -> CORE.TASK_LOAD_FACT_DAILY_POSITION
--         (Step 9 adds the data-quality task here, in parallel with the position snapshot)
--
-- Cost: the WHEN condition is checked by Snowflake's cloud services layer without starting the
-- warehouse; when no new file has landed, the run is SKIPPED and DW_WH stays suspended. The
-- warehouse only runs when Snowpipe has loaded something.
-- Safety: no overlapping runs; root retries a failed graph twice; any task that fails 3 runs in a
-- row is suspended automatically. Each procedure is one transaction, so a failed run leaves the
-- streams unconsumed and the next run picks the same rows up again.
--
-- Tasks are created SUSPENDED. Start / stop / inspect with:  python -m scripts.tasks resume|suspend|status|run-once
-- Re-running this file replaces the tasks: suspend the graph first (python -m scripts.tasks suspend).

USE ROLE DW_ROLE;
USE DATABASE BROKER_DW;

CREATE OR REPLACE TASK CORE.TASK_ROOT
  WAREHOUSE = DW_WH
  SCHEDULE = '5 MINUTE'
  ALLOW_OVERLAPPING_EXECUTION = FALSE
  SUSPEND_TASK_AFTER_NUM_FAILURES = 3
  TASK_AUTO_RETRY_ATTEMPTS = 2
  USER_TASK_TIMEOUT_MS = 900000
  COMMENT = 'Root of the load graph; loads CORE.DIM_SECURITY'
  WHEN SYSTEM$STREAM_HAS_DATA('RAW.STRM_SECURITIES')
    OR SYSTEM$STREAM_HAS_DATA('RAW.STRM_ACCOUNTS')
    OR SYSTEM$STREAM_HAS_DATA('RAW.STRM_TRADES')
AS
  CALL CORE.SP_LOAD_DIM_SECURITY();

CREATE OR REPLACE TASK CORE.TASK_LOAD_DIM_ACCOUNT
  WAREHOUSE = DW_WH
  USER_TASK_TIMEOUT_MS = 900000
  COMMENT = 'SCD2 merge into CORE.DIM_ACCOUNT'
  AFTER CORE.TASK_ROOT
AS
  CALL CORE.SP_LOAD_DIM_ACCOUNT();

CREATE OR REPLACE TASK CORE.TASK_LOAD_FACT_TRADE
  WAREHOUSE = DW_WH
  USER_TASK_TIMEOUT_MS = 900000
  COMMENT = 'Validate trades, reject bad rows, load CORE.FACT_TRADE'
  AFTER CORE.TASK_LOAD_DIM_ACCOUNT
AS
  CALL CORE.SP_LOAD_FACT_TRADE();

CREATE OR REPLACE TASK CORE.TASK_LOAD_FACT_DAILY_POSITION
  WAREHOUSE = DW_WH
  USER_TASK_TIMEOUT_MS = 900000
  COMMENT = 'Rebuild CORE.FACT_DAILY_POSITION from the earliest new trade date'
  AFTER CORE.TASK_LOAD_FACT_TRADE
AS
  CALL CORE.SP_LOAD_FACT_DAILY_POSITION();

SHOW TASKS IN SCHEMA CORE;
