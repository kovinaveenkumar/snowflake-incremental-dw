-- Step 9: reconciliation, control totals and data-quality checks that fail loudly.
--
-- Control register: every control file the sender delivers (RAW.STRM_CONTROL_TOTALS) is registered in
-- AUDIT.CONTROL_REGISTER as PENDING. Each DQ run re-checks the PENDING files and closes them as PASS or FAIL.
--
-- Per trades file (the three-way reconciliation, sender control -> RAW -> CORE):
--   FILE_VS_RAW_ROWS / _QUANTITY / _NOTIONAL   sender control totals = what landed in RAW.TRADES
--   RAW_VS_CORE_ROWS                           RAW rows = FACT_TRADE rows + rejected rows (nothing lost)
--   CONTROL_VS_CORE_QUANTITY / _NOTIONAL       sender totals = loaded + rejected (every unit accounted for)
--   REJECT_RATE_PCT                            WARN (not FAIL) above 2 %
-- Per accounts file: FILE_VS_RAW_ROWS.
-- A file stays PENDING (not FAIL) while it has not fully landed or been loaded yet: no RAW rows, or RAW
-- rows not yet processed while RAW.STRM_TRADES still holds data. Snowpipe and the Task schedule are
-- asynchronous, so a control file can arrive before its data file.
--
-- Global checks, every run: exactly one current DIM_ACCOUNT row per account; contiguous version ranges;
-- unique trade_id; no orphan keys; trades linked to an active account version in force on the trade date;
-- every FACT_DAILY_POSITION row equals the running sum of its trades.
--
-- Fail loudly: results are COMMITTED to AUDIT.DQ_RESULTS first, then the procedure raises an error, so the
-- Task run shows FAILED (and after 3 failures in a row the task suspends itself), AUDIT.LOAD_LOG records
-- FAILED, and the evidence stays queryable. WARN results do not fail the run.
--
-- The DQ task joins the Task graph after the position snapshot. Changing the graph needs the root suspended:
--   python -m scripts.tasks suspend; run 08_tasks.sql then this file; python -m scripts.tasks resume

USE ROLE DW_ROLE;
USE DATABASE BROKER_DW;

CREATE TABLE IF NOT EXISTS AUDIT.CONTROL_REGISTER (
  file_name       VARCHAR,
  file_date       DATE,
  row_count       NUMBER,
  total_quantity  NUMBER(18,0),
  total_notional  NUMBER(20,2),
  status          VARCHAR,       -- PENDING / PASS / FAIL
  registered_at   TIMESTAMP_LTZ,
  closed_at       TIMESTAMP_LTZ,
  closed_by_run   VARCHAR
) COMMENT = 'Sender control files and their reconciliation status';

CREATE TABLE IF NOT EXISTS AUDIT.DQ_RESULTS (
  run_id      VARCHAR,
  checked_at  TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  check_name  VARCHAR,
  scope       VARCHAR,          -- file name, or GLOBAL
  expected    NUMBER(38,4),
  actual      NUMBER(38,4),
  status      VARCHAR,          -- PASS / FAIL / WARN / PENDING
  details     VARCHAR
) COMMENT = 'One row per data-quality check per run';

-- Live three-way reconciliation per trades file: sender control vs RAW vs CORE (+ rejected).
CREATE OR REPLACE VIEW AUDIT.V_TRADE_FILE_RECON AS
WITH ctl AS (
  SELECT file_name, file_date, row_count, total_quantity, total_notional
    FROM AUDIT.CONTROL_REGISTER
   WHERE STARTSWITH(file_name, 'trades_')
),
raw AS (
  SELECT REGEXP_REPLACE(SPLIT_PART(_file_name, '/', -1), '\\.gz$', '') AS file_name,
         COUNT(*) AS raw_rows, SUM(TRY_TO_NUMBER(quantity, 18, 0)) AS raw_quantity,
         SUM(TRY_TO_NUMBER(notional, 20, 2)) AS raw_notional
    FROM RAW.TRADES GROUP BY 1
),
fact AS (
  SELECT REGEXP_REPLACE(SPLIT_PART(source_file, '/', -1), '\\.gz$', '') AS file_name,
         COUNT(*) AS fact_rows, SUM(quantity) AS fact_quantity, SUM(notional) AS fact_notional
    FROM CORE.FACT_TRADE GROUP BY 1
),
rej AS (
  SELECT REGEXP_REPLACE(SPLIT_PART(source_file, '/', -1), '\\.gz$', '') AS file_name,
         COUNT(*) AS rejected_rows, SUM(TRY_TO_NUMBER(record:quantity::VARCHAR, 18, 0)) AS rejected_quantity,
         SUM(TRY_TO_NUMBER(record:notional::VARCHAR, 20, 2)) AS rejected_notional
    FROM AUDIT.REJECTED_ROWS WHERE source_table = 'RAW.TRADES' GROUP BY 1
)
SELECT c.file_name, c.file_date,
       c.row_count AS ctl_rows, c.total_quantity AS ctl_quantity, c.total_notional AS ctl_notional,
       COALESCE(r.raw_rows, 0) AS raw_rows, COALESCE(r.raw_quantity, 0) AS raw_quantity,
       COALESCE(r.raw_notional, 0) AS raw_notional,
       COALESCE(f.fact_rows, 0) AS fact_rows, COALESCE(f.fact_quantity, 0) AS fact_quantity,
       COALESCE(f.fact_notional, 0) AS fact_notional,
       COALESCE(j.rejected_rows, 0) AS rejected_rows, COALESCE(j.rejected_quantity, 0) AS rejected_quantity,
       COALESCE(j.rejected_notional, 0) AS rejected_notional
  FROM ctl c
  LEFT JOIN raw r  ON r.file_name = c.file_name
  LEFT JOIN fact f ON f.file_name = c.file_name
  LEFT JOIN rej j  ON j.file_name = c.file_name;

CREATE OR REPLACE VIEW AUDIT.V_ACCOUNT_FILE_RECON AS
SELECT c.file_name, c.file_date, c.row_count AS ctl_rows, COALESCE(r.raw_rows, 0) AS raw_rows
  FROM AUDIT.CONTROL_REGISTER c
  LEFT JOIN (SELECT REGEXP_REPLACE(SPLIT_PART(_file_name, '/', -1), '\\.gz$', '') AS file_name, COUNT(*) AS raw_rows
               FROM RAW.ACCOUNTS GROUP BY 1) r
    ON r.file_name = c.file_name
 WHERE STARTSWITH(c.file_name, 'accounts_');

CREATE OR REPLACE PROCEDURE AUDIT.SP_RUN_DQ_CHECKS()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  run_id          VARCHAR DEFAULT UUID_STRING();
  started         TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP();
  trades_waiting  BOOLEAN DEFAULT FALSE;
  n_registered    NUMBER DEFAULT 0;
  n_pass          NUMBER DEFAULT 0;
  n_fail          NUMBER DEFAULT 0;
  n_warn          NUMBER DEFAULT 0;
  n_pending       NUMBER DEFAULT 0;
  dq_failed       EXCEPTION (-20001, 'Data-quality checks FAILED: see AUDIT.DQ_RESULTS for the latest run_id');
BEGIN
  SELECT SYSTEM$STREAM_HAS_DATA('RAW.STRM_TRADES') INTO :trades_waiting;

  BEGIN TRANSACTION;

  -- 1. Register newly delivered control files.
  INSERT INTO AUDIT.CONTROL_REGISTER (file_name, file_date, row_count, total_quantity, total_notional, status, registered_at)
  SELECT file_name, TRY_TO_DATE(file_date), TRY_TO_NUMBER(row_count),
         TRY_TO_NUMBER(total_quantity, 18, 0), TRY_TO_NUMBER(total_notional, 20, 2), 'PENDING', CURRENT_TIMESTAMP()
    FROM RAW.STRM_CONTROL_TOTALS;
  n_registered := SQLROWCOUNT;

  -- 2. Per-file reconciliation for every open (PENDING) control file.
  INSERT INTO AUDIT.DQ_RESULTS (run_id, check_name, scope, expected, actual, status, details)
  WITH open_trades AS (
    SELECT v.*,
           (v.raw_rows = 0 OR (v.raw_rows > v.fact_rows + v.rejected_rows AND :trades_waiting)) AS not_ready
      FROM AUDIT.V_TRADE_FILE_RECON v
      JOIN AUDIT.CONTROL_REGISTER c ON c.file_name = v.file_name AND c.status = 'PENDING'
  ),
  checks AS (
    SELECT file_name, not_ready, 'FILE_VS_RAW_ROWS' AS check_name, ctl_rows AS expected, raw_rows AS actual FROM open_trades
    UNION ALL SELECT file_name, not_ready, 'FILE_VS_RAW_QUANTITY', ctl_quantity, raw_quantity FROM open_trades
    UNION ALL SELECT file_name, not_ready, 'FILE_VS_RAW_NOTIONAL', ctl_notional, raw_notional FROM open_trades
    UNION ALL SELECT file_name, not_ready, 'RAW_VS_CORE_ROWS', raw_rows, fact_rows + rejected_rows FROM open_trades
    UNION ALL SELECT file_name, not_ready, 'CONTROL_VS_CORE_QUANTITY', ctl_quantity, fact_quantity + rejected_quantity FROM open_trades
    UNION ALL SELECT file_name, not_ready, 'CONTROL_VS_CORE_NOTIONAL', ctl_notional, fact_notional + rejected_notional FROM open_trades
    UNION ALL
    SELECT v.file_name, v.raw_rows = 0, 'FILE_VS_RAW_ROWS', v.ctl_rows, v.raw_rows
      FROM AUDIT.V_ACCOUNT_FILE_RECON v
      JOIN AUDIT.CONTROL_REGISTER c ON c.file_name = v.file_name AND c.status = 'PENDING'
  )
  SELECT :run_id, check_name, file_name, expected, actual,
         CASE WHEN not_ready THEN 'PENDING' WHEN expected = actual THEN 'PASS' ELSE 'FAIL' END,
         CASE WHEN not_ready THEN 'file not fully landed/loaded yet'
              WHEN expected <> actual THEN 'difference ' || TO_VARCHAR(actual - expected) END
    FROM checks
  UNION ALL
  SELECT :run_id, 'REJECT_RATE_PCT', file_name, 2, ROUND(100 * rejected_rows / NULLIF(raw_rows, 0), 4),
         CASE WHEN not_ready THEN 'PENDING' WHEN 100 * rejected_rows / NULLIF(raw_rows, 0) > 2 THEN 'WARN' ELSE 'PASS' END,
         rejected_rows || ' of ' || raw_rows || ' rows rejected (threshold 2 %)'
    FROM open_trades;

  -- 3. Global integrity checks, every run.
  INSERT INTO AUDIT.DQ_RESULTS (run_id, check_name, scope, expected, actual, status, details)
  WITH g AS (
    SELECT 'DIM_ACCOUNT_ONE_CURRENT_ROW' AS check_name,
           (SELECT COUNT(*) FROM (SELECT account_id FROM CORE.DIM_ACCOUNT GROUP BY account_id
                                  HAVING COUNT_IF(is_current) <> 1)) AS bad,
           'accounts without exactly one current version' AS details
    UNION ALL
    SELECT 'DIM_ACCOUNT_CONTIGUOUS_RANGES',
           (SELECT COUNT(*) FROM (
              SELECT effective_to, is_current,
                     LEAD(effective_from) OVER (PARTITION BY account_id ORDER BY effective_from) AS next_from
                FROM CORE.DIM_ACCOUNT)
             WHERE (next_from IS NOT NULL AND (next_from - effective_to <> 1 OR is_current))
                OR (next_from IS NULL AND (effective_to <> '9999-12-31'::DATE OR NOT is_current))),
           'versions with a gap, overlap or wrong open end'
    UNION ALL
    SELECT 'FACT_TRADE_UNIQUE_TRADE_ID',
           (SELECT COUNT(*) - COUNT(DISTINCT trade_id) FROM CORE.FACT_TRADE), 'duplicate trade_id rows'
    UNION ALL
    SELECT 'FACT_TRADE_NO_ORPHANS',
           (SELECT COUNT(*) FROM CORE.FACT_TRADE f
              LEFT JOIN CORE.DIM_ACCOUNT a ON a.account_sk = f.account_sk
              LEFT JOIN CORE.DIM_SECURITY s ON s.security_sk = f.security_sk
             WHERE a.account_sk IS NULL OR s.security_sk IS NULL), 'trades with a missing dimension row'
    UNION ALL
    SELECT 'FACT_TRADE_ACCOUNT_VERSION_AT_TRADE_DATE',
           (SELECT COUNT(*) FROM CORE.FACT_TRADE f JOIN CORE.DIM_ACCOUNT a ON a.account_sk = f.account_sk
             WHERE a.status <> 'ACTIVE' OR f.trade_date NOT BETWEEN a.effective_from AND a.effective_to),
           'trades linked to a non-active or out-of-range account version'
    UNION ALL
    SELECT 'POSITION_EQUALS_RUNNING_SUM_OF_TRADES',
           (SELECT COUNT(*) FROM (
              SELECT p.position_date, p.account_id, p.security_sk, p.position_quantity,
                     COALESCE(SUM(t.qty), 0) AS expected_quantity
                FROM CORE.FACT_DAILY_POSITION p
                LEFT JOIN (SELECT a.account_id, f.security_sk, f.trade_date, SUM(f.signed_quantity) AS qty
                             FROM CORE.FACT_TRADE f JOIN CORE.DIM_ACCOUNT a ON a.account_sk = f.account_sk
                            GROUP BY 1, 2, 3) t
                  ON t.account_id = p.account_id AND t.security_sk = p.security_sk AND t.trade_date <= p.position_date
               GROUP BY 1, 2, 3, 4
              HAVING p.position_quantity <> expected_quantity)),
           'snapshot rows that differ from the running sum of trades'
  )
  SELECT :run_id, check_name, 'GLOBAL', 0, bad, IFF(bad = 0, 'PASS', 'FAIL'), IFF(bad = 0, NULL, bad || ' ' || details)
    FROM g;

  -- 4. Close the control files that are no longer pending.
  UPDATE AUDIT.CONTROL_REGISTER c
     SET status = s.file_status, closed_at = CURRENT_TIMESTAMP(), closed_by_run = :run_id
    FROM (SELECT scope,
                 CASE WHEN COUNT_IF(status = 'FAIL') > 0 THEN 'FAIL'
                      WHEN COUNT_IF(status = 'PENDING') > 0 THEN 'PENDING'
                      ELSE 'PASS' END AS file_status
            FROM AUDIT.DQ_RESULTS WHERE run_id = :run_id AND scope <> 'GLOBAL'
           GROUP BY scope) s
   WHERE c.file_name = s.scope AND c.status = 'PENDING' AND s.file_status <> 'PENDING';

  SELECT COUNT_IF(status = 'PASS'), COUNT_IF(status = 'FAIL'), COUNT_IF(status = 'WARN'), COUNT_IF(status = 'PENDING')
    INTO :n_pass, :n_fail, :n_warn, :n_pending
    FROM AUDIT.DQ_RESULTS WHERE run_id = :run_id;

  INSERT INTO AUDIT.LOAD_LOG (load_run_id, procedure_name, started_at, finished_at, status, rows_read, message)
  VALUES (:run_id, 'AUDIT.SP_RUN_DQ_CHECKS', :started, CURRENT_TIMESTAMP(), IFF(:n_fail > 0, 'DQ_FAILED', 'SUCCESS'),
          :n_registered, :n_pass || ' pass, ' || :n_fail || ' fail, ' || :n_warn || ' warn, ' || :n_pending || ' pending');

  COMMIT;   -- keep the evidence even when failing

  IF (n_fail > 0) THEN
    RAISE dq_failed;
  END IF;

  RETURN OBJECT_CONSTRUCT('run_id', run_id, 'controls_registered', n_registered, 'pass', n_pass,
                          'fail', n_fail, 'warn', n_warn, 'pending', n_pending);
EXCEPTION
  WHEN dq_failed THEN
    RAISE;
  WHEN OTHER THEN
    ROLLBACK;
    INSERT INTO AUDIT.LOAD_LOG (load_run_id, procedure_name, started_at, finished_at, status, message)
    VALUES (:run_id, 'AUDIT.SP_RUN_DQ_CHECKS', :started, CURRENT_TIMESTAMP(), 'FAILED', :SQLERRM);
    RAISE;
END;
$$;

CREATE OR REPLACE TASK CORE.TASK_RUN_DQ_CHECKS  -- tasks of one graph must share a schema
  WAREHOUSE = DW_WH
  USER_TASK_TIMEOUT_MS = 900000
  COMMENT = 'Reconciliation, control totals and integrity checks; FAILS the graph run on any failed check'
  AFTER CORE.TASK_LOAD_FACT_DAILY_POSITION
AS
  CALL AUDIT.SP_RUN_DQ_CHECKS();
