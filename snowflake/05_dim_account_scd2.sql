-- Step 5: CORE.DIM_ACCOUNT as SCD Type 2, loaded from RAW.STRM_ACCOUNTS by CORE.SP_LOAD_DIM_ACCOUNT().
--
-- Rules
--   * Versions are dated by the business date of the file (file_date), not the load time, so a trade can
--     be joined to the account version in force on its trade_date (Step 6).
--   * Change detection: hash_diff = MD5 over the tracked attributes. A row whose hash equals the
--     previous version is "unchanged" and creates nothing.
--   * New account      -> insert version, effective_from = file_date, effective_to = 9999-12-31, is_current.
--     Changed account  -> close the current version (effective_to = new effective_from - 1 day,
--                         is_current = FALSE) and insert the new one.
--     Unchanged        -> no-op.
--   * Catch-up batches: if the stream holds several days for one account (e.g. the schedule was paused),
--     the versions are chained in file_date order inside the same run.
--   * Rows that fail validation, or arrive out of order (file_date on or before the current version
--     with different values), go to AUDIT.REJECTED_ROWS. An identical replay is ignored.
--   * All of it runs in one transaction; the stream offset only advances if the whole run commits.
-- Snowflake does not enforce PRIMARY KEY / UNIQUE; they are declared for documentation and BI tools,
-- and the invariants (one current row per account, no overlapping ranges) are asserted by the tests.

USE ROLE DW_ROLE;
USE DATABASE BROKER_DW;

-- Shared audit tables (also used by Steps 6 and 9).
CREATE TABLE IF NOT EXISTS AUDIT.REJECTED_ROWS (
  rejected_at        TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  load_run_id        VARCHAR,
  source_table       VARCHAR,
  source_file        VARCHAR,
  source_row_number  NUMBER,
  business_key       VARCHAR,
  reject_reason      VARCHAR,
  record             VARIANT
) COMMENT = 'Rows rejected by validation, one row per rejected source row';

CREATE TABLE IF NOT EXISTS AUDIT.LOAD_LOG (
  load_run_id        VARCHAR,
  procedure_name     VARCHAR,
  started_at         TIMESTAMP_LTZ,
  finished_at        TIMESTAMP_LTZ,
  status             VARCHAR,
  rows_read          NUMBER,
  rows_rejected      NUMBER,
  rows_unchanged     NUMBER,
  versions_closed    NUMBER,
  versions_inserted  NUMBER,
  message            VARCHAR
) COMMENT = 'One row per load procedure run';

CREATE SEQUENCE IF NOT EXISTS CORE.SEQ_ACCOUNT_SK COMMENT = 'Surrogate keys for CORE.DIM_ACCOUNT';

CREATE TABLE IF NOT EXISTS CORE.DIM_ACCOUNT (
  account_sk      NUMBER        DEFAULT CORE.SEQ_ACCOUNT_SK.NEXTVAL NOT NULL,
  account_id      VARCHAR       NOT NULL,
  account_name    VARCHAR,
  account_type    VARCHAR,
  address_line    VARCHAR,
  city            VARCHAR,
  state           VARCHAR,
  postal_code     VARCHAR,
  risk_tier       VARCHAR,
  status          VARCHAR,
  open_date       DATE,
  hash_diff       VARCHAR(32)   NOT NULL,
  effective_from  DATE          NOT NULL,
  effective_to    DATE          NOT NULL,
  is_current      BOOLEAN       NOT NULL,
  source_file     VARCHAR,
  load_run_id     VARCHAR,
  inserted_at     TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  updated_at      TIMESTAMP_LTZ,
  CONSTRAINT pk_dim_account PRIMARY KEY (account_sk),
  CONSTRAINT uk_dim_account_version UNIQUE (account_id, effective_from)
) COMMENT = 'Account dimension, SCD Type 2 by file_date';

CREATE OR REPLACE PROCEDURE CORE.SP_LOAD_DIM_ACCOUNT()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  run_id      VARCHAR DEFAULT UUID_STRING();
  started     TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP();
  n_read      NUMBER DEFAULT 0;
  n_rejected  NUMBER DEFAULT 0;
  n_valid     NUMBER DEFAULT 0;
  n_changes   NUMBER DEFAULT 0;
  n_closed    NUMBER DEFAULT 0;
  n_inserted  NUMBER DEFAULT 0;
  result      VARIANT;
BEGIN
  -- DDL commits implicitly in Snowflake, so the work tables are created before the transaction.
  CREATE OR REPLACE TEMPORARY TABLE CORE.TMP_ACCOUNT_BATCH (
    account_id VARCHAR, account_name VARCHAR, account_type VARCHAR, address_line VARCHAR, city VARCHAR,
    state VARCHAR, postal_code VARCHAR, risk_tier VARCHAR, status VARCHAR, open_date DATE, file_date DATE,
    hash_diff VARCHAR(32), source_file VARCHAR, source_row_number NUMBER, record VARIANT,
    reject_reason VARCHAR, is_replay BOOLEAN
  );
  CREATE OR REPLACE TEMPORARY TABLE CORE.TMP_ACCOUNT_VERSIONS LIKE CORE.DIM_ACCOUNT;

  BEGIN TRANSACTION;

  -- 1. Read the stream once (this is the statement that consumes it at COMMIT), typing and validating.
  INSERT INTO CORE.TMP_ACCOUNT_BATCH
  SELECT
    NULLIF(TRIM(account_id), ''), account_name, account_type, address_line, city, state, postal_code,
    UPPER(risk_tier), UPPER(status), TRY_TO_DATE(open_date), TRY_TO_DATE(file_date),
    MD5(CONCAT_WS('|', COALESCE(account_name, ''), COALESCE(account_type, ''), COALESCE(address_line, ''),
                       COALESCE(city, ''), COALESCE(state, ''), COALESCE(postal_code, ''),
                       COALESCE(UPPER(risk_tier), ''), COALESCE(UPPER(status), ''), COALESCE(open_date, ''))),
    _file_name, _file_row_number,
    OBJECT_CONSTRUCT_KEEP_NULL(
      'account_id', account_id, 'account_name', account_name, 'account_type', account_type,
      'address_line', address_line, 'city', city, 'state', state, 'postal_code', postal_code,
      'risk_tier', risk_tier, 'status', status, 'open_date', open_date, 'file_date', file_date),
    CASE
      WHEN NULLIF(TRIM(account_id), '') IS NULL                 THEN 'MISSING_ACCOUNT_ID'
      WHEN TRY_TO_DATE(file_date) IS NULL                       THEN 'INVALID_FILE_DATE'
      WHEN UPPER(status) NOT IN ('ACTIVE', 'SUSPENDED', 'CLOSED') OR status IS NULL THEN 'INVALID_STATUS'
      WHEN UPPER(risk_tier) NOT IN ('LOW', 'MEDIUM', 'HIGH') OR risk_tier IS NULL   THEN 'INVALID_RISK_TIER'
    END,
    FALSE
  FROM RAW.STRM_ACCOUNTS;
  n_read := SQLROWCOUNT;

  -- 2. Rows dated on/before the account's current version: identical = replay (ignored), different = out of order.
  UPDATE CORE.TMP_ACCOUNT_BATCH b
     SET is_replay     = (b.hash_diff = d.hash_diff),
         reject_reason = IFF(b.hash_diff = d.hash_diff, NULL, 'OUT_OF_ORDER')
    FROM CORE.DIM_ACCOUNT d
   WHERE d.account_id = b.account_id
     AND d.is_current
     AND b.reject_reason IS NULL
     AND b.file_date <= d.effective_from;

  INSERT INTO AUDIT.REJECTED_ROWS (load_run_id, source_table, source_file, source_row_number, business_key, reject_reason, record)
  SELECT :run_id, 'RAW.ACCOUNTS', source_file, source_row_number, account_id, reject_reason, record
    FROM CORE.TMP_ACCOUNT_BATCH
   WHERE reject_reason IS NOT NULL;
  n_rejected := SQLROWCOUNT;

  -- 3. Build the new versions: one row per (account, file_date), keep only real changes, chain them.
  INSERT INTO CORE.TMP_ACCOUNT_VERSIONS (
    account_sk, account_id, account_name, account_type, address_line, city, state, postal_code,
    risk_tier, status, open_date, hash_diff, effective_from, effective_to, is_current,
    source_file, load_run_id
  )
  WITH valid AS (
    SELECT *
      FROM CORE.TMP_ACCOUNT_BATCH
     WHERE reject_reason IS NULL AND NOT is_replay
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id, file_date
                               ORDER BY source_file DESC, source_row_number DESC) = 1
  ),
  compared AS (
    SELECT v.*,
           COALESCE(LAG(v.hash_diff) OVER (PARTITION BY v.account_id ORDER BY v.file_date), d.hash_diff) AS prev_hash
      FROM valid v
      LEFT JOIN CORE.DIM_ACCOUNT d
        ON d.account_id = v.account_id AND d.is_current
  ),
  changes AS (
    SELECT * FROM compared WHERE prev_hash IS NULL OR prev_hash <> hash_diff
  ),
  chained AS (
    SELECT c.*,
           LEAD(file_date) OVER (PARTITION BY account_id ORDER BY file_date) AS next_from
      FROM changes c
  )
  SELECT CORE.SEQ_ACCOUNT_SK.NEXTVAL, account_id, account_name, account_type, address_line, city, state,
         postal_code, risk_tier, status, open_date, hash_diff,
         file_date,
         COALESCE(next_from - 1, '9999-12-31'::DATE),
         next_from IS NULL,
         source_file, :run_id
    FROM chained;
  n_changes := SQLROWCOUNT;

  SELECT COUNT(*) INTO :n_valid
    FROM (SELECT DISTINCT account_id, file_date FROM CORE.TMP_ACCOUNT_BATCH WHERE reject_reason IS NULL);

  -- 4. Close the current version of every account that changed (the SCD2 MERGE).
  MERGE INTO CORE.DIM_ACCOUNT d
  USING (SELECT account_id, MIN(effective_from) AS first_new_from
           FROM CORE.TMP_ACCOUNT_VERSIONS
          GROUP BY account_id) v
     ON d.account_id = v.account_id
    AND d.is_current
  WHEN MATCHED THEN UPDATE SET
       d.effective_to = v.first_new_from - 1,
       d.is_current   = FALSE,
       d.updated_at   = CURRENT_TIMESTAMP(),
       d.load_run_id  = :run_id;
  n_closed := SQLROWCOUNT;

  -- 5. Insert the new versions.
  INSERT INTO CORE.DIM_ACCOUNT (
    account_sk, account_id, account_name, account_type, address_line, city, state, postal_code,
    risk_tier, status, open_date, hash_diff, effective_from, effective_to, is_current, source_file, load_run_id
  )
  SELECT account_sk, account_id, account_name, account_type, address_line, city, state, postal_code,
         risk_tier, status, open_date, hash_diff, effective_from, effective_to, is_current, source_file, load_run_id
    FROM CORE.TMP_ACCOUNT_VERSIONS;
  n_inserted := SQLROWCOUNT;

  INSERT INTO AUDIT.LOAD_LOG (load_run_id, procedure_name, started_at, finished_at, status, rows_read,
                              rows_rejected, rows_unchanged, versions_closed, versions_inserted)
  VALUES (:run_id, 'CORE.SP_LOAD_DIM_ACCOUNT', :started, CURRENT_TIMESTAMP(), 'SUCCESS',
          :n_read, :n_rejected, :n_valid - :n_changes, :n_closed, :n_inserted);

  COMMIT;

  result := OBJECT_CONSTRUCT('load_run_id', run_id, 'rows_read', n_read, 'rows_rejected', n_rejected,
                             'rows_unchanged', n_valid - n_changes, 'versions_closed', n_closed,
                             'versions_inserted', n_inserted);
  RETURN result;
EXCEPTION
  WHEN OTHER THEN
    ROLLBACK;
    INSERT INTO AUDIT.LOAD_LOG (load_run_id, procedure_name, started_at, finished_at, status, message)
    VALUES (:run_id, 'CORE.SP_LOAD_DIM_ACCOUNT', :started, CURRENT_TIMESTAMP(), 'FAILED', :SQLERRM);
    RAISE;
END;
$$;
