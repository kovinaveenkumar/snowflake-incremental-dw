-- Step 6: CORE.DIM_SECURITY (Type 1) and CORE.FACT_TRADE (transaction grain: one row per valid trade).
--
-- Load order (the Task tree in Step 8 follows it): SP_LOAD_DIM_SECURITY -> SP_LOAD_DIM_ACCOUNT -> SP_LOAD_FACT_TRADE.
--
-- Trade validation. Each rejected row gets exactly one reason, the first that applies:
--   row checks    MISSING_TRADE_ID, MISSING_ACCOUNT_ID, MISSING_SECURITY_ID, INVALID_DATE, INVALID_QUANTITY,
--                 NEGATIVE_QUANTITY, ZERO_QUANTITY, INVALID_PRICE, INVALID_SIDE, NOTIONAL_MISMATCH (> 0.01),
--                 SETTLEMENT_BEFORE_TRADE
--   lookups       UNKNOWN_SECURITY, UNKNOWN_ACCOUNT (no account version in force on trade_date),
--                 ACCOUNT_NOT_ACTIVE (version in force on trade_date is SUSPENDED/CLOSED)
--   duplicates    DUPLICATE_TRADE_ID (already in FACT_TRADE, or a repeat inside the batch; the first
--                 occurrence by file and row number is kept)
-- Rejected rows go to AUDIT.REJECTED_ROWS with the original record, so rows read = rows inserted + rows rejected.
--
-- Account key: the DIM_ACCOUNT version whose [effective_from, effective_to] contains trade_date, i.e. the
-- account as it was when the trade happened, not as it is today.

USE ROLE DW_ROLE;
USE DATABASE BROKER_DW;

ALTER TABLE AUDIT.LOAD_LOG ADD COLUMN IF NOT EXISTS rows_inserted NUMBER;
ALTER TABLE AUDIT.LOAD_LOG ADD COLUMN IF NOT EXISTS rows_updated NUMBER;

CREATE SEQUENCE IF NOT EXISTS CORE.SEQ_SECURITY_SK COMMENT = 'Surrogate keys for CORE.DIM_SECURITY';

CREATE TABLE IF NOT EXISTS CORE.DIM_SECURITY (
  security_sk    NUMBER        DEFAULT CORE.SEQ_SECURITY_SK.NEXTVAL NOT NULL,
  security_id    VARCHAR       NOT NULL,
  symbol         VARCHAR,
  security_name  VARCHAR,
  asset_class    VARCHAR,
  currency       VARCHAR(3),
  hash_diff      VARCHAR(32)   NOT NULL,
  source_file    VARCHAR,
  load_run_id    VARCHAR,
  inserted_at    TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  updated_at     TIMESTAMP_LTZ,
  CONSTRAINT pk_dim_security PRIMARY KEY (security_sk),
  CONSTRAINT uk_dim_security UNIQUE (security_id)
) COMMENT = 'Security master, Type 1 (latest values overwrite)';

CREATE TABLE IF NOT EXISTS CORE.FACT_TRADE (
  trade_id           VARCHAR        NOT NULL,
  account_sk         NUMBER         NOT NULL,
  security_sk        NUMBER         NOT NULL,
  trade_date         DATE           NOT NULL,
  trade_ts           TIMESTAMP_NTZ  NOT NULL,
  settlement_date    DATE           NOT NULL,
  side               VARCHAR(4)     NOT NULL,
  quantity           NUMBER(18,0)   NOT NULL,
  signed_quantity    NUMBER(18,0)   NOT NULL,  -- BUY +, SELL -
  price              NUMBER(18,2)   NOT NULL,
  notional           NUMBER(20,2)   NOT NULL,
  signed_notional    NUMBER(20,2)   NOT NULL,  -- BUY +, SELL -
  file_date          DATE,
  source_file        VARCHAR,
  source_row_number  NUMBER,
  load_run_id        VARCHAR,
  loaded_at          TIMESTAMP_LTZ  DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT pk_fact_trade PRIMARY KEY (trade_id),
  CONSTRAINT fk_fact_trade_account FOREIGN KEY (account_sk) REFERENCES CORE.DIM_ACCOUNT (account_sk),
  CONSTRAINT fk_fact_trade_security FOREIGN KEY (security_sk) REFERENCES CORE.DIM_SECURITY (security_sk)
) COMMENT = 'Trade fact, grain: one row per valid trade';

CREATE OR REPLACE PROCEDURE CORE.SP_LOAD_DIM_SECURITY()
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
  n_inserted  NUMBER DEFAULT 0;
  n_updated   NUMBER DEFAULT 0;
BEGIN
  CREATE OR REPLACE TEMPORARY TABLE CORE.TMP_SECURITY_BATCH (
    security_id VARCHAR, symbol VARCHAR, security_name VARCHAR, asset_class VARCHAR, currency VARCHAR,
    hash_diff VARCHAR(32), source_file VARCHAR, source_row_number NUMBER, record VARIANT, reject_reason VARCHAR
  );

  BEGIN TRANSACTION;

  INSERT INTO CORE.TMP_SECURITY_BATCH
  SELECT
    NULLIF(TRIM(security_id), ''), symbol, security_name, UPPER(asset_class), UPPER(currency),
    MD5(CONCAT_WS('|', COALESCE(symbol, ''), COALESCE(security_name, ''),
                       COALESCE(UPPER(asset_class), ''), COALESCE(UPPER(currency), ''))),
    _file_name, _file_row_number,
    OBJECT_CONSTRUCT_KEEP_NULL('security_id', security_id, 'symbol', symbol, 'security_name', security_name,
                               'asset_class', asset_class, 'currency', currency),
    CASE
      WHEN NULLIF(TRIM(security_id), '') IS NULL THEN 'MISSING_SECURITY_ID'
      WHEN asset_class IS NULL OR UPPER(asset_class) NOT IN ('EQUITY', 'ETF', 'BOND') THEN 'INVALID_ASSET_CLASS'
      WHEN currency IS NULL OR LENGTH(TRIM(currency)) <> 3 THEN 'INVALID_CURRENCY'
    END
  FROM RAW.STRM_SECURITIES;
  n_read := SQLROWCOUNT;

  INSERT INTO AUDIT.REJECTED_ROWS (load_run_id, source_table, source_file, source_row_number, business_key, reject_reason, record)
  SELECT :run_id, 'RAW.SECURITIES', source_file, source_row_number, security_id, reject_reason, record
    FROM CORE.TMP_SECURITY_BATCH
   WHERE reject_reason IS NOT NULL;
  n_rejected := SQLROWCOUNT;

  MERGE INTO CORE.DIM_SECURITY d
  USING (
    SELECT *
      FROM CORE.TMP_SECURITY_BATCH
     WHERE reject_reason IS NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY security_id ORDER BY source_file DESC, source_row_number DESC) = 1
  ) s
     ON d.security_id = s.security_id
  WHEN MATCHED AND d.hash_diff <> s.hash_diff THEN UPDATE SET
       d.symbol = s.symbol, d.security_name = s.security_name, d.asset_class = s.asset_class,
       d.currency = s.currency, d.hash_diff = s.hash_diff, d.source_file = s.source_file,
       d.load_run_id = :run_id, d.updated_at = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT
       (security_id, symbol, security_name, asset_class, currency, hash_diff, source_file, load_run_id)
       VALUES (s.security_id, s.symbol, s.security_name, s.asset_class, s.currency, s.hash_diff, s.source_file, :run_id);

  SELECT "number of rows inserted", "number of rows updated" INTO :n_inserted, :n_updated
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

  INSERT INTO AUDIT.LOAD_LOG (load_run_id, procedure_name, started_at, finished_at, status,
                              rows_read, rows_rejected, rows_inserted, rows_updated)
  VALUES (:run_id, 'CORE.SP_LOAD_DIM_SECURITY', :started, CURRENT_TIMESTAMP(), 'SUCCESS',
          :n_read, :n_rejected, :n_inserted, :n_updated);

  COMMIT;
  RETURN OBJECT_CONSTRUCT('load_run_id', run_id, 'rows_read', n_read, 'rows_rejected', n_rejected,
                          'rows_inserted', n_inserted, 'rows_updated', n_updated);
EXCEPTION
  WHEN OTHER THEN
    ROLLBACK;
    INSERT INTO AUDIT.LOAD_LOG (load_run_id, procedure_name, started_at, finished_at, status, message)
    VALUES (:run_id, 'CORE.SP_LOAD_DIM_SECURITY', :started, CURRENT_TIMESTAMP(), 'FAILED', :SQLERRM);
    RAISE;
END;
$$;

CREATE OR REPLACE PROCEDURE CORE.SP_LOAD_FACT_TRADE()
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
  n_inserted  NUMBER DEFAULT 0;
BEGIN
  CREATE OR REPLACE TEMPORARY TABLE CORE.TMP_TRADE_BATCH (
    trade_id VARCHAR, trade_date DATE, trade_ts TIMESTAMP_NTZ, settlement_date DATE, account_id VARCHAR,
    security_id VARCHAR, side VARCHAR, quantity NUMBER(18,0), price NUMBER(18,2), notional NUMBER(20,2),
    file_date DATE, source_file VARCHAR, source_row_number NUMBER, record VARIANT, reject_reason VARCHAR,
    account_sk NUMBER, security_sk NUMBER
  );

  BEGIN TRANSACTION;

  -- 1. Read the stream once, type every column and apply the row-level checks.
  INSERT INTO CORE.TMP_TRADE_BATCH
  SELECT
    NULLIF(TRIM(trade_id), ''), TRY_TO_DATE(trade_date), TRY_TO_TIMESTAMP_NTZ(trade_ts), TRY_TO_DATE(settlement_date),
    NULLIF(TRIM(account_id), ''), NULLIF(TRIM(security_id), ''), UPPER(side),
    TRY_TO_NUMBER(quantity, 18, 0), TRY_TO_NUMBER(price, 18, 2), TRY_TO_NUMBER(notional, 20, 2), TRY_TO_DATE(file_date),
    _file_name, _file_row_number,
    OBJECT_CONSTRUCT_KEEP_NULL(
      'trade_id', trade_id, 'trade_date', trade_date, 'trade_ts', trade_ts, 'settlement_date', settlement_date,
      'account_id', account_id, 'security_id', security_id, 'side', side, 'quantity', quantity,
      'price', price, 'notional', notional, 'file_date', file_date),
    CASE
      WHEN NULLIF(TRIM(trade_id), '') IS NULL     THEN 'MISSING_TRADE_ID'
      WHEN NULLIF(TRIM(account_id), '') IS NULL   THEN 'MISSING_ACCOUNT_ID'
      WHEN NULLIF(TRIM(security_id), '') IS NULL  THEN 'MISSING_SECURITY_ID'
      WHEN TRY_TO_DATE(trade_date) IS NULL OR TRY_TO_TIMESTAMP_NTZ(trade_ts) IS NULL
        OR TRY_TO_DATE(settlement_date) IS NULL   THEN 'INVALID_DATE'
      WHEN TRY_TO_NUMBER(quantity, 18, 0) IS NULL THEN 'INVALID_QUANTITY'
      WHEN TRY_TO_NUMBER(quantity, 18, 0) < 0     THEN 'NEGATIVE_QUANTITY'
      WHEN TRY_TO_NUMBER(quantity, 18, 0) = 0     THEN 'ZERO_QUANTITY'
      WHEN TRY_TO_NUMBER(price, 18, 2) IS NULL OR TRY_TO_NUMBER(price, 18, 2) <= 0 THEN 'INVALID_PRICE'
      WHEN side IS NULL OR UPPER(side) NOT IN ('BUY', 'SELL') THEN 'INVALID_SIDE'
      WHEN TRY_TO_NUMBER(notional, 20, 2) IS NULL
        OR ABS(TRY_TO_NUMBER(notional, 20, 2) - TRY_TO_NUMBER(quantity, 18, 0) * TRY_TO_NUMBER(price, 18, 2)) > 0.01
                                                  THEN 'NOTIONAL_MISMATCH'
      WHEN TRY_TO_DATE(settlement_date) < TRY_TO_DATE(trade_date) THEN 'SETTLEMENT_BEFORE_TRADE'
    END,
    NULL, NULL
  FROM RAW.STRM_TRADES;
  n_read := SQLROWCOUNT;

  -- 2. Lookups: security, then the account version in force on the trade date.
  UPDATE CORE.TMP_TRADE_BATCH b
     SET security_sk = s.security_sk
    FROM CORE.DIM_SECURITY s
   WHERE s.security_id = b.security_id AND b.reject_reason IS NULL;
  UPDATE CORE.TMP_TRADE_BATCH SET reject_reason = 'UNKNOWN_SECURITY'
   WHERE reject_reason IS NULL AND security_sk IS NULL;

  UPDATE CORE.TMP_TRADE_BATCH b
     SET account_sk = d.account_sk,
         reject_reason = IFF(d.status = 'ACTIVE', NULL, 'ACCOUNT_NOT_ACTIVE')
    FROM CORE.DIM_ACCOUNT d
   WHERE d.account_id = b.account_id
     AND b.trade_date BETWEEN d.effective_from AND d.effective_to
     AND b.reject_reason IS NULL;
  UPDATE CORE.TMP_TRADE_BATCH SET reject_reason = 'UNKNOWN_ACCOUNT'
   WHERE reject_reason IS NULL AND account_sk IS NULL;

  -- 3. Duplicates: already loaded, or repeated inside this batch (keep the first occurrence).
  UPDATE CORE.TMP_TRADE_BATCH b
     SET reject_reason = 'DUPLICATE_TRADE_ID'
    FROM CORE.FACT_TRADE f
   WHERE f.trade_id = b.trade_id AND b.reject_reason IS NULL;

  UPDATE CORE.TMP_TRADE_BATCH b
     SET reject_reason = 'DUPLICATE_TRADE_ID'
    FROM (SELECT source_file, source_row_number
            FROM CORE.TMP_TRADE_BATCH
           WHERE reject_reason IS NULL
          QUALIFY ROW_NUMBER() OVER (PARTITION BY trade_id ORDER BY source_file, source_row_number) > 1) dup
   WHERE b.source_file = dup.source_file AND b.source_row_number = dup.source_row_number;

  INSERT INTO AUDIT.REJECTED_ROWS (load_run_id, source_table, source_file, source_row_number, business_key, reject_reason, record)
  SELECT :run_id, 'RAW.TRADES', source_file, source_row_number, trade_id, reject_reason, record
    FROM CORE.TMP_TRADE_BATCH
   WHERE reject_reason IS NOT NULL;
  n_rejected := SQLROWCOUNT;

  -- 4. Load the valid trades.
  INSERT INTO CORE.FACT_TRADE (
    trade_id, account_sk, security_sk, trade_date, trade_ts, settlement_date, side, quantity, signed_quantity,
    price, notional, signed_notional, file_date, source_file, source_row_number, load_run_id
  )
  SELECT trade_id, account_sk, security_sk, trade_date, trade_ts, settlement_date, side, quantity,
         IFF(side = 'BUY', quantity, -quantity),
         price, notional, IFF(side = 'BUY', notional, -notional),
         file_date, source_file, source_row_number, :run_id
    FROM CORE.TMP_TRADE_BATCH
   WHERE reject_reason IS NULL;
  n_inserted := SQLROWCOUNT;

  INSERT INTO AUDIT.LOAD_LOG (load_run_id, procedure_name, started_at, finished_at, status,
                              rows_read, rows_rejected, rows_inserted)
  VALUES (:run_id, 'CORE.SP_LOAD_FACT_TRADE', :started, CURRENT_TIMESTAMP(), 'SUCCESS',
          :n_read, :n_rejected, :n_inserted);

  COMMIT;
  RETURN OBJECT_CONSTRUCT('load_run_id', run_id, 'rows_read', n_read, 'rows_rejected', n_rejected,
                          'rows_inserted', n_inserted);
EXCEPTION
  WHEN OTHER THEN
    ROLLBACK;
    INSERT INTO AUDIT.LOAD_LOG (load_run_id, procedure_name, started_at, finished_at, status, message)
    VALUES (:run_id, 'CORE.SP_LOAD_FACT_TRADE', :started, CURRENT_TIMESTAMP(), 'FAILED', :SQLERRM);
    RAISE;
END;
$$;
