-- Step 12: PL/SQL package with the same SCD Type 2 load, trade validation and reconciliation as the
-- Snowflake procedures, processing one file_date at a time.
--
--   run_day(p_file_date)   load_dim_security -> load_dim_account -> load_fact_trade -> reconcile, one transaction.
--   Errors: any failure rolls the whole day back and re-raises; a reconciliation mismatch raises
--   ORA-20001 (e_recon_failed). log_event uses an AUTONOMOUS TRANSACTION, so the log row survives the rollback.

CREATE OR REPLACE PACKAGE pkg_load AS
  e_recon_failed EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_recon_failed, -20001);

  PROCEDURE log_event(p_file_date DATE, p_step VARCHAR2, p_status VARCHAR2, p_rows NUMBER, p_message VARCHAR2 DEFAULT NULL);
  PROCEDURE load_dim_security;
  PROCEDURE load_dim_account(p_file_date DATE);
  PROCEDURE load_fact_trade(p_file_date DATE);
  PROCEDURE reconcile(p_file_date DATE);
  PROCEDURE run_day(p_file_date DATE);
END pkg_load;
/

CREATE OR REPLACE PACKAGE BODY pkg_load AS

  c_open_end CONSTANT DATE := DATE '9999-12-31';

  FUNCTION file_tag(p_file_date DATE) RETURN VARCHAR2 IS
  BEGIN
    RETURN TO_CHAR(p_file_date, 'YYYYMMDD');
  END;

  PROCEDURE log_event(p_file_date DATE, p_step VARCHAR2, p_status VARCHAR2, p_rows NUMBER, p_message VARCHAR2 DEFAULT NULL) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    INSERT INTO load_log (file_date, step, status, row_count, message)
    VALUES (p_file_date, p_step, p_status, p_rows, SUBSTR(p_message, 1, 4000));
    COMMIT;
  END;

  -- Type 1: latest values overwrite.
  PROCEDURE load_dim_security IS
  BEGIN
    MERGE INTO dim_security d
    USING (SELECT security_id, symbol, security_name, UPPER(asset_class) AS asset_class, UPPER(currency) AS currency
             FROM stg_securities WHERE security_id IS NOT NULL) s
       ON (d.security_id = s.security_id)
    WHEN MATCHED THEN UPDATE SET d.symbol = s.symbol, d.security_name = s.security_name,
                                 d.asset_class = s.asset_class, d.currency = s.currency
    WHEN NOT MATCHED THEN INSERT (security_id, symbol, security_name, asset_class, currency)
                          VALUES (s.security_id, s.symbol, s.security_name, s.asset_class, s.currency);
  END;

  -- SCD Type 2 for one file: reject invalid rows, close changed current versions, insert new versions.
  PROCEDURE load_dim_account(p_file_date DATE) IS
    v_file   VARCHAR2(100) := 'accounts_' || file_tag(p_file_date) || '.csv';
    v_closed NUMBER;
    v_new    NUMBER;
  BEGIN
    INSERT INTO rejected_rows (source_table, file_name, file_row, business_key, reject_reason)
    SELECT 'STG_ACCOUNTS', file_name, file_row, account_id,
           CASE WHEN account_id IS NULL THEN 'MISSING_ACCOUNT_ID'
                WHEN UPPER(status) NOT IN ('ACTIVE', 'SUSPENDED', 'CLOSED') OR status IS NULL THEN 'INVALID_STATUS'
                ELSE 'INVALID_RISK_TIER' END
      FROM stg_accounts
     WHERE file_name = v_file
       AND (account_id IS NULL OR status IS NULL OR UPPER(status) NOT IN ('ACTIVE', 'SUSPENDED', 'CLOSED')
            OR risk_tier IS NULL OR UPPER(risk_tier) NOT IN ('LOW', 'MEDIUM', 'HIGH'));

    -- Close the current version of accounts whose tracked attributes changed. (MERGE cannot update a column
    -- used in its ON clause, so the match is on the current version's surrogate key.)
    MERGE INTO dim_account d
    USING (
      SELECT cur.account_sk
        FROM stg_accounts s
        JOIN dim_account cur ON cur.account_id = s.account_id AND cur.is_current
       WHERE s.file_name = v_file
         AND s.account_id IS NOT NULL
         AND UPPER(s.status) IN ('ACTIVE', 'SUSPENDED', 'CLOSED')
         AND UPPER(s.risk_tier) IN ('LOW', 'MEDIUM', 'HIGH')
         AND cur.hash_diff <> RAWTOHEX(STANDARD_HASH(s.account_name || '|' || s.account_type || '|' || s.address_line
               || '|' || s.city || '|' || s.state || '|' || s.postal_code || '|' || UPPER(s.risk_tier) || '|'
               || UPPER(s.status) || '|' || s.open_date, 'MD5'))
    ) chg
       ON (d.account_sk = chg.account_sk)
    WHEN MATCHED THEN UPDATE SET d.effective_to = p_file_date - 1, d.is_current = FALSE;
    v_closed := SQL%ROWCOUNT;

    -- Insert a version for every valid account without a current row: new accounts and the ones just closed.
    INSERT INTO dim_account (account_id, account_name, account_type, address_line, city, state, postal_code,
                             risk_tier, status, open_date, hash_diff, effective_from, effective_to, is_current)
    SELECT s.account_id, s.account_name, s.account_type, s.address_line, s.city, s.state, s.postal_code,
           UPPER(s.risk_tier), UPPER(s.status), TO_DATE(s.open_date, 'YYYY-MM-DD'),
           RAWTOHEX(STANDARD_HASH(s.account_name || '|' || s.account_type || '|' || s.address_line || '|' || s.city
             || '|' || s.state || '|' || s.postal_code || '|' || UPPER(s.risk_tier) || '|' || UPPER(s.status)
             || '|' || s.open_date, 'MD5')),
           p_file_date, c_open_end, TRUE
      FROM stg_accounts s
     WHERE s.file_name = v_file
       AND s.account_id IS NOT NULL
       AND UPPER(s.status) IN ('ACTIVE', 'SUSPENDED', 'CLOSED')
       AND UPPER(s.risk_tier) IN ('LOW', 'MEDIUM', 'HIGH')
       AND NOT EXISTS (SELECT 1 FROM dim_account d WHERE d.account_id = s.account_id AND d.is_current);
    v_new := SQL%ROWCOUNT;

    log_event(p_file_date, 'LOAD_DIM_ACCOUNT', 'OK', v_new, 'versions closed ' || v_closed || ', inserted ' || v_new);
  END;

  -- Trades for one file: one reject reason per row (first that applies), then load the valid rows.
  PROCEDURE load_fact_trade(p_file_date DATE) IS
    v_file     VARCHAR2(100) := 'trades_' || file_tag(p_file_date) || '.csv';
    v_rejected NUMBER;
    v_loaded   NUMBER;
  BEGIN
    INSERT INTO rejected_rows (source_table, file_name, file_row, business_key, reject_reason, quantity, notional)
    WITH t AS (
      SELECT s.*,
             TO_NUMBER(s.quantity DEFAULT NULL ON CONVERSION ERROR) AS qty,
             TO_NUMBER(s.price DEFAULT NULL ON CONVERSION ERROR) AS px,
             TO_NUMBER(s.notional DEFAULT NULL ON CONVERSION ERROR) AS ntl,
             TO_DATE(s.trade_date DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD') AS tdate,
             (SELECT MAX(sec.security_sk) FROM dim_security sec WHERE sec.security_id = s.security_id) AS security_sk,
             (SELECT MAX(a.status) FROM dim_account a
               WHERE a.account_id = s.account_id
                 AND TO_DATE(s.trade_date DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD') BETWEEN a.effective_from AND a.effective_to) AS acct_status,
             ROW_NUMBER() OVER (PARTITION BY s.trade_id ORDER BY s.file_row) AS occurrence,
             (SELECT COUNT(*) FROM fact_trade f WHERE f.trade_id = s.trade_id) AS already_loaded
        FROM stg_trades s
       WHERE s.file_name = v_file
    ),
    reasons AS (
      SELECT t.*,
             CASE
               WHEN trade_id IS NULL                          THEN 'MISSING_TRADE_ID'
               WHEN account_id IS NULL                        THEN 'MISSING_ACCOUNT_ID'
               WHEN security_id IS NULL                       THEN 'MISSING_SECURITY_ID'
               WHEN tdate IS NULL                             THEN 'INVALID_DATE'
               WHEN qty IS NULL                               THEN 'INVALID_QUANTITY'
               WHEN qty < 0                                   THEN 'NEGATIVE_QUANTITY'
               WHEN qty = 0                                   THEN 'ZERO_QUANTITY'
               WHEN px IS NULL OR px <= 0                     THEN 'INVALID_PRICE'
               WHEN UPPER(side) NOT IN ('BUY', 'SELL') OR side IS NULL THEN 'INVALID_SIDE'
               WHEN ntl IS NULL OR ABS(ntl - qty * px) > 0.01 THEN 'NOTIONAL_MISMATCH'
               WHEN security_sk IS NULL                       THEN 'UNKNOWN_SECURITY'
               WHEN acct_status IS NULL                       THEN 'UNKNOWN_ACCOUNT'
               WHEN acct_status <> 'ACTIVE'                   THEN 'ACCOUNT_NOT_ACTIVE'
               WHEN already_loaded > 0 OR occurrence > 1      THEN 'DUPLICATE_TRADE_ID'
             END AS reject_reason
        FROM t
    )
    SELECT 'STG_TRADES', file_name, file_row, trade_id, reject_reason, quantity, notional
      FROM reasons
     WHERE reject_reason IS NOT NULL;
    v_rejected := SQL%ROWCOUNT;

    INSERT INTO fact_trade (trade_id, account_sk, security_sk, trade_date, side, quantity, price, notional, file_name, file_row)
    SELECT s.trade_id, a.account_sk, sec.security_sk, TO_DATE(s.trade_date, 'YYYY-MM-DD'), UPPER(s.side),
           TO_NUMBER(s.quantity), TO_NUMBER(s.price), TO_NUMBER(s.notional), s.file_name, s.file_row
      FROM stg_trades s
      JOIN dim_security sec ON sec.security_id = s.security_id
      JOIN dim_account a    ON a.account_id = s.account_id
                           AND TO_DATE(s.trade_date, 'YYYY-MM-DD') BETWEEN a.effective_from AND a.effective_to
     WHERE s.file_name = v_file
       AND NOT EXISTS (SELECT 1 FROM rejected_rows r WHERE r.file_name = s.file_name AND r.file_row = s.file_row);
    v_loaded := SQL%ROWCOUNT;

    log_event(p_file_date, 'LOAD_FACT_TRADE', 'OK', v_loaded, 'loaded ' || v_loaded || ', rejected ' || v_rejected);
  END;

  -- Sender control totals vs staged file vs loaded + rejected. Any mismatch raises ORA-20001.
  PROCEDURE reconcile(p_file_date DATE) IS
    v_file  VARCHAR2(100) := 'trades_' || file_tag(p_file_date) || '.csv';
    v_fails NUMBER;
  BEGIN
    INSERT INTO recon_results (file_name, check_name, expected, actual, status)
    WITH c AS (
      SELECT TO_NUMBER(row_count) AS ctl_rows, TO_NUMBER(total_quantity) AS ctl_qty, TO_NUMBER(total_notional) AS ctl_ntl
        FROM stg_control WHERE file_name = v_file
    ),
    s AS (SELECT COUNT(*) AS n, SUM(TO_NUMBER(quantity)) AS q, SUM(TO_NUMBER(notional)) AS ntl
            FROM stg_trades WHERE file_name = v_file),
    f AS (SELECT COUNT(*) AS n, NVL(SUM(quantity), 0) AS q, NVL(SUM(notional), 0) AS ntl
            FROM fact_trade WHERE file_name = v_file),
    r AS (SELECT COUNT(*) AS n, NVL(SUM(TO_NUMBER(quantity)), 0) AS q, NVL(SUM(TO_NUMBER(notional)), 0) AS ntl
            FROM rejected_rows WHERE file_name = v_file),
    checks AS (
      SELECT 'FILE_VS_STAGE_ROWS' AS check_name, c.ctl_rows AS expected, s.n AS actual FROM c, s
      UNION ALL SELECT 'FILE_VS_STAGE_NOTIONAL', c.ctl_ntl, s.ntl FROM c, s
      UNION ALL SELECT 'STAGE_VS_CORE_ROWS', s.n, f.n + r.n FROM s, f, r
      UNION ALL SELECT 'CONTROL_VS_CORE_QUANTITY', c.ctl_qty, f.q + r.q FROM c, f, r
      UNION ALL SELECT 'CONTROL_VS_CORE_NOTIONAL', c.ctl_ntl, f.ntl + r.ntl FROM c, f, r
    )
    SELECT v_file, check_name, expected, actual, CASE WHEN expected = actual THEN 'PASS' ELSE 'FAIL' END
      FROM checks;

    SELECT COUNT(*) INTO v_fails FROM recon_results WHERE file_name = v_file AND status = 'FAIL';
    IF v_fails > 0 THEN
      RAISE_APPLICATION_ERROR(-20001, 'Reconciliation failed for ' || v_file || ': ' || v_fails || ' check(s)');
    END IF;
    log_event(p_file_date, 'RECONCILE', 'OK', 5, v_file || ' 5 checks PASS');
  END;

  PROCEDURE run_day(p_file_date DATE) IS
  BEGIN
    load_dim_security;
    load_dim_account(p_file_date);
    load_fact_trade(p_file_date);
    reconcile(p_file_date);
    COMMIT;
  EXCEPTION
    WHEN e_recon_failed THEN
      ROLLBACK;
      log_event(p_file_date, 'RUN_DAY', 'FAILED', NULL, SQLERRM);
      RAISE;
    WHEN OTHERS THEN
      ROLLBACK;
      log_event(p_file_date, 'RUN_DAY', 'ERROR', NULL, SQLERRM || ' ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
      RAISE;
  END;

END pkg_load;
/
