-- Step 7: CORE.FACT_DAILY_POSITION, a periodic snapshot fact.
-- Grain: one row per account x security x business day, end-of-day.
--
-- Rules
--   * Business days are the trade dates present in FACT_TRADE.
--   * position_quantity = running sum of signed_quantity (BUY +, SELL -) up to and including the day.
--     Positions can go negative (short); the synthetic feed does not enforce holdings before a sale.
--   * A row exists when the position is non-zero OR the pair traded that day. So a position that is
--     closed out still shows on the day it went to zero, and then disappears.
--   * close_price = price of the security's last trade (by trade_ts) on or before the day, carried
--     forward on days it did not trade. market_value = position_quantity * close_price.
--   * account_sk = the DIM_ACCOUNT version in force on position_date (positions are summed by the
--     durable account_id, so a change of address does not break the running total).
--
-- Incremental load: CORE.STRM_FACT_TRADE tells the procedure which trade dates received new trades.
-- It rebuilds the snapshot from the earliest of those dates forward, starting from the stored
-- positions of the previous business day. A late trade (dated in the past) therefore corrects every
-- later day too; the normal daily case rebuilds just the new day.

USE ROLE DW_ROLE;
USE DATABASE BROKER_DW;

CREATE TABLE IF NOT EXISTS CORE.FACT_DAILY_POSITION (
  position_date      DATE          NOT NULL,
  account_sk         NUMBER        NOT NULL,
  account_id         VARCHAR       NOT NULL,  -- durable key, used to roll positions across account versions
  security_sk        NUMBER        NOT NULL,
  position_quantity  NUMBER(18,0)  NOT NULL,  -- end of day
  buy_quantity       NUMBER(18,0)  NOT NULL,  -- that day
  sell_quantity      NUMBER(18,0)  NOT NULL,  -- that day
  net_quantity       NUMBER(18,0)  NOT NULL,  -- that day, BUY - SELL
  net_notional       NUMBER(20,2)  NOT NULL,  -- that day, signed
  trade_count        NUMBER        NOT NULL,  -- that day
  close_price        NUMBER(18,2),
  market_value       NUMBER(24,2),
  load_run_id        VARCHAR,
  loaded_at          TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT pk_fact_daily_position PRIMARY KEY (position_date, account_id, security_sk),
  CONSTRAINT fk_position_account FOREIGN KEY (account_sk) REFERENCES CORE.DIM_ACCOUNT (account_sk),
  CONSTRAINT fk_position_security FOREIGN KEY (security_sk) REFERENCES CORE.DIM_SECURITY (security_sk)
) COMMENT = 'Daily position snapshot, grain: account x security x business day';

CREATE STREAM IF NOT EXISTS CORE.STRM_FACT_TRADE
  ON TABLE CORE.FACT_TRADE
  APPEND_ONLY = TRUE
  SHOW_INITIAL_ROWS = TRUE
  COMMENT = 'New trades -> which snapshot days to rebuild (SP_LOAD_FACT_DAILY_POSITION)';

CREATE OR REPLACE PROCEDURE CORE.SP_LOAD_FACT_DAILY_POSITION()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  run_id      VARCHAR DEFAULT UUID_STRING();
  started     TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP();
  n_read      NUMBER DEFAULT 0;
  n_deleted   NUMBER DEFAULT 0;
  n_inserted  NUMBER DEFAULT 0;
  from_date   DATE;
BEGIN
  CREATE OR REPLACE TEMPORARY TABLE CORE.TMP_POSITION_TRIGGER (trade_date DATE);

  BEGIN TRANSACTION;

  INSERT INTO CORE.TMP_POSITION_TRIGGER SELECT trade_date FROM CORE.STRM_FACT_TRADE;
  n_read := SQLROWCOUNT;
  SELECT MIN(trade_date) INTO :from_date FROM CORE.TMP_POSITION_TRIGGER;

  IF (from_date IS NOT NULL) THEN
    DELETE FROM CORE.FACT_DAILY_POSITION WHERE position_date >= :from_date;
    n_deleted := SQLROWCOUNT;

    INSERT INTO CORE.FACT_DAILY_POSITION (
      position_date, account_sk, account_id, security_sk, position_quantity, buy_quantity, sell_quantity,
      net_quantity, net_notional, trade_count, close_price, market_value, load_run_id
    )
    WITH calendar AS (
      SELECT DISTINCT trade_date AS day FROM CORE.FACT_TRADE
    ),
    build_days AS (
      SELECT day FROM calendar WHERE day >= :from_date
    ),
    base AS (   -- end-of-day positions on the last business day before the rebuild
      SELECT account_id, security_sk, position_quantity AS base_quantity
        FROM CORE.FACT_DAILY_POSITION
       WHERE position_date = (SELECT MAX(position_date) FROM CORE.FACT_DAILY_POSITION WHERE position_date < :from_date)
         AND position_quantity <> 0
    ),
    activity AS (
      SELECT a.account_id, f.security_sk, f.trade_date AS day,
             SUM(IFF(f.side = 'BUY', f.quantity, 0))  AS buy_quantity,
             SUM(IFF(f.side = 'SELL', f.quantity, 0)) AS sell_quantity,
             SUM(f.signed_quantity)                   AS net_quantity,
             SUM(f.signed_notional)                   AS net_notional,
             COUNT(*)                                 AS trade_count
        FROM CORE.FACT_TRADE f
        JOIN CORE.DIM_ACCOUNT a ON a.account_sk = f.account_sk
       WHERE f.trade_date >= :from_date
       GROUP BY a.account_id, f.security_sk, f.trade_date
    ),
    pairs AS (  -- every holding carried in, plus every pair that trades, from its first day
      SELECT account_id, security_sk, MIN(first_day) AS first_day
        FROM (SELECT account_id, security_sk, :from_date AS first_day FROM base
              UNION ALL
              SELECT account_id, security_sk, MIN(day) FROM activity GROUP BY account_id, security_sk)
       GROUP BY account_id, security_sk
    ),
    rolled AS (
      SELECT p.account_id, p.security_sk, d.day,
             COALESCE(b.base_quantity, 0)
               + SUM(COALESCE(a.net_quantity, 0)) OVER (PARTITION BY p.account_id, p.security_sk ORDER BY d.day
                                                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                                                    AS position_quantity,
             COALESCE(a.buy_quantity, 0)  AS buy_quantity,
             COALESCE(a.sell_quantity, 0) AS sell_quantity,
             COALESCE(a.net_quantity, 0)  AS net_quantity,
             COALESCE(a.net_notional, 0)  AS net_notional,
             COALESCE(a.trade_count, 0)   AS trade_count
        FROM pairs p
        JOIN build_days d ON d.day >= p.first_day
        LEFT JOIN base b     ON b.account_id = p.account_id AND b.security_sk = p.security_sk
        LEFT JOIN activity a ON a.account_id = p.account_id AND a.security_sk = p.security_sk AND a.day = d.day
    ),
    last_trade_price AS (
      SELECT security_sk, trade_date AS day, price
        FROM CORE.FACT_TRADE
      QUALIFY ROW_NUMBER() OVER (PARTITION BY security_sk, trade_date ORDER BY trade_ts DESC, trade_id DESC) = 1
    ),
    close_price AS (
      SELECT s.security_sk, c.day,
             LAST_VALUE(l.price IGNORE NULLS) OVER (PARTITION BY s.security_sk ORDER BY c.day
                                                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS close_price
        FROM (SELECT DISTINCT security_sk FROM CORE.FACT_TRADE) s
       CROSS JOIN calendar c
        LEFT JOIN last_trade_price l ON l.security_sk = s.security_sk AND l.day = c.day
    )
    SELECT r.day, acct.account_sk, r.account_id, r.security_sk, r.position_quantity, r.buy_quantity,
           r.sell_quantity, r.net_quantity, r.net_notional, r.trade_count, cp.close_price,
           r.position_quantity * cp.close_price, :run_id
      FROM rolled r
      JOIN CORE.DIM_ACCOUNT acct
        ON acct.account_id = r.account_id AND r.day BETWEEN acct.effective_from AND acct.effective_to
      LEFT JOIN close_price cp ON cp.security_sk = r.security_sk AND cp.day = r.day
     WHERE r.position_quantity <> 0 OR r.trade_count > 0;
    n_inserted := SQLROWCOUNT;
  END IF;

  INSERT INTO AUDIT.LOAD_LOG (load_run_id, procedure_name, started_at, finished_at, status,
                              rows_read, rows_inserted, message)
  VALUES (:run_id, 'CORE.SP_LOAD_FACT_DAILY_POSITION', :started, CURRENT_TIMESTAMP(), 'SUCCESS',
          :n_read, :n_inserted,
          IFF(:from_date IS NULL, 'no new trades',
              'rebuilt from ' || TO_VARCHAR(:from_date) || ', replaced ' || :n_deleted || ' rows'));

  COMMIT;
  RETURN OBJECT_CONSTRUCT('load_run_id', run_id, 'trades_read', n_read, 'rebuilt_from', from_date,
                          'rows_deleted', n_deleted, 'rows_inserted', n_inserted);
EXCEPTION
  WHEN OTHER THEN
    ROLLBACK;
    INSERT INTO AUDIT.LOAD_LOG (load_run_id, procedure_name, started_at, finished_at, status, message)
    VALUES (:run_id, 'CORE.SP_LOAD_FACT_DAILY_POSITION', :started, CURRENT_TIMESTAMP(), 'FAILED', :SQLERRM);
    RAISE;
END;
$$;
