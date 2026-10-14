-- Step 10: output table for the Snowpark price-outlier check (snowpark/dq_checks.py).
-- The check itself is Python (Snowpark DataFrame API); its summary row goes to AUDIT.DQ_RESULTS.

USE ROLE DW_ROLE;
USE DATABASE BROKER_DW;

CREATE TABLE IF NOT EXISTS AUDIT.PRICE_OUTLIERS (
  run_id          VARCHAR,
  checked_at      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  threshold_pct   NUMBER(10,4),
  trade_id        VARCHAR,
  trade_date      DATE,
  security_sk     NUMBER,
  security_id     VARCHAR,
  symbol          VARCHAR,
  side            VARCHAR,
  quantity        NUMBER(18,0),
  price           NUMBER(18,2),
  reference_vwap  NUMBER(38,6),
  deviation_pct   NUMBER(38,4),
  peer_trades     NUMBER
) COMMENT = 'Trades whose price deviates from the same-day peer VWAP (Snowpark DQ check)';
