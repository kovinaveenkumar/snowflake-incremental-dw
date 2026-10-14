-- Step 2: RAW tables (created before 03_pipes.sql, which loads into them). Every business column is VARCHAR so a bad value never fails the load;
-- typing and validation happen downstream, and failures go to AUDIT.REJECTED_ROWS.

USE ROLE DW_ROLE;
USE DATABASE BROKER_DW;

CREATE TABLE IF NOT EXISTS RAW.ACCOUNTS (
  account_id       VARCHAR,
  account_name     VARCHAR,
  account_type     VARCHAR,
  address_line     VARCHAR,
  city             VARCHAR,
  state            VARCHAR,
  postal_code      VARCHAR,
  risk_tier        VARCHAR,
  status           VARCHAR,
  open_date        VARCHAR,
  file_date        VARCHAR,
  _file_name       VARCHAR,
  _file_row_number NUMBER,
  _loaded_at       TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW.TRADES (
  trade_id         VARCHAR,
  trade_date       VARCHAR,
  trade_ts         VARCHAR,
  settlement_date  VARCHAR,
  account_id       VARCHAR,
  security_id      VARCHAR,
  side             VARCHAR,
  quantity         VARCHAR,
  price            VARCHAR,
  notional         VARCHAR,
  file_date        VARCHAR,
  _file_name       VARCHAR,
  _file_row_number NUMBER,
  _loaded_at       TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW.CONTROL_TOTALS (
  file_name        VARCHAR,
  file_date        VARCHAR,
  row_count        VARCHAR,
  total_quantity   VARCHAR,
  total_notional   VARCHAR,
  _file_name       VARCHAR,
  _file_row_number NUMBER,
  _loaded_at       TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW.SECURITIES (
  security_id      VARCHAR,
  symbol           VARCHAR,
  security_name    VARCHAR,
  asset_class      VARCHAR,
  currency         VARCHAR,
  _file_name       VARCHAR,
  _file_row_number NUMBER,
  _loaded_at       TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);
