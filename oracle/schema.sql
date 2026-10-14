-- Step 12: Oracle 23ai schema for the PL/SQL version of the load (same files, same rules as Snowflake).
-- Staging tables hold the files as text; pkg_load moves one file_date at a time into the dimensional model.

BEGIN
  FOR t IN (SELECT table_name FROM user_tables
             WHERE table_name IN ('STG_ACCOUNTS','STG_TRADES','STG_SECURITIES','STG_CONTROL','DIM_ACCOUNT',
                                  'DIM_SECURITY','FACT_TRADE','REJECTED_ROWS','RECON_RESULTS','LOAD_LOG')) LOOP
    EXECUTE IMMEDIATE 'DROP TABLE ' || t.table_name || ' CASCADE CONSTRAINTS PURGE';
  END LOOP;
END;
/

CREATE TABLE stg_accounts (
  account_id VARCHAR2(50), account_name VARCHAR2(200), account_type VARCHAR2(50), address_line VARCHAR2(200),
  city VARCHAR2(100), state VARCHAR2(10), postal_code VARCHAR2(20), risk_tier VARCHAR2(20), status VARCHAR2(20),
  open_date VARCHAR2(20), file_date VARCHAR2(20), file_name VARCHAR2(100), file_row NUMBER
);

CREATE TABLE stg_trades (
  trade_id VARCHAR2(50), trade_date VARCHAR2(20), trade_ts VARCHAR2(30), settlement_date VARCHAR2(20),
  account_id VARCHAR2(50), security_id VARCHAR2(50), side VARCHAR2(10), quantity VARCHAR2(30),
  price VARCHAR2(30), notional VARCHAR2(30), file_date VARCHAR2(20), file_name VARCHAR2(100), file_row NUMBER
);

CREATE TABLE stg_securities (
  security_id VARCHAR2(50), symbol VARCHAR2(20), security_name VARCHAR2(200), asset_class VARCHAR2(20),
  currency VARCHAR2(10)
);

CREATE TABLE stg_control (
  file_name VARCHAR2(100), file_date VARCHAR2(20), row_count VARCHAR2(30), total_quantity VARCHAR2(30),
  total_notional VARCHAR2(30)
);

CREATE TABLE dim_security (
  security_sk   NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  security_id   VARCHAR2(50) NOT NULL UNIQUE,
  symbol        VARCHAR2(20),
  security_name VARCHAR2(200),
  asset_class   VARCHAR2(20),
  currency      VARCHAR2(10)
);

CREATE TABLE dim_account (
  account_sk     NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  account_id     VARCHAR2(50) NOT NULL,
  account_name   VARCHAR2(200),
  account_type   VARCHAR2(50),
  address_line   VARCHAR2(200),
  city           VARCHAR2(100),
  state          VARCHAR2(10),
  postal_code    VARCHAR2(20),
  risk_tier      VARCHAR2(20),
  status         VARCHAR2(20),
  open_date      DATE,
  hash_diff      VARCHAR2(32) NOT NULL,
  effective_from DATE NOT NULL,
  effective_to   DATE NOT NULL,
  is_current     BOOLEAN NOT NULL,
  CONSTRAINT uk_dim_account_version UNIQUE (account_id, effective_from)
);
CREATE INDEX ix_dim_account_current ON dim_account (account_id, is_current);

CREATE TABLE fact_trade (
  trade_id        VARCHAR2(50) PRIMARY KEY,
  account_sk      NUMBER NOT NULL REFERENCES dim_account (account_sk),
  security_sk     NUMBER NOT NULL REFERENCES dim_security (security_sk),
  trade_date      DATE NOT NULL,
  side            VARCHAR2(4) NOT NULL,
  quantity        NUMBER(18,0) NOT NULL,
  price           NUMBER(18,2) NOT NULL,
  notional        NUMBER(20,2) NOT NULL,
  file_name       VARCHAR2(100),
  file_row        NUMBER
);

CREATE TABLE rejected_rows (
  rejected_at    TIMESTAMP DEFAULT SYSTIMESTAMP,
  source_table   VARCHAR2(30),
  file_name      VARCHAR2(100),
  file_row       NUMBER,
  business_key   VARCHAR2(50),
  reject_reason  VARCHAR2(40),
  quantity       VARCHAR2(30),
  notional       VARCHAR2(30)
);

CREATE TABLE recon_results (
  checked_at  TIMESTAMP DEFAULT SYSTIMESTAMP,
  file_name   VARCHAR2(100),
  check_name  VARCHAR2(40),
  expected    NUMBER,
  actual      NUMBER,
  status      VARCHAR2(10)
);

CREATE TABLE load_log (
  log_id      NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  logged_at   TIMESTAMP DEFAULT SYSTIMESTAMP,
  file_date   DATE,
  step        VARCHAR2(40),
  status      VARCHAR2(10),
  row_count   NUMBER,
  message     VARCHAR2(4000)
);
