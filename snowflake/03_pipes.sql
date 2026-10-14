-- Step 3: one Snowpipe per feed on RAW.LANDING.
-- Internal stages cannot raise cloud storage events, so files are announced to each pipe through the
-- Snowpipe REST API (insertFiles) by generator/upload_to_stage.py. Paths sent to the API are relative
-- to the stage root (e.g. trades/trades_20260901.csv.gz), and each pipe's PATTERN picks its own feed.
-- Snowpipe remembers loaded file names for 14 days, so re-sending a file does not load it twice.
-- ON_ERROR = SKIP_FILE: RAW columns are all text, so only a structurally broken file is skipped;
-- bad values are caught downstream and written to AUDIT.REJECTED_ROWS.

USE ROLE DW_ROLE;
USE DATABASE BROKER_DW;

CREATE PIPE IF NOT EXISTS RAW.PIPE_ACCOUNTS
  COMMENT = 'Daily account files -> RAW.ACCOUNTS'
AS
COPY INTO RAW.ACCOUNTS (
  account_id, account_name, account_type, address_line, city, state, postal_code,
  risk_tier, status, open_date, file_date, _file_name, _file_row_number, _loaded_at
)
FROM (
  SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
         METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME
  FROM @RAW.LANDING
)
PATTERN = '.*accounts/accounts_[0-9]{8}[.]csv([.]gz)?'
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_CSV')
ON_ERROR = 'SKIP_FILE';

CREATE PIPE IF NOT EXISTS RAW.PIPE_TRADES
  COMMENT = 'Daily trade files -> RAW.TRADES'
AS
COPY INTO RAW.TRADES (
  trade_id, trade_date, trade_ts, settlement_date, account_id, security_id, side,
  quantity, price, notional, file_date, _file_name, _file_row_number, _loaded_at
)
FROM (
  SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
         METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME
  FROM @RAW.LANDING
)
PATTERN = '.*trades/trades_[0-9]{8}[.]csv([.]gz)?'
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_CSV')
ON_ERROR = 'SKIP_FILE';

CREATE PIPE IF NOT EXISTS RAW.PIPE_CONTROL_TOTALS
  COMMENT = 'Sender control totals -> RAW.CONTROL_TOTALS'
AS
COPY INTO RAW.CONTROL_TOTALS (
  file_name, file_date, row_count, total_quantity, total_notional,
  _file_name, _file_row_number, _loaded_at
)
FROM (
  SELECT $1, $2, $3, $4, $5,
         METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME
  FROM @RAW.LANDING
)
PATTERN = '.*control/control_[0-9]{8}[.]csv([.]gz)?'
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_CSV')
ON_ERROR = 'SKIP_FILE';

CREATE PIPE IF NOT EXISTS RAW.PIPE_SECURITIES
  COMMENT = 'Security master -> RAW.SECURITIES'
AS
COPY INTO RAW.SECURITIES (
  security_id, symbol, security_name, asset_class, currency,
  _file_name, _file_row_number, _loaded_at
)
FROM (
  SELECT $1, $2, $3, $4, $5,
         METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME
  FROM @RAW.LANDING
)
PATTERN = '.*reference/securities[.]csv([.]gz)?'
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_CSV')
ON_ERROR = 'SKIP_FILE';

SHOW PIPES IN SCHEMA RAW;
