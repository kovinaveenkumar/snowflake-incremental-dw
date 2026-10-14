-- Step 2: external stage on the S3 landing prefix.
-- (Snowpipes for both feeds are added here in Step 3.)

USE ROLE DW_ROLE;
USE DATABASE BROKER_DW;

CREATE STAGE IF NOT EXISTS RAW.S3_BROKER_STAGE
  URL = 's3://{{S3_BUCKET}}/broker/'
  STORAGE_INTEGRATION = S3_BROKER_INT
  FILE_FORMAT = RAW.FF_CSV
  COMMENT = 'Landing zone: broker/accounts/, broker/trades/, broker/control/, broker/reference/';
