-- Step 2: internal stage where the daily broker files land (PUT from generator/upload_to_stage.py).
-- Server-side encrypted; the directory table lists staged files for reconciliation (files vs RAW).

USE ROLE DW_ROLE;
USE DATABASE BROKER_DW;

CREATE STAGE IF NOT EXISTS RAW.LANDING
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
  DIRECTORY = (ENABLE = TRUE)
  FILE_FORMAT = RAW.FF_CSV
  COMMENT = 'Landing zone: accounts/, trades/, control/, reference/';
