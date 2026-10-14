-- Step 2: storage integration so Snowflake can read the S3 bucket through an IAM role
-- (no AWS keys stored in Snowflake). Run as ACCOUNTADMIN after the IAM role exists.
-- After running, copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID from DESC
-- into the IAM role's trust policy (python -m infra.aws_setup update-trust does this).

USE ROLE ACCOUNTADMIN;

CREATE STORAGE INTEGRATION IF NOT EXISTS S3_BROKER_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '{{SNOWFLAKE_IAM_ROLE_ARN}}'
  STORAGE_ALLOWED_LOCATIONS = ('s3://{{S3_BUCKET}}/broker/');

GRANT USAGE ON INTEGRATION S3_BROKER_INT TO ROLE DW_ROLE;

DESC INTEGRATION S3_BROKER_INT;
