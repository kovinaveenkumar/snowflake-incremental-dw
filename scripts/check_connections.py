"""Read-only connection check. Creates nothing in Snowflake or AWS.

Usage: python -m scripts.check_connections
"""

from common.connections import aws_session, env, snowflake_connect


def check_snowflake() -> None:
    with snowflake_connect() as conn:
        row = conn.cursor().execute(
            "SELECT CURRENT_ACCOUNT(), CURRENT_REGION(), CURRENT_USER(), CURRENT_ROLE(), CURRENT_VERSION()"
        ).fetchone()
    account, region, user, role, version = row
    print(f"Snowflake OK: account={account} region={region} user={user} role={role} version={version}")


def check_aws() -> None:
    session = aws_session()
    identity = session.client("sts").get_caller_identity()
    print(f"AWS OK: account={identity['Account']} arn={identity['Arn']} region={session.region_name}")
    bucket = env("S3_BUCKET", required=False)
    if bucket:
        s3 = session.client("s3")
        try:
            s3.head_bucket(Bucket=bucket)
            print(f"S3 bucket {bucket}: exists and is reachable")
        except s3.exceptions.ClientError as e:
            print(f"S3 bucket {bucket}: not created yet or not accessible ({e.response['Error']['Code']})")


if __name__ == "__main__":
    for name, check in [("Snowflake", check_snowflake), ("AWS", check_aws)]:
        try:
            check()
        except SystemExit as e:
            print(f"{name} skipped: {e}")
        except Exception as e:  # report and keep going so both sides are checked
            print(f"{name} FAILED: {type(e).__name__}: {e}")
