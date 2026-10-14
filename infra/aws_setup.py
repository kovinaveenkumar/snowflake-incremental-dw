"""AWS resources for the project: one S3 bucket and one IAM role Snowflake assumes to read it.

Each subcommand does one thing, so each can be approved separately:
  python -m infra.aws_setup plan            show what would be created (read-only)
  python -m infra.aws_setup create-bucket   private bucket, public access blocked, SSE-S3
  python -m infra.aws_setup create-role     IAM role + inline read-only policy on s3://<bucket>/broker/*
                                            (trust starts as this AWS account only; tightened below)
  python -m infra.aws_setup update-trust    trust only Snowflake's IAM user with the integration's
                                            external ID (read from DESC INTEGRATION S3_BROKER_INT)
  python -m infra.aws_setup teardown        delete the role and the bucket (bucket must be empty)
"""

from __future__ import annotations

import json
import sys

from common.connections import aws_session, env, snowflake_connect

ROLE_NAME = "snowflake-broker-dw-s3-read"
POLICY_NAME = "s3-broker-prefix-read"
PREFIX = "broker/"


def _bucket() -> str:
    return env("S3_BUCKET")


def read_policy(bucket: str) -> dict:
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject", "s3:GetObjectVersion"],
                "Resource": f"arn:aws:s3:::{bucket}/{PREFIX}*",
            },
            {
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": f"arn:aws:s3:::{bucket}",
                "Condition": {"StringLike": {"s3:prefix": [f"{PREFIX}*"]}},
            },
            {
                # GetBucketLocation carries no s3:prefix key, so it cannot share the condition above.
                "Effect": "Allow",
                "Action": "s3:GetBucketLocation",
                "Resource": f"arn:aws:s3:::{bucket}",
            },
        ],
    }


def trust_policy(principal_arn: str, external_id: str | None = None) -> dict:
    statement = {"Effect": "Allow", "Principal": {"AWS": principal_arn}, "Action": "sts:AssumeRole"}
    if external_id:
        statement["Condition"] = {"StringEquals": {"sts:ExternalId": external_id}}
    return {"Version": "2012-10-17", "Statement": [statement]}


def plan() -> None:
    session = aws_session()
    account = session.client("sts").get_caller_identity()["Account"]
    print(f"AWS account {account}, region {session.region_name}")
    print(f"Bucket: s3://{_bucket()} (private, public access blocked, SSE-S3, landing prefix {PREFIX})")
    print(f"IAM role: {ROLE_NAME}\nInline policy {POLICY_NAME}:")
    print(json.dumps(read_policy(_bucket()), indent=2))


def create_bucket() -> None:
    session = aws_session()
    s3, bucket, region = session.client("s3"), _bucket(), session.region_name
    kwargs = {"Bucket": bucket}
    if region != "us-east-1":
        kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}
    s3.create_bucket(**kwargs)
    s3.put_public_access_block(Bucket=bucket, PublicAccessBlockConfiguration={
        "BlockPublicAcls": True, "IgnorePublicAcls": True,
        "BlockPublicPolicy": True, "RestrictPublicBuckets": True,
    })
    s3.put_bucket_encryption(Bucket=bucket, ServerSideEncryptionConfiguration={
        "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}],
    })
    s3.put_bucket_tagging(Bucket=bucket, Tagging={"TagSet": [{"Key": "project", "Value": "snowflake-incremental-dw"}]})
    print(f"Created s3://{bucket} in {region}")


def create_role() -> None:
    session = aws_session()
    iam = session.client("iam")
    account = session.client("sts").get_caller_identity()["Account"]
    role = iam.create_role(
        RoleName=ROLE_NAME,
        AssumeRolePolicyDocument=json.dumps(trust_policy(f"arn:aws:iam::{account}:root")),
        Description="Assumed by the Snowflake storage integration S3_BROKER_INT (read-only on broker/)",
        Tags=[{"Key": "project", "Value": "snowflake-incremental-dw"}],
    )["Role"]
    iam.put_role_policy(RoleName=ROLE_NAME, PolicyName=POLICY_NAME, PolicyDocument=json.dumps(read_policy(_bucket())))
    print(f"Created role {role['Arn']}")
    print(f"Put this in .env:  SNOWFLAKE_IAM_ROLE_ARN={role['Arn']}")


def update_trust() -> None:
    with snowflake_connect(role="ACCOUNTADMIN") as conn:
        rows = conn.cursor().execute("DESC INTEGRATION S3_BROKER_INT").fetchall()
    props = {r[0]: r[2] for r in rows}
    user_arn, external_id = props["STORAGE_AWS_IAM_USER_ARN"], props["STORAGE_AWS_EXTERNAL_ID"]
    aws_session().client("iam").update_assume_role_policy(
        RoleName=ROLE_NAME, PolicyDocument=json.dumps(trust_policy(user_arn, external_id)),
    )
    print(f"Role {ROLE_NAME} now trusts only {user_arn} with the integration's external ID")


def teardown() -> None:
    session = aws_session()
    iam = session.client("iam")
    iam.delete_role_policy(RoleName=ROLE_NAME, PolicyName=POLICY_NAME)
    iam.delete_role(RoleName=ROLE_NAME)
    session.client("s3").delete_bucket(Bucket=_bucket())
    print(f"Deleted role {ROLE_NAME} and bucket {_bucket()}")


COMMANDS = {
    "plan": plan, "create-bucket": create_bucket, "create-role": create_role,
    "update-trust": update_trust, "teardown": teardown,
}

if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in COMMANDS:
        sys.exit(__doc__)
    COMMANDS[sys.argv[1]]()
