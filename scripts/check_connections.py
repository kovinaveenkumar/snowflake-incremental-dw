"""Read-only connection check. Creates nothing in Snowflake.

Usage: python -m scripts.check_connections
"""

from common.connections import snowflake_connect


def check_snowflake() -> None:
    with snowflake_connect() as conn:
        row = conn.cursor().execute(
            "SELECT CURRENT_ACCOUNT(), CURRENT_REGION(), CURRENT_USER(), CURRENT_ROLE(), CURRENT_VERSION()"
        ).fetchone()
    account, region, user, role, version = row
    print(f"Snowflake OK: account={account} region={region} user={user} role={role} version={version}")


if __name__ == "__main__":
    check_snowflake()
