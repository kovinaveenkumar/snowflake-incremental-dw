"""Shared fixtures. Snowflake-backed tests run only with RUN_SNOWFLAKE_TESTS=1.

They build a temporary TRANSIENT database (BROKER_DW_TEST) from the project's own SQL files, so the
code under test is exactly what runs in BROKER_DW, and drop it afterwards.
"""

import os
from pathlib import Path

import pytest

SQL_DIR = Path(__file__).resolve().parents[1] / "snowflake"
TEST_DB = "BROKER_DW_TEST"
SCHEMAS = ["RAW", "STAGE", "CORE", "MART", "AUDIT"]


def pytest_configure(config):
    config.addinivalue_line("markers", "snowflake: needs a live Snowflake connection (RUN_SNOWFLAKE_TESTS=1)")


def pytest_collection_modifyitems(config, items):
    if os.getenv("RUN_SNOWFLAKE_TESTS") == "1":
        return
    skip = pytest.mark.skip(reason="set RUN_SNOWFLAKE_TESTS=1 to run Snowflake-backed tests")
    for item in items:
        if "snowflake" in item.keywords:
            item.add_marker(skip)


def build_test_db(conn, sql_files: list[str]) -> None:
    from scripts.run_sql import execute, render

    execute(conn, f"CREATE OR REPLACE TRANSIENT DATABASE {TEST_DB}; USE DATABASE {TEST_DB};"
                  + "".join(f" CREATE SCHEMA {s};" for s in SCHEMAS))
    for name in sql_files:
        sql = render((SQL_DIR / name).read_text()).replace("USE DATABASE BROKER_DW;", f"USE DATABASE {TEST_DB};")
        execute(conn, sql)


@pytest.fixture(scope="module")
def sf_test_db(request):
    """Open connection to a fresh BROKER_DW_TEST built from the SQL files named by the test module."""
    from common.connections import snowflake_connect

    conn = snowflake_connect()
    try:
        build_test_db(conn, request.module.SQL_FILES)
        yield conn
    finally:
        conn.cursor().execute(f"DROP DATABASE IF EXISTS {TEST_DB}")
        conn.close()
