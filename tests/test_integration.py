"""End to end in BROKER_DW_TEST, with nothing mocked:
generate files -> PUT to the stage -> Snowpipe REST insertFiles -> wait for COPY_HISTORY -> run the real Task
graph once -> assert every layer against the generator's own manifest and a replay of the SCD2 rules.
"""

import csv
from datetime import date

import pytest

from tests.sf_data import run_dq, run_procs

pytestmark = pytest.mark.snowflake

SQL_FILES = ["01_stage.sql", "02_raw_tables.sql", "03_pipes.sql", "04_streams.sql", "05_dim_account_scd2.sql",
             "06_fact_trade.sql", "07_fact_daily_position.sql", "08_tasks.sql", "09_data_quality.sql",
             "10_snowpark_dq.sql"]
FILE_FORMAT = """CREATE FILE FORMAT RAW.FF_CSV TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                 EMPTY_FIELD_AS_NULL = TRUE TRIM_SPACE = TRUE ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE"""
TRACKED = ["account_name", "account_type", "address_line", "city", "state", "postal_code", "risk_tier", "status",
           "open_date"]


@pytest.fixture(scope="module")
def sf_test_db(request, monkeypatch_module):
    """Like conftest.sf_test_db, but the stage needs the file format first, and Snowpipe REST calls target the test DB."""
    from common.connections import snowflake_connect
    from scripts.run_sql import execute, render
    from tests.conftest import SCHEMAS, SQL_DIR, TEST_DB

    monkeypatch_module.setenv("SNOWFLAKE_DATABASE", TEST_DB)
    conn = snowflake_connect()
    try:
        execute(conn, f"CREATE OR REPLACE TRANSIENT DATABASE {TEST_DB}; USE DATABASE {TEST_DB};"
                      + "".join(f" CREATE SCHEMA {s};" for s in SCHEMAS) + f" {FILE_FORMAT};")
        for name in SQL_FILES:
            execute(conn, render((SQL_DIR / name).read_text()).replace("USE DATABASE BROKER_DW;", f"USE DATABASE {TEST_DB};"))
        yield conn
    finally:
        conn.cursor().execute(f"DROP DATABASE IF EXISTS {TEST_DB}")
        conn.close()


@pytest.fixture(scope="module")
def monkeypatch_module():
    mp = pytest.MonkeyPatch()
    yield mp
    mp.undo()


@pytest.fixture(scope="module")
def pipeline(sf_test_db, tmp_path_factory):
    import time

    from generator.generate_files import GeneratorConfig, generate, write_files
    from generator.upload_to_stage import days_in_range, files_for, notify_pipes, put_files, wait_for_loads
    from scripts.tasks import run_once, task_runs

    data_dir = tmp_path_factory.mktemp("raw")
    manifest = write_files(generate(GeneratorConfig(seed=11, n_accounts=40, n_days=3, n_securities=15,
                                                    trades_per_day=(150, 170))), data_dir)
    cur = sf_test_db.cursor()
    run_procs(cur)   # first read of every stream (empty SHOW_INITIAL_ROWS snapshot)
    run_dq(cur)

    first, last = (date.fromisoformat(manifest["days"][i]["file_date"]) for i in (0, -1))
    days = days_in_range(data_dir, first, last)
    plan = [f for i, d in enumerate(days) for f in files_for(d, data_dir, reference=(i == 0))]
    started = time.time()
    put_files(cur, plan)
    notify_pipes(plan)
    loads = wait_for_loads(cur, plan, started)
    run = run_once(cur, wait=True)
    return {"cur": cur, "manifest": manifest, "data_dir": data_dir, "loads": loads, "run": run,
            "tasks": {t["name"]: t["state"] for t in task_runs(cur, run["run_id"])}}


def test_every_file_loaded_by_snowpipe(pipeline):
    loads = pipeline["loads"]
    assert len(loads) == 1 + 3 * 3
    assert {r["status"] for r in loads} == {"Loaded"}
    assert all(r["error_count"] == 0 for r in loads)


def test_task_graph_succeeded(pipeline):
    assert pipeline["run"]["state"] == "SUCCEEDED", pipeline["run"]
    assert set(pipeline["tasks"].values()) == {"SUCCEEDED"} and len(pipeline["tasks"]) == 5


def test_row_counts_match_manifest(pipeline):
    cur, totals = pipeline["cur"], pipeline["manifest"]["totals"]
    injected = totals["injected_bad_rows"]
    raw, fact, rejected = cur.execute("""SELECT (SELECT COUNT(*) FROM RAW.TRADES), (SELECT COUNT(*) FROM CORE.FACT_TRADE),
                                                (SELECT COUNT(*) FROM AUDIT.REJECTED_ROWS WHERE source_table = 'RAW.TRADES')""").fetchone()
    assert (raw, fact, rejected) == (totals["trade_rows"], totals["trade_rows"] - injected, injected)
    expected = sorted((i["trade_id"], i["type"]) for d in pipeline["manifest"]["days"] for i in d["injected_bad_rows"])
    assert sorted(cur.execute("""SELECT business_key, reject_reason FROM AUDIT.REJECTED_ROWS
                                  WHERE source_table = 'RAW.TRADES'""").fetchall()) == expected


def test_scd2_history_and_current_flags(pipeline):
    current, versions, closed = {}, 0, 0
    for day in pipeline["manifest"]["days"]:
        for r in csv.DictReader(open(pipeline["data_dir"] / day["accounts_file"])):
            key = tuple(r[k] for k in TRACKED)
            if r["account_id"] not in current or current[r["account_id"]] != key:
                versions += 1
                closed += r["account_id"] in current
            current[r["account_id"]] = key
    cur = pipeline["cur"]
    got = cur.execute("SELECT COUNT(*), COUNT_IF(is_current), COUNT_IF(NOT is_current) FROM CORE.DIM_ACCOUNT").fetchone()
    assert got == (versions, len(current), closed)
    assert cur.execute("""SELECT COUNT(*) FROM (SELECT account_id FROM CORE.DIM_ACCOUNT
                           GROUP BY 1 HAVING COUNT_IF(is_current) <> 1)""").fetchone()[0] == 0


def test_dq_reconciled_every_file(pipeline):
    cur = pipeline["cur"]
    assert cur.execute("SELECT status, COUNT(*) FROM AUDIT.CONTROL_REGISTER GROUP BY 1").fetchall() == [("PASS", 6)]
    message = cur.execute("""SELECT message FROM AUDIT.LOAD_LOG WHERE procedure_name = 'AUDIT.SP_RUN_DQ_CHECKS'
                              ORDER BY started_at DESC LIMIT 1""").fetchone()[0]
    assert " 0 fail" in message and " 0 pending" in message, message


def test_positions_for_every_day(pipeline):
    cur = pipeline["cur"]
    assert cur.execute("SELECT COUNT(DISTINCT position_date) FROM CORE.FACT_DAILY_POSITION").fetchone()[0] == 3
    assert cur.execute("SELECT SUM(trade_count) FROM CORE.FACT_DAILY_POSITION").fetchone()[0] == \
        cur.execute("SELECT COUNT(*) FROM CORE.FACT_TRADE").fetchone()[0]
