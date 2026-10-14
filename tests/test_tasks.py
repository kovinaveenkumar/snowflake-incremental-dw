"""Task graph (snowflake/08_tasks.sql).

Offline: the graph's order matches the order the procedures are designed (and tested) to run in.
Snowflake-backed: the real graph runs end to end in BROKER_DW_TEST and loads every layer.
"""

import re
from datetime import date
from pathlib import Path

import pytest

from tests.sf_data import LOAD_ORDER, accounts, run_procs, securities, trade, trades

SQL_FILES = ["03_raw_tables.sql", "04_streams.sql", "05_dim_account_scd2.sql", "06_fact_trade.sql",
             "07_fact_daily_position.sql", "08_tasks.sql"]
TASKS_SQL = (Path(__file__).resolve().parents[1] / "snowflake" / "08_tasks.sql").read_text()
D1 = date(2026, 9, 1)


def _tasks() -> dict[str, dict]:
    tasks = {}
    for name, body in re.findall(r"CREATE OR REPLACE TASK (CORE\.\w+)(.*?CALL CORE\.\w+\(\));", TASKS_SQL, flags=re.S):
        after = re.search(r"AFTER (CORE\.\w+)", body)
        tasks[name] = {"after": after.group(1) if after else None,
                       "call": re.search(r"CALL CORE\.(\w+)\(\)", body).group(1), "body": body}
    return tasks


def test_graph_runs_procedures_in_load_order():
    tasks = _tasks()
    (root,) = [n for n, t in tasks.items() if t["after"] is None]
    chain, current = [], root
    while current:
        chain.append(tasks[current]["call"])
        current = next((n for n, t in tasks.items() if t["after"] == current), None)
    assert tuple(chain) == LOAD_ORDER


def test_root_is_scheduled_and_gated_on_raw_streams():
    root = next(t for t in _tasks().values() if t["after"] is None)["body"]
    assert "SCHEDULE = '5 MINUTE'" in root
    assert "ALLOW_OVERLAPPING_EXECUTION = FALSE" in root
    gated = set(re.findall(r"SYSTEM\$STREAM_HAS_DATA\('(RAW\.\w+)'\)", root))
    assert gated == {"RAW.STRM_SECURITIES", "RAW.STRM_ACCOUNTS", "RAW.STRM_TRADES"}


@pytest.mark.snowflake
def test_graph_loads_every_layer(sf_test_db):
    from scripts.tasks import run_once, task_runs

    cur = sf_test_db.cursor()
    run_procs(cur)  # first read of every stream (empty SHOW_INITIAL_ROWS snapshot)

    securities(cur, [{"security_id": "S1"}])
    accounts(cur, D1, [{"account_id": "A1"}])
    trades(cur, D1, [trade("T1", "A1", "S1", D1), trade("T2", "A1", "S1", D1, side="SELL", quantity=4),
                     trade("T3", "", "S1", D1)])

    run = run_once(cur, wait=True)
    assert run["state"] == "SUCCEEDED", run
    states = {t["name"]: t["state"] for t in task_runs(cur, run["run_id"])}
    assert states == {"TASK_ROOT": "SUCCEEDED", "TASK_LOAD_DIM_ACCOUNT": "SUCCEEDED",
                      "TASK_LOAD_FACT_TRADE": "SUCCEEDED", "TASK_LOAD_FACT_DAILY_POSITION": "SUCCEEDED"}

    counts = cur.execute("""SELECT (SELECT COUNT(*) FROM CORE.DIM_SECURITY), (SELECT COUNT(*) FROM CORE.DIM_ACCOUNT),
                                   (SELECT COUNT(*) FROM CORE.FACT_TRADE), (SELECT COUNT(*) FROM AUDIT.REJECTED_ROWS),
                                   (SELECT position_quantity FROM CORE.FACT_DAILY_POSITION)""").fetchone()
    assert counts == (1, 1, 2, 1, 6)

    # Nothing new: the root's WHEN condition is false, so the run is SKIPPED (no warehouse, no load).
    again = run_once(cur, wait=True)
    assert again["state"] == "SKIPPED", again
    assert "evaluated to false" in again["first_error_message"]
    assert cur.execute("SELECT COUNT(*) FROM CORE.FACT_TRADE").fetchone()[0] == 2
