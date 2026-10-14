"""Snowpark price-outlier check (snowpark/dq_checks.py).

Offline: the DataFrame logic runs in Snowpark's local testing mode (no Snowflake connection).
Snowflake-backed: run() against the real tables in BROKER_DW_TEST, including what it writes back.

Fixture, security S1 on day 1: 10 @ 100, 10 @ 101, 10 @ 99 and a fat-finger 1 @ 150.
  T4's leave-one-out VWAP = (1000 + 1010 + 990) / 30 = 100.00 -> 50 % off -> flagged.
  The others stay under 5 % (2.33 %, 0.89 %, 3.75 %) even with T4 among their peers.
S2 on day 1 has only 2 trades (1 peer each): not judged, however far apart.
S1 on day 2: three trades at 100: nothing to flag.
"""

from datetime import date
from decimal import Decimal

import pytest

from snowpark.dq_checks import find_price_outliers, score_trades

D1, D2 = date(2026, 9, 1), date(2026, 9, 2)
TRADES = [  # trade_id, security, day, quantity, price
    ("T1", "S1", D1, 10, "100.00"), ("T2", "S1", D1, 10, "101.00"), ("T3", "S1", D1, 10, "99.00"),
    ("T4", "S1", D1, 1, "150.00"),
    ("T5", "S2", D1, 10, "50.00"), ("T6", "S2", D1, 10, "500.00"),
    ("T7", "S1", D2, 5, "100.00"), ("T8", "S1", D2, 5, "100.00"), ("T9", "S1", D2, 5, "100.00"),
]
SK = {"S1": 1, "S2": 2}


@pytest.fixture(scope="module")
def local_session():
    from snowflake.snowpark import Session

    session = Session.builder.config("local_testing", True).create()
    yield session
    session.close()


def _frames(session):
    trades = session.create_dataframe(
        [[tid, SK[sec], day, "BUY", qty, float(price), float(Decimal(price) * qty)]
         for tid, sec, day, qty, price in TRADES],
        schema=["TRADE_ID", "SECURITY_SK", "TRADE_DATE", "SIDE", "QUANTITY", "PRICE", "NOTIONAL"])
    securities = session.create_dataframe([[1, "S1", "AAA"], [2, "S2", "BBB"]],
                                          schema=["SECURITY_SK", "SECURITY_ID", "SYMBOL"])
    return trades, securities


def test_only_the_fat_finger_trade_is_flagged(local_session):
    trades, securities = _frames(local_session)
    rows = find_price_outliers(trades, securities, threshold_pct=5.0).collect()
    assert [(r["TRADE_ID"], r["SECURITY_ID"], round(float(r["REFERENCE_VWAP"]), 2), round(float(r["DEVIATION_PCT"]), 4))
            for r in rows] == [("T4", "S1", 100.0, 50.0)]


def test_days_with_too_few_peers_are_not_judged(local_session):
    trades, _ = _frames(local_session)
    judged = {r["TRADE_ID"]: r for r in score_trades(trades, min_peers=2).collect()}
    assert set(judged) == {"T1", "T2", "T3", "T4", "T7", "T8", "T9"}            # S2 day 1 skipped
    assert [round(float(judged[t]["DEVIATION_PCT"]), 2) for t in ("T1", "T2", "T3")] == [2.33, 0.89, 3.75]
    assert {round(float(judged[t]["DEVIATION_PCT"]), 4) for t in ("T7", "T8", "T9")} == {0.0}


def test_threshold_controls_sensitivity(local_session):
    trades, securities = _frames(local_session)
    flagged = {r["TRADE_ID"] for r in find_price_outliers(trades, securities, threshold_pct=2.0).collect()}
    assert flagged == {"T1", "T3", "T4"}


@pytest.mark.snowflake
def test_run_writes_outliers_and_dq_result(sf_test_db):
    from common.connections import snowpark_session
    from snowpark.dq_checks import run
    from tests.conftest import TEST_DB
    from tests.sf_data import accounts, run_procs, securities, trade, trades

    cur = sf_test_db.cursor()
    run_procs(cur)
    securities(cur, [{"security_id": "S1"}, {"security_id": "S2"}])
    accounts(cur, D1, [{"account_id": "A1"}])
    for day in (D1, D2):
        trades(cur, day, [trade(tid, "A1", sec, d, quantity=qty, price=price)
                          for tid, sec, d, qty, price in TRADES if d == day])
    run_procs(cur)

    session = snowpark_session(database=TEST_DB)
    try:
        result = run(session, threshold_pct=5.0)
    finally:
        session.close()

    assert (result["evaluated"], result["flagged"], result["status"], result["max_deviation_pct"]) == (7, 1, "WARN", 50.0)
    outlier = cur.execute("""SELECT trade_id, security_id, reference_vwap, deviation_pct, peer_trades
                               FROM AUDIT.PRICE_OUTLIERS WHERE run_id = %s""", (result["run_id"],)).fetchall()
    assert outlier == [("T4", "S1", Decimal("100.000000"), Decimal("50.0000"), 3)]
    dq = cur.execute("""SELECT check_name, scope, actual, status FROM AUDIT.DQ_RESULTS WHERE run_id = %s""",
                     (result["run_id"],)).fetchall()
    assert dq == [("SNOWPARK_PRICE_OUTLIERS", "GLOBAL", Decimal("1.0000"), "WARN")]


SQL_FILES = ["03_raw_tables.sql", "04_streams.sql", "05_dim_account_scd2.sql", "06_fact_trade.sql",
             "07_fact_daily_position.sql", "08_tasks.sql", "09_data_quality.sql", "10_snowpark_dq.sql"]
