"""FACT_DAILY_POSITION snapshot rules, run against the real procedures on a small fixture.

  run 0  first read of every stream (empty)
  day 1  A1 buys S1 twice (15 @ last 102), A2 sells S2 short (-3 @ 50)
  day 2  A2 moves address (new account version); A1 sells all S1 (-> 0), A2 covers S2 (-> 0), A2 buys S1 (7)
  day 3  A1 buys S2 (4); A2's S1 holding carries forward with no trades
  day 4  file holds only a LATE trade dated day 1: A1 buys S2 (2) -> snapshot rebuilt from day 1
  then   an empty run
"""

from datetime import date
from decimal import Decimal

import pytest

from tests.sf_data import accounts, run_procs, securities, trade, trades

pytestmark = pytest.mark.snowflake

SQL_FILES = ["03_raw_tables.sql", "04_streams.sql", "05_dim_account_scd2.sql", "06_fact_trade.sql",
             "07_fact_daily_position.sql"]

D1, D2, D3, D4 = (date(2026, 9, d) for d in (1, 2, 3, 4))
POS = "SP_LOAD_FACT_DAILY_POSITION"


def _snapshot(cur) -> dict[tuple, dict]:
    cur.execute("""SELECT p.position_date, p.account_id, s.security_id, p.account_sk, p.position_quantity,
                          p.net_quantity, p.trade_count, p.close_price, p.market_value
                     FROM CORE.FACT_DAILY_POSITION p JOIN CORE.DIM_SECURITY s ON s.security_sk = p.security_sk""")
    cols = [d[0].lower() for d in cur.description]
    return {(r[0], r[1], r[2]): dict(zip(cols, r)) for r in cur.fetchall()}


def _key(snap: dict) -> dict[tuple, tuple]:
    """(day, account, security) -> (position, trade_count, close_price, market_value)"""
    return {k: (v["position_quantity"], v["trade_count"], v["close_price"], v["market_value"]) for k, v in snap.items()}


@pytest.fixture(scope="module")
def scenario(sf_test_db):
    cur = sf_test_db.cursor()
    out = {"cur": cur, "initial": run_procs(cur)}

    securities(cur, [{"security_id": "S1"}, {"security_id": "S2"}])
    accounts(cur, D1, [{"account_id": "A1"}, {"account_id": "A2"}])
    trades(cur, D1, [
        trade("T1", "A1", "S1", D1, quantity=10, price="100.00", ts="10:00:00"),
        trade("T2", "A1", "S1", D1, quantity=5, price="102.00", ts="11:00:00"),
        trade("T3", "A2", "S2", D1, side="SELL", quantity=3, price="50.00", ts="10:00:00"),
    ])
    out["day1"] = run_procs(cur)
    out["snap1"] = _snapshot(cur)

    accounts(cur, D2, [{"account_id": "A2", "address_line": "7 Elm St"}])
    trades(cur, D2, [
        trade("T4", "A1", "S1", D2, side="SELL", quantity=15, price="104.00", ts="10:00:00"),
        trade("T5", "A2", "S2", D2, quantity=3, price="52.00", ts="10:00:00"),
        trade("T6", "A2", "S1", D2, quantity=7, price="103.00", ts="12:00:00"),
    ])
    out["day2"] = run_procs(cur)

    trades(cur, D3, [trade("T7", "A1", "S2", D3, quantity=4, price="55.00")])
    out["day3"] = run_procs(cur)
    out["snap3"] = _snapshot(cur)

    trades(cur, D4, [trade("T8", "A1", "S2", D1, quantity=2, price="49.00", ts="09:45:00")])
    out["day4"] = run_procs(cur)
    out["snap4"] = _snapshot(cur)

    out["empty"] = run_procs(cur)
    out["snap_empty"] = _snapshot(cur)
    return out


def test_day1_positions_and_close_price(scenario):
    assert _key(scenario["snap1"]) == {
        (D1, "A1", "S1"): (15, 2, Decimal("102.00"), Decimal("1530.00")),   # close = last trade by time
        (D1, "A2", "S2"): (-3, 1, Decimal("50.00"), Decimal("-150.00")),    # short position
    }


def test_days_1_to_3_closed_positions_and_carry_forward(scenario):
    assert scenario["day3"][POS]["rows_deleted"] == 0
    assert _key(scenario["snap3"]) == {
        (D1, "A1", "S1"): (15, 2, Decimal("102.00"), Decimal("1530.00")),
        (D1, "A2", "S2"): (-3, 1, Decimal("50.00"), Decimal("-150.00")),
        (D2, "A1", "S1"): (0, 1, Decimal("103.00"), Decimal("0.00")),       # closed out: shown on that day
        (D2, "A2", "S2"): (0, 1, Decimal("52.00"), Decimal("0.00")),
        (D2, "A2", "S1"): (7, 1, Decimal("103.00"), Decimal("721.00")),
        (D3, "A2", "S1"): (7, 0, Decimal("103.00"), Decimal("721.00")),     # no trades: carried, price carried
        (D3, "A1", "S2"): (4, 1, Decimal("55.00"), Decimal("220.00")),
    }


def test_account_version_follows_position_date(scenario):
    cur, snap = scenario["cur"], scenario["snap3"]
    v1, v2 = [r[0] for r in cur.execute("""SELECT account_sk FROM CORE.DIM_ACCOUNT WHERE account_id = 'A2'
                                             ORDER BY effective_from""").fetchall()]
    assert snap[(D1, "A2", "S2")]["account_sk"] == v1
    assert snap[(D2, "A2", "S1")]["account_sk"] == v2
    assert snap[(D3, "A2", "S1")]["account_sk"] == v2


def test_late_trade_rebuilds_from_its_date(scenario):
    result = scenario["day4"][POS]
    assert result["rebuilt_from"] == D1.isoformat()
    assert result["rows_deleted"] == len(scenario["snap3"])
    assert _key(scenario["snap4"]) == {
        (D1, "A1", "S1"): (15, 2, Decimal("102.00"), Decimal("1530.00")),
        (D1, "A2", "S2"): (-3, 1, Decimal("50.00"), Decimal("-150.00")),
        (D1, "A1", "S2"): (2, 1, Decimal("50.00"), Decimal("100.00")),      # S2 close still T3 (10:00 > 09:45)
        (D2, "A1", "S1"): (0, 1, Decimal("103.00"), Decimal("0.00")),
        (D2, "A2", "S2"): (0, 1, Decimal("52.00"), Decimal("0.00")),
        (D2, "A2", "S1"): (7, 1, Decimal("103.00"), Decimal("721.00")),
        (D2, "A1", "S2"): (2, 0, Decimal("52.00"), Decimal("104.00")),
        (D3, "A2", "S1"): (7, 0, Decimal("103.00"), Decimal("721.00")),
        (D3, "A1", "S2"): (6, 1, Decimal("55.00"), Decimal("330.00")),
    }


def test_empty_run_changes_nothing(scenario):
    assert scenario["empty"][POS]["trades_read"] == 0
    assert scenario["snap_empty"] == scenario["snap4"]


def test_snapshot_equals_running_sum_of_trades(scenario):
    """Independent check: every snapshot row equals the sum of signed trades up to that day."""
    mismatches = scenario["cur"].execute("""
        SELECT COUNT(*)
          FROM CORE.FACT_DAILY_POSITION p
         WHERE p.position_quantity <> (
               SELECT COALESCE(SUM(f.signed_quantity), 0)
                 FROM CORE.FACT_TRADE f JOIN CORE.DIM_ACCOUNT a ON a.account_sk = f.account_sk
                WHERE a.account_id = p.account_id AND f.security_sk = p.security_sk
                  AND f.trade_date <= p.position_date)""").fetchone()[0]
    assert mismatches == 0
