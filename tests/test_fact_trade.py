"""DIM_SECURITY (Type 1) and FACT_TRADE validation, run against the real procedures on a small fixture.

Each "day" runs the procedures in Task-tree order: SP_LOAD_DIM_SECURITY -> SP_LOAD_DIM_ACCOUNT -> SP_LOAD_FACT_TRADE.
  run 0  first read of each stream (empty SHOW_INITIAL_ROWS snapshot)
  day 1  securities S1, S2; accounts A1, A2 active; 10 trades: 2 good and one of each kind of bad row
  day 2  S1 renamed; A1 moves address, A2 suspended; a trade on A1 dated day 2, a late trade on A1 dated
         day 1, a trade on suspended A2, and a replay of a day-1 trade
  day 3  nothing new
"""

from datetime import date
from decimal import Decimal

import pytest

from tests.sf_data import accounts as _accounts, run_procs, securities as _securities, trade, trades as _trades

pytestmark = pytest.mark.snowflake

SQL_FILES = ["03_raw_tables.sql", "04_streams.sql", "05_dim_account_scd2.sql", "06_fact_trade.sql"]
FACT_PROCS = ("SP_LOAD_DIM_SECURITY", "SP_LOAD_DIM_ACCOUNT", "SP_LOAD_FACT_TRADE")

D1, D2 = date(2026, 9, 1), date(2026, 9, 2)


def _run_all(cur) -> dict:
    return run_procs(cur, FACT_PROCS)


@pytest.fixture(scope="module")
def scenario(sf_test_db):
    cur = sf_test_db.cursor()
    initial = _run_all(cur)

    _securities(cur, [{"security_id": "S1"}, {"security_id": "S2"}])
    _accounts(cur, D1, [{"account_id": "A1"}, {"account_id": "A2"}])
    _trades(cur, D1, [
        trade("T1", "A1", "S1", D1),
        trade("T2", "A2", "S2", D1, side="SELL", quantity=5, price="50.00"),
        trade("T3", "", "S1", D1),
        trade("T4", "A1", "S1", D1, quantity=-3),
        trade("T5", "A1", "UNK1", D1),
        trade("T1", "A1", "S1", D1),                       # exact repeat of T1
        trade("T7", "A1", "S1", D1, notional="999.00"),
        trade("T8", "A1", "S1", D1, side="HOLD"),
        trade("T9", "A9", "S1", D1),
        trade("T10", "A1", "S1", D1, quantity=0),
    ])
    day1 = _run_all(cur)

    _securities(cur, [{"security_id": "S1", "security_name": "Test Corp Renamed"}, {"security_id": "S2"}])
    _accounts(cur, D2, [{"account_id": "A1", "address_line": "99 New Rd"}, {"account_id": "A2", "status": "SUSPENDED"}])
    _trades(cur, D2, [
        trade("T11", "A1", "S1", D2),
        trade("T12", "A1", "S2", D1),                      # late trade: belongs to A1's day-1 version
        trade("T13", "A2", "S1", D2),
        trade("T2", "A2", "S2", D1, side="SELL", quantity=5, price="50.00"),  # replay of a loaded trade
    ])
    day2 = _run_all(cur)
    day3 = _run_all(cur)
    return {"cur": cur, "initial": initial, "day1": day1, "day2": day2, "day3": day3}


def _fact(cur) -> dict[str, dict]:
    cur.execute("SELECT * FROM CORE.FACT_TRADE")
    cols = [d[0].lower() for d in cur.description]
    return {r[0]: dict(zip(cols, r)) for r in cur.fetchall()}


def _sk(cur, account_id: str, on: date) -> int:
    return cur.execute("""SELECT account_sk FROM CORE.DIM_ACCOUNT WHERE account_id = %s
                          AND %s BETWEEN effective_from AND effective_to""", (account_id, on)).fetchone()[0]


def test_initial_read_is_empty(scenario):
    for result in scenario["initial"].values():
        assert result["rows_read"] == 0


def test_day1_two_trades_loaded_eight_rejected(scenario):
    r = scenario["day1"]["SP_LOAD_FACT_TRADE"]
    assert (r["rows_read"], r["rows_inserted"], r["rows_rejected"]) == (10, 2, 8)
    sec = scenario["day1"]["SP_LOAD_DIM_SECURITY"]
    assert (sec["rows_read"], sec["rows_inserted"], sec["rows_updated"]) == (2, 2, 0)


def test_day1_reject_reasons(scenario):
    rows = scenario["cur"].execute("""SELECT business_key, reject_reason FROM AUDIT.REJECTED_ROWS
                                       WHERE source_table = 'RAW.TRADES' AND source_file LIKE '%20260901%'
                                       ORDER BY business_key""").fetchall()
    assert rows == [("T1", "DUPLICATE_TRADE_ID"), ("T10", "ZERO_QUANTITY"), ("T3", "MISSING_ACCOUNT_ID"),
                    ("T4", "NEGATIVE_QUANTITY"), ("T5", "UNKNOWN_SECURITY"), ("T7", "NOTIONAL_MISMATCH"),
                    ("T8", "INVALID_SIDE"), ("T9", "UNKNOWN_ACCOUNT")]


def test_day2_account_version_at_trade_time(scenario):
    cur = scenario["cur"]
    r = scenario["day2"]["SP_LOAD_FACT_TRADE"]
    assert (r["rows_read"], r["rows_inserted"], r["rows_rejected"]) == (4, 2, 2)
    fact = _fact(cur)
    assert fact["T11"]["account_sk"] == _sk(cur, "A1", D2)
    assert fact["T12"]["account_sk"] == _sk(cur, "A1", D1)
    assert _sk(cur, "A1", D1) != _sk(cur, "A1", D2)
    rejected = cur.execute("""SELECT business_key, reject_reason FROM AUDIT.REJECTED_ROWS
                               WHERE source_table = 'RAW.TRADES' AND source_file LIKE '%20260902%'
                               ORDER BY business_key""").fetchall()
    assert rejected == [("T13", "ACCOUNT_NOT_ACTIVE"), ("T2", "DUPLICATE_TRADE_ID")]


def test_day2_security_is_type1(scenario):
    cur = scenario["cur"]
    sec = scenario["day2"]["SP_LOAD_DIM_SECURITY"]
    assert (sec["rows_read"], sec["rows_inserted"], sec["rows_updated"]) == (2, 0, 1)
    rows = cur.execute("SELECT security_id, security_name FROM CORE.DIM_SECURITY ORDER BY 1").fetchall()
    assert rows == [("S1", "Test Corp Renamed"), ("S2", "Test Corp")]
    fact = _fact(cur)
    s1_sk = cur.execute("SELECT security_sk FROM CORE.DIM_SECURITY WHERE security_id = 'S1'").fetchone()[0]
    assert fact["T1"]["security_sk"] == fact["T11"]["security_sk"] == s1_sk  # key unchanged by the update


def test_signed_measures(scenario):
    fact = _fact(scenario["cur"])
    assert (fact["T1"]["signed_quantity"], fact["T1"]["signed_notional"]) == (10, Decimal("1000.00"))
    assert (fact["T2"]["signed_quantity"], fact["T2"]["signed_notional"]) == (-5, Decimal("-250.00"))


def test_day3_empty_run(scenario):
    for result in scenario["day3"].values():
        assert result["rows_read"] == 0


def test_every_row_is_loaded_or_rejected_once(scenario):
    cur = scenario["cur"]
    raw = cur.execute("SELECT COUNT(*) FROM RAW.TRADES").fetchone()[0]
    fact = cur.execute("SELECT COUNT(*), COUNT(DISTINCT trade_id) FROM CORE.FACT_TRADE").fetchone()
    rejected = cur.execute("SELECT COUNT(*) FROM AUDIT.REJECTED_ROWS WHERE source_table = 'RAW.TRADES'").fetchone()[0]
    assert fact[0] == fact[1] == 4
    assert raw == fact[0] + rejected == 14
    orphans = cur.execute("""SELECT COUNT(*) FROM CORE.FACT_TRADE f
                              LEFT JOIN CORE.DIM_ACCOUNT a ON a.account_sk = f.account_sk
                              LEFT JOIN CORE.DIM_SECURITY s ON s.security_sk = f.security_sk
                              WHERE a.account_sk IS NULL OR s.security_sk IS NULL""").fetchone()[0]
    assert orphans == 0
