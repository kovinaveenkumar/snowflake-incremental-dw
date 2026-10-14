"""Reconciliation, control totals and DQ checks (snowflake/09_data_quality.sql) on a small fixture.

  run 0  first read of every stream (empty)
  day 1  2 good trades + 1 bad, control file exact            -> all PASS, reject rate WARN (1 of 3)
  day 2  1 good trade, sender control notional is 1.00 too high -> FAIL, error raised, evidence kept
  day 3  control file arrives before its trades file           -> PENDING (nothing landed)
         trades land in RAW but are not loaded yet             -> PENDING (stream still holds them)
         trades loaded                                         -> PASS
  then   DIM_ACCOUNT corrupted (two current rows for A1)       -> global checks FAIL
"""

from datetime import date
from decimal import Decimal

import pytest
from snowflake.connector.errors import ProgrammingError

from tests.sf_data import accounts, controls, run_dq, run_procs, securities, trade, trades

pytestmark = pytest.mark.snowflake

SQL_FILES = ["03_raw_tables.sql", "04_streams.sql", "05_dim_account_scd2.sql", "06_fact_trade.sql",
             "07_fact_daily_position.sql", "08_tasks.sql", "09_data_quality.sql"]
D1, D2, D3 = (date(2026, 9, d) for d in (1, 2, 3))
TRADE_CHECKS = {"FILE_VS_RAW_ROWS", "FILE_VS_RAW_QUANTITY", "FILE_VS_RAW_NOTIONAL", "RAW_VS_CORE_ROWS",
                "CONTROL_VS_CORE_QUANTITY", "CONTROL_VS_CORE_NOTIONAL", "REJECT_RATE_PCT"}


def _last_run(cur) -> tuple[str, dict[tuple[str, str], str]]:
    run_id = cur.execute("""SELECT load_run_id FROM AUDIT.LOAD_LOG WHERE procedure_name = 'AUDIT.SP_RUN_DQ_CHECKS'
                             ORDER BY started_at DESC LIMIT 1""").fetchone()[0]
    rows = cur.execute("SELECT scope, check_name, status FROM AUDIT.DQ_RESULTS WHERE run_id = %s", (run_id,)).fetchall()
    return run_id, {(scope, check): status for scope, check, status in rows}


def _register(cur) -> dict[str, str]:
    return dict(cur.execute("SELECT file_name, status FROM AUDIT.CONTROL_REGISTER").fetchall())


def _statuses(results: dict, scope: str) -> dict[str, str]:
    return {check: status for (s, check), status in results.items() if s == scope}


@pytest.fixture(scope="module")
def cur(sf_test_db):
    cur = sf_test_db.cursor()
    run_procs(cur)
    run_dq(cur)
    return cur


@pytest.fixture(scope="module")
def day1(cur):
    securities(cur, [{"security_id": "S1"}])
    accounts(cur, D1, [{"account_id": "A1"}, {"account_id": "A2"}])
    rows = [trade("T1", "A1", "S1", D1), trade("T2", "A2", "S1", D1, side="SELL", quantity=3),
            trade("T3", "", "S1", D1)]
    trades(cur, D1, rows)
    controls(cur, D1, trade_rows=rows, account_rows=2)
    run_procs(cur)
    return run_dq(cur), _last_run(cur)[1]


def test_day1_reconciles(cur, day1):
    result, results = day1
    assert (result["controls_registered"], result["fail"], result["pending"]) == (2, 0, 0)
    trades_checks = _statuses(results, "trades_20260901.csv")
    assert set(trades_checks) == TRADE_CHECKS
    assert {c: s for c, s in trades_checks.items() if c != "REJECT_RATE_PCT"} == dict.fromkeys(TRADE_CHECKS - {"REJECT_RATE_PCT"}, "PASS")
    assert trades_checks["REJECT_RATE_PCT"] == "WARN"           # 1 of 3 rejected; a warning does not fail
    assert _statuses(results, "accounts_20260901.csv") == {"FILE_VS_RAW_ROWS": "PASS"}
    assert set(_statuses(results, "GLOBAL").values()) == {"PASS"}
    assert _register(cur) == {"accounts_20260901.csv": "PASS", "trades_20260901.csv": "PASS"}


def test_day2_control_mismatch_fails_loudly_and_keeps_evidence(cur, day1):
    rows = [trade("T4", "A1", "S1", D2)]
    trades(cur, D2, rows)
    controls(cur, D2, trade_rows=rows, notional_adjust=Decimal("1.00"))
    run_procs(cur)
    with pytest.raises(ProgrammingError, match="Data-quality checks FAILED"):
        run_dq(cur)
    run_id, results = _last_run(cur)
    checks = _statuses(results, "trades_20260902.csv")
    assert {c for c, s in checks.items() if s == "FAIL"} == {"FILE_VS_RAW_NOTIONAL", "CONTROL_VS_CORE_NOTIONAL"}
    assert checks["RAW_VS_CORE_ROWS"] == checks["FILE_VS_RAW_ROWS"] == "PASS"
    detail = cur.execute("""SELECT details FROM AUDIT.DQ_RESULTS WHERE run_id = %s
                             AND check_name = 'FILE_VS_RAW_NOTIONAL'""", (run_id,)).fetchone()[0]
    assert detail == "difference -1.0000"
    assert _register(cur)["trades_20260902.csv"] == "FAIL"
    assert cur.execute("SELECT status FROM AUDIT.LOAD_LOG WHERE load_run_id = %s", (run_id,)).fetchone()[0] == "DQ_FAILED"


def test_day3_pending_until_loaded_then_pass(cur, day1):
    rows = [trade("T5", "A2", "S1", D3)]
    controls(cur, D3, trade_rows=rows)
    run_procs(cur)
    assert run_dq(cur)["pending"] == 7
    assert set(_statuses(_last_run(cur)[1], "trades_20260903.csv").values()) == {"PENDING"}

    trades(cur, D3, rows)                                      # landed in RAW, not loaded yet
    assert run_dq(cur)["pending"] == 7
    assert _register(cur)["trades_20260903.csv"] == "PENDING"

    run_procs(cur)
    result = run_dq(cur)
    assert (result["fail"], result["pending"]) == (0, 0)
    assert set(_statuses(_last_run(cur)[1], "trades_20260903.csv").values()) == {"PASS"}
    assert _register(cur)["trades_20260903.csv"] == "PASS"


def test_closed_files_are_not_rechecked(cur, day1):
    run_dq(cur)
    assert _statuses(_last_run(cur)[1], "trades_20260901.csv") == {}


def test_global_integrity_failure(cur, day1):
    accounts(cur, D3, [{"account_id": "A1", "address_line": "99 New Rd"}])
    run_procs(cur)
    cur.execute("""UPDATE CORE.DIM_ACCOUNT SET is_current = TRUE
                    WHERE account_id = 'A1' AND effective_from = '2026-09-01'""")
    with pytest.raises(ProgrammingError, match="Data-quality checks FAILED"):
        run_dq(cur)
    failed = {c for c, s in _statuses(_last_run(cur)[1], "GLOBAL").items() if s == "FAIL"}
    assert failed == {"DIM_ACCOUNT_ONE_CURRENT_ROW", "DIM_ACCOUNT_CONTIGUOUS_RANGES"}
