"""SCD Type 2 rules for CORE.DIM_ACCOUNT, run against the real procedure on a small fixture.

Scenario (each block is one call of CORE.SP_LOAD_DIM_ACCOUNT):
  run 1  day 1: A1, A2, A3 new
  run 2  day 2: A1 address changed, A2 unchanged, A4 new, one row with no account_id, A5 with a bad status
  run 3  days 3+4 in one catch-up batch: A1 changes on both days (plus an exact duplicate row), A3 changes
  run 4  late rows: A3 dated day 2 with different values (out of order), A2 dated day 1 identical (replay)
  run 5  nothing new
"""

from datetime import date

import pytest

pytestmark = pytest.mark.snowflake

SQL_FILES = ["03_raw_tables.sql", "04_streams.sql", "05_dim_account_scd2.sql"]

D1, D2, D3, D4 = (date(2026, 9, d) for d in (1, 2, 3, 4))
OPEN = date(9999, 12, 31)
BASE = {
    "account_name": "Test Holder", "account_type": "INDIVIDUAL", "address_line": "1 Main St",
    "city": "Boston", "state": "MA", "postal_code": "02101", "risk_tier": "LOW", "status": "ACTIVE",
    "open_date": "2020-01-01",
}
COLS = ["account_id", *BASE, "file_date", "_file_name", "_file_row_number"]


def _load(cur, file_date: date, rows: list[dict]) -> None:
    file_name = f"accounts/accounts_{file_date:%Y%m%d}.csv.gz"
    values = [
        tuple({**BASE, **r, "file_date": file_date.isoformat(), "_file_name": file_name,
               "_file_row_number": i}.get(c) for c in COLS)
        for i, r in enumerate(rows, start=1)
    ]
    cur.executemany(f"INSERT INTO RAW.ACCOUNTS ({', '.join(COLS)}) VALUES ({', '.join(['%s'] * len(COLS))})", values)


def _run(cur) -> dict:
    import json

    return json.loads(cur.execute("CALL CORE.SP_LOAD_DIM_ACCOUNT()").fetchone()[0])


def _dim(cur) -> dict[str, list[dict]]:
    cur.execute("""SELECT account_id, account_sk, effective_from, effective_to, is_current,
                          address_line, city, risk_tier, status
                     FROM CORE.DIM_ACCOUNT ORDER BY account_id, effective_from""")
    cols = [d[0].lower() for d in cur.description]
    history: dict[str, list[dict]] = {}
    for row in cur.fetchall():
        rec = dict(zip(cols, row))
        history.setdefault(rec["account_id"], []).append(rec)
    return history


@pytest.fixture(scope="module")
def scenario(sf_test_db):
    cur = sf_test_db.cursor()
    runs, dims = [], []

    _load(cur, D1, [{"account_id": "A1"}, {"account_id": "A2"}, {"account_id": "A3"}])
    runs.append(_run(cur)); dims.append(_dim(cur))

    _load(cur, D2, [
        {"account_id": "A1", "address_line": "99 New Rd"},
        {"account_id": "A2"},
        {"account_id": "A4"},
        {"account_id": ""},
        {"account_id": "A5", "status": "FROZEN"},
    ])
    runs.append(_run(cur)); dims.append(_dim(cur))

    _load(cur, D3, [{"account_id": "A1", "address_line": "99 New Rd", "risk_tier": "HIGH"}])
    _load(cur, D4, [
        {"account_id": "A1", "address_line": "99 New Rd", "risk_tier": "HIGH", "status": "SUSPENDED"},
        {"account_id": "A1", "address_line": "99 New Rd", "risk_tier": "HIGH", "status": "SUSPENDED"},
        {"account_id": "A3", "city": "Chicago"},
    ])
    runs.append(_run(cur)); dims.append(_dim(cur))

    _load(cur, D2, [{"account_id": "A3", "city": "Denver"}])
    _load(cur, D1, [{"account_id": "A2"}])
    runs.append(_run(cur)); dims.append(_dim(cur))

    runs.append(_run(cur)); dims.append(_dim(cur))
    return {"cur": cur, "runs": runs, "dims": dims}


def _counts(run: dict) -> tuple:
    return (run["rows_read"], run["rows_rejected"], run["rows_unchanged"],
            run["versions_closed"], run["versions_inserted"])


# (rows_read, rows_rejected, rows_unchanged, versions_closed, versions_inserted)
def test_run1_new_accounts(scenario):
    assert _counts(scenario["runs"][0]) == (3, 0, 0, 0, 3)
    dim = scenario["dims"][0]
    for acct in ("A1", "A2", "A3"):
        (v,) = dim[acct]
        assert (v["effective_from"], v["effective_to"], v["is_current"]) == (D1, OPEN, True)


def test_run2_changed_unchanged_new_and_rejected(scenario):
    assert _counts(scenario["runs"][1]) == (5, 2, 1, 1, 2)
    dim = scenario["dims"][1]
    old, new = dim["A1"]
    assert (old["effective_to"], old["is_current"], old["address_line"]) == (D1, False, "1 Main St")
    assert (new["effective_from"], new["effective_to"], new["is_current"], new["address_line"]) == (D2, OPEN, True, "99 New Rd")
    assert len(dim["A2"]) == 1 and dim["A2"][0]["effective_from"] == D1  # unchanged: no new version
    assert dim["A4"][0]["effective_from"] == D2
    assert "A5" not in dim


def test_run3_catch_up_batch_chains_versions(scenario):
    assert _counts(scenario["runs"][2]) == (4, 0, 0, 2, 3)
    a1 = scenario["dims"][2]["A1"]
    assert [(v["effective_from"], v["effective_to"], v["is_current"]) for v in a1] == [
        (D1, D1, False), (D2, D2, False), (D3, D3, False), (D4, OPEN, True)]
    assert [(v["risk_tier"], v["status"]) for v in a1][-2:] == [("HIGH", "ACTIVE"), ("HIGH", "SUSPENDED")]
    a3 = scenario["dims"][2]["A3"]
    assert [(v["effective_from"], v["effective_to"], v["city"]) for v in a3] == [(D1, D3, "Boston"), (D4, OPEN, "Chicago")]


def test_run4_out_of_order_rejected_and_replay_ignored(scenario):
    assert _counts(scenario["runs"][3]) == (2, 1, 1, 0, 0)
    assert scenario["dims"][3] == scenario["dims"][2]


def test_run5_empty_stream_is_a_no_op(scenario):
    assert _counts(scenario["runs"][4]) == (0, 0, 0, 0, 0)
    assert scenario["dims"][4] == scenario["dims"][3]
    assert scenario["cur"].execute("SELECT SYSTEM$STREAM_HAS_DATA('RAW.STRM_ACCOUNTS')").fetchone()[0] is False


def test_invariants_one_current_row_and_contiguous_ranges(scenario):
    dim = scenario["dims"][-1]
    sks = [v["account_sk"] for versions in dim.values() for v in versions]
    assert len(sks) == len(set(sks))
    for acct, versions in dim.items():
        assert sum(v["is_current"] for v in versions) == 1, acct
        assert versions[-1]["is_current"] and versions[-1]["effective_to"] == OPEN
        for prev, nxt in zip(versions, versions[1:]):
            assert (nxt["effective_from"] - prev["effective_to"]).days == 1, acct


def test_rejected_rows_and_load_log(scenario):
    cur = scenario["cur"]
    rejected = cur.execute("""SELECT business_key, reject_reason FROM AUDIT.REJECTED_ROWS
                               ORDER BY reject_reason""").fetchall()
    assert rejected == [("A5", "INVALID_STATUS"), (None, "MISSING_ACCOUNT_ID"), ("A3", "OUT_OF_ORDER")]
    statuses = cur.execute("SELECT status, COUNT(*) FROM AUDIT.LOAD_LOG GROUP BY status").fetchall()
    assert statuses == [("SUCCESS", 5)]
