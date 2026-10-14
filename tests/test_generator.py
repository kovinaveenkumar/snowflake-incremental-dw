"""Unit tests for generator/generate_files.py."""

import csv
import json
from collections import Counter
from datetime import date
from decimal import Decimal

import pytest

from generator.generate_files import (
    BAD_ROW_TYPES,
    GeneratorConfig,
    business_days,
    control_totals,
    generate,
    next_business_day,
    write_files,
)

SMALL = GeneratorConfig(seed=7, n_accounts=50, n_days=5, n_securities=20, trades_per_day=(100, 120))


@pytest.fixture(scope="module")
def full():
    """The default run: 500 accounts, 30 business days."""
    return generate(GeneratorConfig())


@pytest.fixture(scope="module")
def small():
    return generate(SMALL)


def _read_csv(path):
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


# --- calendar helpers ---------------------------------------------------------

def test_business_days_skip_weekends():
    days = business_days(date(2026, 9, 4), 3)  # Friday
    assert days == [date(2026, 9, 4), date(2026, 9, 7), date(2026, 9, 8)]


def test_next_business_day_is_t_plus_1():
    assert next_business_day(date(2026, 9, 3)) == date(2026, 9, 4)  # Thu -> Fri
    assert next_business_day(date(2026, 9, 4)) == date(2026, 9, 7)  # Fri -> Mon


# --- determinism --------------------------------------------------------------

def test_same_seed_gives_identical_files(tmp_path):
    write_files(generate(SMALL), tmp_path / "a")
    write_files(generate(SMALL), tmp_path / "b")
    names = sorted(p.name for p in (tmp_path / "a").iterdir())
    assert names == sorted(p.name for p in (tmp_path / "b").iterdir())
    for name in names:
        assert (tmp_path / "a" / name).read_bytes() == (tmp_path / "b" / name).read_bytes(), name


def test_different_seed_gives_different_data():
    other = generate(GeneratorConfig(**{**SMALL.__dict__, "seed": 8}))
    assert generate(SMALL).days[0].trades != other.days[0].trades


# --- size and shape of the default run ----------------------------------------

def test_default_run_size(full):
    assert len(full.days) == 30
    assert all(d.file_date.weekday() < 5 for d in full.days)
    assert len(full.days[0].accounts) == 500
    for d in full.days:
        good = len(d.trades) - len(d.injected)
        assert 1900 <= good <= 2100


def test_day_one_is_full_snapshot_with_unique_ids(full):
    ids = [a["account_id"] for a in full.days[0].accounts]
    assert len(ids) == len(set(ids)) == 500
    assert all(a["status"] == "ACTIVE" for a in full.days[0].accounts)


# --- SCD2 history -------------------------------------------------------------

def test_later_days_contain_changed_new_and_unchanged_accounts(small):
    last_seen = {a["account_id"]: a for a in small.days[0].accounts}
    seen_changed = seen_new = seen_unchanged = 0
    tracked = ["account_name", "address_line", "city", "state", "postal_code", "risk_tier", "status"]
    for day in small.days[1:]:
        for a in day.accounts:
            prev = last_seen.get(a["account_id"])
            if prev is None:
                seen_new += 1
                assert a["open_date"] == day.file_date.isoformat()
            elif any(a[k] != prev[k] for k in tracked):
                seen_changed += 1
            else:
                seen_unchanged += 1
            last_seen[a["account_id"]] = a
    # New accounts are random (0-3/day), so they are checked on the full run below.
    assert seen_changed > 0 and seen_unchanged > 0


def test_full_run_has_attribute_history(full):
    versions = Counter()
    for day in full.days:
        for a in day.accounts:
            versions[a["account_id"]] += 1
    assert sum(1 for c in versions.values() if c > 1) > 50
    assert len(versions) > 500  # new accounts opened after day 1


def test_closed_accounts_never_reopen(full):
    status = {}
    for day in full.days:
        for a in day.accounts:
            if status.get(a["account_id"]) == "CLOSED":
                assert a["status"] == "CLOSED", a["account_id"]
            status[a["account_id"]] = a["status"]


# --- good trades --------------------------------------------------------------

def _good_trades(day):
    bad_ids = Counter(i["trade_id"] for i in day.injected if i["type"] != "DUPLICATE_TRADE_ID")
    dup_ids = Counter(i["trade_id"] for i in day.injected if i["type"] == "DUPLICATE_TRADE_ID")
    good, dropped = [], Counter()
    for t in day.trades:
        if t["trade_id"] in bad_ids:
            continue
        if dropped[t["trade_id"]] < dup_ids[t["trade_id"]]:
            dropped[t["trade_id"]] += 1
            continue
        good.append(t)
    return good


def test_good_trades_reference_active_accounts_and_known_securities(full):
    security_ids = {s["security_id"] for s in full.securities}
    status = {}
    for day in full.days:
        status.update({a["account_id"]: a["status"] for a in day.accounts})
        for t in _good_trades(day):
            assert status.get(t["account_id"]) == "ACTIVE", t
            assert t["security_id"] in security_ids
            assert t["quantity"] > 0
            assert t["notional"] == (t["price"] * t["quantity"]).quantize(Decimal("0.01"))
            assert t["trade_date"] == t["file_date"] == day.file_date.isoformat()
            assert t["settlement_date"] == next_business_day(day.file_date).isoformat()
            assert "09:30:00" <= t["trade_ts"][11:] <= "16:00:00"


def test_good_trade_ids_unique_across_all_days(full):
    ids = [t["trade_id"] for day in full.days for t in _good_trades(day)]
    assert len(ids) == len(set(ids))


# --- injected bad rows ----------------------------------------------------------

def test_every_bad_row_type_injected_every_day(full):
    for day in full.days:
        assert {i["type"] for i in day.injected} == set(BAD_ROW_TYPES)


def test_injected_rows_are_actually_bad(full):
    security_ids = {s["security_id"] for s in full.securities}
    for day in full.days:
        by_id = Counter(t["trade_id"] for t in day.trades)
        rows = {t["trade_id"]: t for t in day.trades}
        for inj in day.injected:
            row = rows[inj["trade_id"]]
            if inj["type"] == "MISSING_ACCOUNT_ID":
                assert row["account_id"] == ""
            elif inj["type"] == "NEGATIVE_QUANTITY":
                assert row["quantity"] < 0
            elif inj["type"] == "UNKNOWN_SECURITY":
                assert row["security_id"] not in security_ids
            else:
                assert by_id[inj["trade_id"]] == 2


def test_only_injected_rows_are_bad(full):
    """Each bad row is bad for exactly one reason, so later rejection counts are exact."""
    security_ids = {s["security_id"] for s in full.securities}
    for day in full.days:
        injected = Counter(i["type"] for i in day.injected)
        found = Counter()
        seen = set()
        for t in day.trades:
            reasons = []
            if t["account_id"] == "":
                reasons.append("MISSING_ACCOUNT_ID")
            if t["quantity"] < 0:
                reasons.append("NEGATIVE_QUANTITY")
            if t["security_id"] not in security_ids:
                reasons.append("UNKNOWN_SECURITY")
            if t["trade_id"] in seen:
                reasons.append("DUPLICATE_TRADE_ID")
            seen.add(t["trade_id"])
            assert len(reasons) <= 1, t
            found.update(reasons)
        assert found == injected


# --- written files ------------------------------------------------------------

def test_written_files_and_manifest(small, tmp_path):
    manifest = write_files(small, tmp_path)
    expected = {"securities.csv", "manifest.json"}
    for d in small.days:
        stamp = f"{d.file_date:%Y%m%d}"
        expected |= {f"accounts_{stamp}.csv", f"trades_{stamp}.csv", f"control_{stamp}.csv"}
    assert {p.name for p in tmp_path.iterdir()} == expected

    on_disk = json.loads((tmp_path / "manifest.json").read_text())
    assert on_disk == manifest
    assert on_disk["synthetic_data"] is True
    for d in on_disk["days"]:
        assert len(_read_csv(tmp_path / d["trades_file"])) == d["trades_rows"]
        assert len(_read_csv(tmp_path / d["accounts_file"])) == d["accounts_rows"]


def test_control_file_matches_trade_file(small, tmp_path):
    write_files(small, tmp_path)
    for d in small.days:
        stamp = f"{d.file_date:%Y%m%d}"
        trades = _read_csv(tmp_path / f"trades_{stamp}.csv")
        control = {r["file_name"]: r for r in _read_csv(tmp_path / f"control_{stamp}.csv")}
        ctl = control[f"trades_{stamp}.csv"]
        assert int(ctl["row_count"]) == len(trades)
        assert int(ctl["total_quantity"]) == sum(int(t["quantity"]) for t in trades)
        assert Decimal(ctl["total_notional"]) == sum(Decimal(t["notional"]) for t in trades)
        assert int(control[f"accounts_{stamp}.csv"]["row_count"]) == len(d.accounts)


def test_control_totals_helper():
    rows = [{"quantity": 3, "notional": Decimal("10.50")}, {"quantity": -1, "notional": Decimal("-2.25")}]
    assert control_totals(rows) == {"row_count": 2, "total_quantity": 2, "total_notional": Decimal("8.25")}
