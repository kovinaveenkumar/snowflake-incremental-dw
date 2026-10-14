"""Generate synthetic daily broker files: accounts and trades.

All data is synthetic. Output is deterministic for a given seed.

Files written to the output folder:
  securities.csv            security master (reference data, written once)
  accounts_YYYYMMDD.csv     day 1: full snapshot; later days: changed, new and a few
                            re-sent unchanged accounts (so SCD2 sees new/changed/unchanged)
  trades_YYYYMMDD.csv       one business day of trades, including injected bad rows
  control_YYYYMMDD.csv      sender control totals for the day's files, as sent (bad rows included)
  manifest.json             run parameters and the injected bad rows per day

Usage:
  python generator/generate_files.py --out data/raw --days 30 --seed 42
"""

from __future__ import annotations

import argparse
import csv
import json
import random
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

ACCOUNT_FIELDS = [
    "account_id", "account_name", "account_type", "address_line", "city", "state",
    "postal_code", "risk_tier", "status", "open_date", "file_date",
]
TRADE_FIELDS = [
    "trade_id", "trade_date", "trade_ts", "settlement_date", "account_id", "security_id",
    "side", "quantity", "price", "notional", "file_date",
]
SECURITY_FIELDS = ["security_id", "symbol", "security_name", "asset_class", "currency"]
CONTROL_FIELDS = ["file_name", "file_date", "row_count", "total_quantity", "total_notional"]

BAD_ROW_TYPES = ["MISSING_ACCOUNT_ID", "NEGATIVE_QUANTITY", "UNKNOWN_SECURITY", "DUPLICATE_TRADE_ID"]

ACCOUNT_TYPES = ["INDIVIDUAL", "JOINT", "IRA", "INSTITUTIONAL"]
RISK_TIERS = ["LOW", "MEDIUM", "HIGH"]
FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda", "David",
    "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica", "Thomas",
    "Sarah", "Priya", "Wei", "Carlos", "Aisha", "Omar", "Mei", "Raj", "Sofia", "Kenji", "Ana",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez",
    "Martinez", "Hernandez", "Lopez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore",
    "Jackson", "Martin", "Lee", "Patel", "Chen", "Nguyen", "Kim", "Shah", "Singh", "Cohen",
]
STREETS = [
    "Main St", "Oak Ave", "Maple Dr", "Cedar Ln", "Park Ave", "Elm St", "Washington Blvd",
    "Lake Rd", "Hill St", "River Rd", "Broadway", "Market St", "Pine St", "Sunset Blvd",
]
CITIES = [
    ("New York", "NY"), ("Jersey City", "NJ"), ("Boston", "MA"), ("Chicago", "IL"),
    ("Tampa", "FL"), ("Dallas", "TX"), ("Charlotte", "NC"), ("Denver", "CO"),
    ("Seattle", "WA"), ("San Francisco", "CA"), ("Atlanta", "GA"), ("Philadelphia", "PA"),
]
ASSET_CLASSES = ["EQUITY"] * 7 + ["ETF"] * 2 + ["BOND"]

CENT = Decimal("0.01")
MARKET_OPEN = time(9, 30)
MARKET_SECONDS = 6 * 3600 + 30 * 60  # 09:30 to 16:00


@dataclass
class GeneratorConfig:
    seed: int = 42
    n_accounts: int = 500
    n_days: int = 30
    start_date: date = date(2026, 9, 1)
    n_securities: int = 100
    trades_per_day: tuple[int, int] = (1900, 2100)
    changes_per_day: tuple[int, int] = (5, 12)
    new_accounts_per_day: tuple[int, int] = (0, 3)
    resent_unchanged_per_day: int = 3
    bad_rows_per_type: tuple[int, int] = (1, 2)


@dataclass
class DayBatch:
    file_date: date
    accounts: list[dict]
    trades: list[dict]
    injected: list[dict] = field(default_factory=list)


@dataclass
class GeneratedData:
    config: GeneratorConfig
    securities: list[dict]
    days: list[DayBatch]


def business_days(start: date, n: int) -> list[date]:
    """n weekdays starting at start (no holiday calendar)."""
    days, d = [], start
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days


def next_business_day(d: date) -> date:
    d += timedelta(days=1)
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d


def _money(x: float | Decimal) -> Decimal:
    return Decimal(str(x)).quantize(CENT, rounding=ROUND_HALF_UP)


def _make_securities(rng: random.Random, n: int) -> tuple[list[dict], dict[str, float]]:
    securities, prices, symbols = [], {}, []
    for i in range(1, n + 1):
        symbol = "".join(rng.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ") for _ in range(rng.randint(3, 4)))
        while symbol in symbols:
            symbol += rng.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
        symbols.append(symbol)
        asset_class = rng.choice(ASSET_CLASSES)
        security_id = f"SEC{i:05d}"
        securities.append({
            "security_id": security_id,
            "symbol": symbol,
            "security_name": f"{symbol} {'Corp' if asset_class == 'EQUITY' else asset_class.title() + ' Fund' if asset_class == 'ETF' else 'Note'}",
            "asset_class": asset_class,
            "currency": "USD",
        })
        prices[security_id] = rng.uniform(90, 110) if asset_class == "BOND" else rng.uniform(10, 500)
    return securities, prices


def _random_address(rng: random.Random) -> dict:
    city, state = rng.choice(CITIES)
    return {
        "address_line": f"{rng.randint(1, 9999)} {rng.choice(STREETS)}",
        "city": city,
        "state": state,
        "postal_code": f"{rng.randint(1000, 99999):05d}",
    }


def _new_account(rng: random.Random, number: int, open_date: date) -> dict:
    account_type = rng.choice(ACCOUNT_TYPES)
    last = rng.choice(LAST_NAMES)
    if account_type == "INSTITUTIONAL":
        name = f"{last} Capital LLC"
    elif account_type == "JOINT":
        name = f"{rng.choice(FIRST_NAMES)} & {rng.choice(FIRST_NAMES)} {last}"
    else:
        name = f"{rng.choice(FIRST_NAMES)} {last}"
    return {
        "account_id": f"A{number:06d}",
        "account_name": name,
        "account_type": account_type,
        **_random_address(rng),
        "risk_tier": rng.choices(RISK_TIERS, weights=[5, 4, 1])[0],
        "status": "ACTIVE",
        "open_date": open_date.isoformat(),
    }


def _apply_changes(rng: random.Random, accounts: dict[str, dict], n_range: tuple[int, int]) -> list[str]:
    """Change address, risk tier or status on a few open accounts. Returns changed ids."""
    candidates = [a for a in accounts.values() if a["status"] != "CLOSED"]
    n = min(rng.randint(*n_range), len(candidates))
    changed = []
    for acct in rng.sample(candidates, n):
        kind = rng.choices(["address", "risk_tier", "status"], weights=[5, 3, 2])[0]
        if kind == "address":
            acct.update(_random_address(rng))
        elif kind == "risk_tier":
            acct["risk_tier"] = rng.choice([t for t in RISK_TIERS if t != acct["risk_tier"]])
        elif acct["status"] == "SUSPENDED":
            acct["status"] = "ACTIVE"
        else:
            acct["status"] = rng.choices(["SUSPENDED", "CLOSED"], weights=[3, 1])[0]
        changed.append(acct["account_id"])
    return changed


def _trade(rng: random.Random, d: date, seq: int, account_id: str, security_id: str,
           prices: dict[str, float]) -> dict:
    quantity = rng.randint(1, 1000)
    price = _money(prices[security_id] * rng.uniform(0.995, 1.005))
    ts = datetime.combine(d, MARKET_OPEN) + timedelta(seconds=rng.randint(0, MARKET_SECONDS))
    return {
        "trade_id": f"T{d:%Y%m%d}{seq:06d}",
        "trade_date": d.isoformat(),
        "trade_ts": ts.isoformat(sep=" "),
        "settlement_date": next_business_day(d).isoformat(),
        "account_id": account_id,
        "security_id": security_id,
        "side": rng.choice(["BUY", "SELL"]),
        "quantity": quantity,
        "price": price,
        "notional": (price * quantity).quantize(CENT),
        "file_date": d.isoformat(),
    }


def _make_trades(rng: random.Random, d: date, accounts: dict[str, dict], security_ids: list[str],
                 prices: dict[str, float], config: GeneratorConfig) -> tuple[list[dict], list[dict]]:
    active = [a["account_id"] for a in accounts.values() if a["status"] == "ACTIVE"]
    n_good = rng.randint(*config.trades_per_day)
    rows = [_trade(rng, d, seq, rng.choice(active), rng.choice(security_ids), prices)
            for seq in range(1, n_good + 1)]

    injected, seq = [], n_good
    for kind in BAD_ROW_TYPES:
        count = rng.randint(*config.bad_rows_per_type)
        if kind == "DUPLICATE_TRADE_ID":
            # The sender re-sends an already-sent trade unchanged.
            bad_rows = [dict(r) for r in rng.sample(rows[:n_good], count)]
        else:
            bad_rows = []
            for _ in range(count):
                seq += 1
                row = _trade(rng, d, seq, rng.choice(active), rng.choice(security_ids), prices)
                if kind == "MISSING_ACCOUNT_ID":
                    row["account_id"] = ""
                elif kind == "NEGATIVE_QUANTITY":
                    row["quantity"] = -row["quantity"]
                    row["notional"] = (row["price"] * row["quantity"]).quantize(CENT)
                else:  # UNKNOWN_SECURITY
                    row["security_id"] = f"UNK{rng.randint(10000, 99999)}"
                bad_rows.append(row)
        rows.extend(bad_rows)
        injected.extend({"type": kind, "trade_id": r["trade_id"]} for r in bad_rows)

    rng.shuffle(rows)
    return rows, injected


def generate(config: GeneratorConfig | None = None) -> GeneratedData:
    config = config or GeneratorConfig()
    rng = random.Random(config.seed)

    securities, prices = _make_securities(rng, config.n_securities)
    security_ids = [s["security_id"] for s in securities]
    accounts: dict[str, dict] = {}
    batches = []

    for i, d in enumerate(business_days(config.start_date, config.n_days)):
        if i == 0:
            for _ in range(config.n_accounts):
                opened = d - timedelta(days=rng.randint(30, 3650))
                acct = _new_account(rng, len(accounts) + 1, opened)
                accounts[acct["account_id"]] = acct
            sent_ids = list(accounts)
        else:
            changed = _apply_changes(rng, accounts, config.changes_per_day)
            new_ids = []
            for _ in range(rng.randint(*config.new_accounts_per_day)):
                acct = _new_account(rng, len(accounts) + 1, d)
                accounts[acct["account_id"]] = acct
                new_ids.append(acct["account_id"])
            untouched = [a for a in accounts if a not in changed and a not in new_ids]
            resent = rng.sample(untouched, min(config.resent_unchanged_per_day, len(untouched)))
            sent_ids = changed + new_ids + resent

        for sid in security_ids:  # daily random walk, +/- 2%
            prices[sid] *= 1 + rng.uniform(-0.02, 0.02)

        account_rows = [{**accounts[a], "file_date": d.isoformat()} for a in sent_ids]
        trades, injected = _make_trades(rng, d, accounts, security_ids, prices, config)
        batches.append(DayBatch(d, account_rows, trades, injected))

    return GeneratedData(config, securities, batches)


def control_totals(trades: list[dict]) -> dict:
    return {
        "row_count": len(trades),
        "total_quantity": sum(int(t["quantity"]) for t in trades),
        "total_notional": sum((Decimal(str(t["notional"])) for t in trades), Decimal("0.00")),
    }


def _write_csv(path: Path, fields: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_files(data: GeneratedData, out_dir: Path) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(out_dir / "securities.csv", SECURITY_FIELDS, data.securities)

    manifest_days = []
    for batch in data.days:
        stamp = f"{batch.file_date:%Y%m%d}"
        accounts_file, trades_file = f"accounts_{stamp}.csv", f"trades_{stamp}.csv"
        _write_csv(out_dir / accounts_file, ACCOUNT_FIELDS, batch.accounts)
        _write_csv(out_dir / trades_file, TRADE_FIELDS, batch.trades)

        totals = control_totals(batch.trades)
        _write_csv(out_dir / f"control_{stamp}.csv", CONTROL_FIELDS, [
            {"file_name": accounts_file, "file_date": batch.file_date.isoformat(),
             "row_count": len(batch.accounts), "total_quantity": "", "total_notional": ""},
            {"file_name": trades_file, "file_date": batch.file_date.isoformat(), **totals},
        ])
        manifest_days.append({
            "file_date": batch.file_date.isoformat(),
            "accounts_file": accounts_file,
            "accounts_rows": len(batch.accounts),
            "trades_file": trades_file,
            "trades_rows": len(batch.trades),
            "injected_bad_rows": batch.injected,
        })

    config = json.loads(json.dumps(asdict(data.config), default=str))  # dates -> ISO, tuples -> lists
    manifest = {
        "synthetic_data": True,
        "config": config,
        "securities": len(data.securities),
        "days": manifest_days,
        "totals": {
            "account_rows": sum(d["accounts_rows"] for d in manifest_days),
            "distinct_accounts": len({a["account_id"] for b in data.days for a in b.accounts}),
            "trade_rows": sum(d["trades_rows"] for d in manifest_days),
            "injected_bad_rows": sum(len(d["injected_bad_rows"]) for d in manifest_days),
        },
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", type=Path, default=Path("data/raw"))
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--accounts", type=int, default=500)
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--start", type=date.fromisoformat, default=date(2026, 9, 1))
    args = parser.parse_args()

    config = GeneratorConfig(seed=args.seed, n_accounts=args.accounts, n_days=args.days, start_date=args.start)
    manifest = write_files(generate(config), args.out)
    days = manifest["days"]
    print(f"Wrote {len(days)} days ({days[0]['file_date']} to {days[-1]['file_date']}) to {args.out}")
    print(json.dumps(manifest["totals"], indent=2))


if __name__ == "__main__":
    main()
