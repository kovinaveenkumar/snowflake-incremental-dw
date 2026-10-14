"""Helpers for Snowflake-backed tests: insert fixture rows into RAW tables and call the load procedures."""

import json
from datetime import date, timedelta
from decimal import Decimal

ACCOUNT = {"account_name": "Test Holder", "account_type": "INDIVIDUAL", "address_line": "1 Main St", "city": "Boston",
           "state": "MA", "postal_code": "02101", "risk_tier": "LOW", "status": "ACTIVE", "open_date": "2020-01-01"}
SECURITY = {"symbol": "TST", "security_name": "Test Corp", "asset_class": "EQUITY", "currency": "USD"}
LOAD_ORDER = ("SP_LOAD_DIM_SECURITY", "SP_LOAD_DIM_ACCOUNT", "SP_LOAD_FACT_TRADE", "SP_LOAD_FACT_DAILY_POSITION")


def insert(cur, table: str, file_name: str, rows: list[dict]) -> None:
    cols = list(rows[0]) + ["_file_name", "_file_row_number"]
    values = [tuple(list(r.values()) + [file_name, i]) for i, r in enumerate(rows, start=1)]
    cur.executemany(f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))})", values)


def accounts(cur, d: date, rows: list[dict]) -> None:
    insert(cur, "RAW.ACCOUNTS", f"accounts/accounts_{d:%Y%m%d}.csv.gz",
           [{**ACCOUNT, **r, "file_date": d.isoformat()} for r in rows])


def securities(cur, rows: list[dict]) -> None:
    insert(cur, "RAW.SECURITIES", "reference/securities.csv.gz", [{**SECURITY, **r} for r in rows])


def trade(trade_id, account_id, security_id, d: date, side="BUY", quantity=10, price="100.00", notional=None,
          ts="10:00:00") -> dict:
    notional = notional if notional is not None else f"{Decimal(price) * quantity:.2f}"
    return {"trade_id": trade_id, "trade_date": d.isoformat(), "trade_ts": f"{d} {ts}",
            "settlement_date": (d + timedelta(days=1)).isoformat(), "account_id": account_id,
            "security_id": security_id, "side": side, "quantity": str(quantity), "price": price,
            "notional": notional}


def trades(cur, file_date: date, rows: list[dict]) -> None:
    insert(cur, "RAW.TRADES", f"trades/trades_{file_date:%Y%m%d}.csv.gz",
           [{**r, "file_date": file_date.isoformat()} for r in rows])


def run_procs(cur, names=LOAD_ORDER) -> dict:
    """Call the load procedures in Task-tree order; returns each procedure's result object."""
    out = {}
    for name in names:
        out[name] = json.loads(cur.execute(f"CALL CORE.{name}()").fetchone()[0])
    return out


def controls(cur, d: date, trade_rows: list[dict] | None = None, account_rows: int | None = None,
             notional_adjust: Decimal = Decimal("0")) -> None:
    """Sender control file for day d, computed from the trade rows (optionally wrong by notional_adjust)."""
    rows = []
    if account_rows is not None:
        rows.append({"file_name": f"accounts_{d:%Y%m%d}.csv", "file_date": d.isoformat(), "row_count": str(account_rows),
                     "total_quantity": None, "total_notional": None})
    if trade_rows is not None:
        notional = sum(Decimal(t["notional"]) for t in trade_rows) + notional_adjust
        rows.append({"file_name": f"trades_{d:%Y%m%d}.csv", "file_date": d.isoformat(), "row_count": str(len(trade_rows)),
                     "total_quantity": str(sum(int(t["quantity"]) for t in trade_rows)), "total_notional": f"{notional:.2f}"})
    insert(cur, "RAW.CONTROL_TOTALS", f"control/control_{d:%Y%m%d}.csv.gz", rows)


def run_dq(cur) -> dict:
    """Call the DQ procedure; raises snowflake.connector.errors.ProgrammingError when a check FAILS."""
    return json.loads(cur.execute("CALL AUDIT.SP_RUN_DQ_CHECKS()").fetchone()[0])
