"""Step 12: build the Oracle schema, load the same generated files, run pkg_load.run_day for every day,
prove the error path, and compare the results with Snowflake.

Usage: python -m oracle.run_oracle
"""

from __future__ import annotations

import csv
import json
import time
from datetime import date
from pathlib import Path

import oracledb

from common.connections import ROOT, env, snowflake_connect

ORACLE_DIR = Path(__file__).resolve().parent
DATA = ROOT / "data" / "raw"


def statements(sql: str) -> list[str]:
    """Split a script: PL/SQL blocks end with a line containing only '/', plain SQL with ';'."""
    out, buf, plsql = [], [], False
    for line in sql.splitlines():
        stripped = line.strip()
        if not buf and (not stripped or stripped.startswith("--")):
            continue
        if not buf:
            plsql = stripped.upper().startswith(("BEGIN", "DECLARE", "CREATE OR REPLACE PACKAGE"))
        if plsql and stripped == "/":
            out.append("\n".join(buf)); buf = []
            continue
        buf.append(line)
        if not plsql and stripped.endswith(";"):
            out.append("\n".join(buf).rstrip().rstrip(";")); buf = []
    return out


def run_script(cur, path: Path) -> None:
    for stmt in statements(path.read_text()):
        cur.execute(stmt)
    errors = cur.execute("SELECT name, line, text FROM user_errors ORDER BY name, sequence").fetchall()
    if errors:
        raise RuntimeError(f"PL/SQL compile errors: {errors}")


def load_csv(cur, table: str, path: Path, extra: dict | None = None) -> int:
    with path.open(newline="") as f:
        rows = list(csv.DictReader(f))
    cols = list(rows[0]) + list(extra or {})
    data = [[r[c] if r.get(c, "") != "" else None for c in rows[0]] + [v(i) for v in (extra or {}).values()]
            for i, r in enumerate(rows, start=1)]
    cur.executemany(f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join(f':{i + 1}' for i in range(len(cols)))})", data)
    return len(rows)


def oracle_counts(cur) -> dict:
    q = lambda s: cur.execute(s).fetchone()
    versions, current, closed = q("SELECT COUNT(*), COUNT(CASE WHEN is_current THEN 1 END), "
                                  "COUNT(CASE WHEN NOT is_current THEN 1 END) FROM dim_account")
    fact, notional = q("SELECT COUNT(*), SUM(notional) FROM fact_trade")
    return {"dim_versions": versions, "dim_current": current, "dim_closed": closed, "fact_rows": fact,
            "fact_notional": float(notional),
            "rejected_by_reason": dict(cur.execute("SELECT reject_reason, COUNT(*) FROM rejected_rows "
                                                   "WHERE source_table = 'STG_TRADES' GROUP BY reject_reason").fetchall()),
            "securities": q("SELECT COUNT(*) FROM dim_security")[0],
            "recon_pass": q("SELECT COUNT(*) FROM recon_results WHERE status = 'PASS'")[0],
            "recon_fail": q("SELECT COUNT(*) FROM recon_results WHERE status = 'FAIL'")[0]}


def snowflake_counts() -> dict:
    with snowflake_connect(database=env("SNOWFLAKE_DATABASE")) as conn:
        cur = conn.cursor()
        q = lambda s: cur.execute(s).fetchone()
        versions, current, closed = q("SELECT COUNT(*), COUNT_IF(is_current), COUNT_IF(NOT is_current) FROM CORE.DIM_ACCOUNT")
        fact, notional = q("SELECT COUNT(*), SUM(notional) FROM CORE.FACT_TRADE")
        return {"dim_versions": versions, "dim_current": current, "dim_closed": closed, "fact_rows": fact,
                "fact_notional": float(notional),
                "rejected_by_reason": dict(cur.execute("SELECT reject_reason, COUNT(*) FROM AUDIT.REJECTED_ROWS "
                                                       "WHERE source_table = 'RAW.TRADES' GROUP BY 1").fetchall()),
                "securities": q("SELECT COUNT(*) FROM CORE.DIM_SECURITY")[0]}


def main() -> None:
    manifest = json.loads((DATA / "manifest.json").read_text())
    conn = oracledb.connect(user=env("ORACLE_USER"), password=env("ORACLE_PASSWORD"), dsn=env("ORACLE_DSN"))
    cur = conn.cursor()
    print("Oracle", conn.version)

    run_script(cur, ORACLE_DIR / "schema.sql")
    run_script(cur, ORACLE_DIR / "pkg_load.sql")

    load_csv(cur, "stg_securities", DATA / "securities.csv")
    for day in manifest["days"]:
        for table, name in (("stg_accounts", day["accounts_file"]), ("stg_trades", day["trades_file"])):
            load_csv(cur, table, DATA / name, {"file_name": lambda i, n=name: n, "file_row": lambda i: i})
        load_csv(cur, "stg_control", DATA / day["accounts_file"].replace("accounts_", "control_"))
    conn.commit()

    started = time.time()
    for day in manifest["days"]:
        cur.callproc("pkg_load.run_day", [date.fromisoformat(day["file_date"])])
    elapsed = time.time() - started
    print(f"pkg_load.run_day x {len(manifest['days'])} days: {elapsed:.1f} s")

    # Error path: a control file with no data must raise ORA-20001, roll back, and still leave a log row.
    cur.execute("INSERT INTO stg_control VALUES ('trades_29991231.csv', '2999-12-31', '1', '1', '1.00')")
    conn.commit()
    try:
        cur.callproc("pkg_load.run_day", [date(2999, 12, 31)])
        print("error path: NOT raised (unexpected)")
    except oracledb.DatabaseError as e:
        logged = cur.execute("SELECT status, message FROM load_log WHERE file_date = DATE '2999-12-31'").fetchall()
        kept = cur.execute("SELECT COUNT(*) FROM recon_results WHERE file_name = 'trades_29991231.csv'").fetchone()[0]
        print(f"error path: raised {str(e).splitlines()[0]} | autonomous log rows: {logged} | recon rows after rollback: {kept}")
    cur.execute("DELETE FROM stg_control WHERE file_name = 'trades_29991231.csv'")
    cur.execute("DELETE FROM load_log WHERE file_date = DATE '2999-12-31'")
    conn.commit()

    ora, snow = oracle_counts(cur), snowflake_counts()
    print(f"\n{'metric':<22} {'oracle':>40} {'snowflake':>40}  match")
    for k in snow:
        print(f"{k:<22} {str(ora[k]):>40} {str(snow[k]):>40}  {'YES' if ora[k] == snow[k] else 'NO'}")
    print(f"oracle reconciliation checks: {ora['recon_pass']} PASS, {ora['recon_fail']} FAIL")
    conn.close()


if __name__ == "__main__":
    main()
