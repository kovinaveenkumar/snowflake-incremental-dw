"""Step 13: UAT checklist. Business-style acceptance checks run against BROKER_DW (read-only), plus the
Oracle comparison, written to docs/uat_checklist.md with the real values and PASS/FAIL.

Usage: python -m scripts.uat
"""

from __future__ import annotations

import csv
import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from common.connections import ROOT, env, snowflake_connect

DATA = ROOT / "data" / "raw"
TRACKED = ["account_name", "account_type", "address_line", "city", "state", "postal_code", "risk_tier", "status",
           "open_date"]


def expected_from_files() -> dict:
    manifest = json.loads((DATA / "manifest.json").read_text())
    current, versions, closed = {}, 0, 0
    for day in manifest["days"]:
        for r in csv.DictReader(open(DATA / day["accounts_file"])):
            key = tuple(r[k] for k in TRACKED)
            if r["account_id"] not in current or current[r["account_id"]] != key:
                versions += 1
                closed += r["account_id"] in current
            current[r["account_id"]] = key
    injected = sorted((i["trade_id"], i["type"]) for d in manifest["days"] for i in d["injected_bad_rows"])
    return {"days": len(manifest["days"]), "trades": manifest["totals"]["trade_rows"], "injected": injected,
            "versions": versions, "current": len(current), "closed": closed}


def oracle_matches(snow: dict) -> tuple[bool, str]:
    try:
        import oracledb
        conn = oracledb.connect(user=env("ORACLE_USER"), password=env("ORACLE_PASSWORD"), dsn=env("ORACLE_DSN"))
    except Exception as e:  # container not running: report, do not guess
        return False, f"Oracle not reachable ({type(e).__name__}); run python -m oracle.run_oracle"
    cur = conn.cursor()
    ora = cur.execute("""SELECT (SELECT COUNT(*) FROM dim_account), (SELECT COUNT(*) FROM dim_account WHERE is_current),
                                (SELECT COUNT(*) FROM fact_trade), (SELECT SUM(notional) FROM fact_trade),
                                (SELECT COUNT(*) FROM rejected_rows WHERE source_table = 'STG_TRADES') FROM dual""").fetchone()
    conn.close()
    norm = lambda t: tuple(Decimal(str(x)) for x in t)   # Oracle returns floats, Snowflake Decimals
    ok = norm(ora) == norm(snow["oracle_tuple"])
    show = lambda t: " / ".join(f"{x:,}" for x in norm(t))
    return ok, f"versions / current / trades / notional / rejected: Oracle {show(ora)}; Snowflake {show(snow['oracle_tuple'])}"


def main() -> None:
    exp = expected_from_files()
    checks: list[tuple[str, str, bool, str]] = []
    with snowflake_connect(database=env("SNOWFLAKE_DATABASE")) as conn:
        cur = conn.cursor()
        q = lambda s, p=None: cur.execute(s, p).fetchone()

        reg = dict(cur.execute("SELECT status, COUNT(*) FROM AUDIT.CONTROL_REGISTER GROUP BY 1").fetchall())
        checks.append(("Delivery", "Every file the broker sent is reconciled against its control file",
                       reg == {"PASS": exp["days"] * 2}, f"control register {reg}, expected {exp['days'] * 2} PASS"))

        rows = q("""SELECT SUM(ctl_rows), SUM(raw_rows), SUM(fact_rows) + SUM(rejected_rows), SUM(ctl_notional),
                           SUM(fact_notional) + SUM(rejected_notional) FROM AUDIT.V_TRADE_FILE_RECON""")
        checks.append(("Completeness", "Trades sent = trades landed = trades loaded + rejected (rows and $)",
                       rows[0] == rows[1] == rows[2] == exp["trades"] and rows[3] == rows[4],
                       f"rows {rows[0]:,} / {rows[1]:,} / {rows[2]:,}; notional {rows[3]:,} = {rows[4]:,}"))

        rejected = sorted(cur.execute("""SELECT business_key, reject_reason FROM AUDIT.REJECTED_ROWS
                                          WHERE source_table = 'RAW.TRADES'""").fetchall())
        checks.append(("Data quality", "Exactly the planted bad trades are rejected, each with its reason",
                       rejected == exp["injected"], f"{len(rejected)} rejected, {len(exp['injected'])} planted"))

        no_reason = q("SELECT COUNT_IF(reject_reason IS NULL OR record IS NULL) FROM AUDIT.REJECTED_ROWS")[0]
        checks.append(("Data quality", "Every rejected row keeps its reason and original record for follow-up",
                       no_reason == 0, f"{no_reason} rejected rows missing reason or record"))

        v, c, cl = q("SELECT COUNT(*), COUNT_IF(is_current), COUNT_IF(NOT is_current) FROM CORE.DIM_ACCOUNT")
        checks.append(("Account history", "Every account change is kept as history (SCD Type 2)",
                       (v, c, cl) == (exp["versions"], exp["current"], exp["closed"]),
                       f"versions/current/closed {v}/{c}/{cl}, expected {exp['versions']}/{exp['current']}/{exp['closed']}"))

        bad_current = q("""SELECT COUNT(*) FROM (SELECT account_id FROM CORE.DIM_ACCOUNT GROUP BY 1
                            HAVING COUNT_IF(is_current) <> 1)""")[0]
        checks.append(("Account history", "Each account has exactly one current record", bad_current == 0,
                       f"{bad_current} accounts without exactly one current record"))

        wrong_version = q("""SELECT COUNT(*) FROM CORE.FACT_TRADE f JOIN CORE.DIM_ACCOUNT a ON a.account_sk = f.account_sk
                              WHERE a.status <> 'ACTIVE' OR f.trade_date NOT BETWEEN a.effective_from AND a.effective_to""")[0]
        checks.append(("Reporting", "Each trade reports the account as it was on the trade date", wrong_version == 0,
                       f"{wrong_version} trades linked to the wrong account version"))

        days, last = q("SELECT COUNT(DISTINCT position_date), MAX(position_date) FROM CORE.FACT_DAILY_POSITION")
        checks.append(("Reporting", "A position snapshot exists for every business day", days == exp["days"],
                       f"{days} snapshot days (last {last}), {exp['days']} business days"))

        last_dq = q("""SELECT message, started_at FROM AUDIT.LOAD_LOG WHERE procedure_name = 'AUDIT.SP_RUN_DQ_CHECKS'
                       ORDER BY started_at DESC LIMIT 1""")
        checks.append(("Controls", "Latest automated DQ run has no failures", " 0 fail," in last_dq[0],
                       f"{last_dq[0]} ({last_dq[1]:%Y-%m-%d %H:%M})"))

        snowpark = q("""SELECT status, details FROM AUDIT.DQ_RESULTS WHERE check_name = 'SNOWPARK_PRICE_OUTLIERS'
                        ORDER BY checked_at DESC LIMIT 1""")
        checks.append(("Controls", "No suspicious trade prices (Snowpark price-outlier check)",
                       snowpark is not None and snowpark[0] == "PASS", snowpark[1] if snowpark else "not run"))

        graph = cur.execute("""SELECT state, COUNT(*) FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
                                 SCHEDULED_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP()), RESULT_LIMIT => 10000))
                               WHERE database_name = CURRENT_DATABASE() GROUP BY 1""").fetchall()
        states = dict(graph)
        checks.append(("Operations", "Scheduled loads run without manual steps and without failures",
                       states.get("SUCCEEDED", 0) > 0 and states.get("FAILED", 0) == 0, f"task runs by state {states}"))

        cur.execute("SHOW RESOURCE MONITORS LIKE 'DW_MONITOR'")
        cols = [d[0] for d in cur.description]
        mon = dict(zip(cols, cur.fetchone()))
        checks.append(("Operations", "Compute cost stays inside the agreed cap",
                       float(mon["used_credits"]) < float(mon["credit_quota"]),
                       f"{mon['used_credits']} of {mon['credit_quota']} credits used"))

        oracle_tuple = (v, c, q("SELECT COUNT(*) FROM CORE.FACT_TRADE")[0], q("SELECT SUM(notional) FROM CORE.FACT_TRADE")[0],
                        len(rejected))
    ok, detail = oracle_matches({"oracle_tuple": oracle_tuple})
    checks.append(("Migration", "Oracle PL/SQL load produces the same results as Snowflake", ok, detail))

    passed = sum(c[2] for c in checks)
    lines = [
        "# UAT checklist",
        "",
        f"Generated by `python -m scripts.uat` on {datetime.now():%Y-%m-%d %H:%M} against BROKER_DW (read-only).",
        "Every value below comes from that run. All data is synthetic.",
        "",
        f"**Result: {passed} of {len(checks)} checks PASS.**",
        "",
        "| # | Area | Acceptance check | Result | Evidence |",
        "|---|---|---|---|---|",
    ]
    lines += [f"| {i} | {area} | {text} | {'PASS' if ok else 'FAIL'} | {detail} |"
              for i, (area, text, ok, detail) in enumerate(checks, start=1)]
    (ROOT / "docs" / "uat_checklist.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
