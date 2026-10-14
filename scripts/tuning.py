"""Step 11: measure SQL tuning on the real data (read-only).

  1. Baseline layout: micro-partitions of the fact tables (clustering can only help when there is more
     than one partition to prune).
  2. Query rewrite: the DQ check "every position equals the running sum of its trades".
       before  range join: each snapshot row joins every earlier trade day of its pair, then aggregates
       after   window running sum per pair + ASOF JOIN to the latest trade day on/before the snapshot day
     Both are run 3 times with the result cache off, after a warm-up run; elapsed, execution and compile
     time and bytes scanned come from INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION. Equivalence is checked on the real
     data (both return 0) and on a deliberately wrong comparison (both must flag every row).

Usage: python -m scripts.tuning
"""

from __future__ import annotations

import json
import statistics

from common.connections import env, snowflake_connect

DAILY = """SELECT a.account_id, f.security_sk, f.trade_date, SUM(f.signed_quantity) AS qty
             FROM CORE.FACT_TRADE f JOIN CORE.DIM_ACCOUNT a ON a.account_sk = f.account_sk
            GROUP BY 1, 2, 3"""

BEFORE = f"""SELECT COUNT(*) FROM (
  SELECT p.position_date, p.account_id, p.security_sk, p.position_quantity, COALESCE(SUM(t.qty), 0) AS expected_quantity
    FROM CORE.FACT_DAILY_POSITION p
    LEFT JOIN ({DAILY}) t
      ON t.account_id = p.account_id AND t.security_sk = p.security_sk AND t.trade_date <= p.position_date
   GROUP BY 1, 2, 3, 4
  HAVING p.position_quantity{{offset}} <> expected_quantity)"""

AFTER = f"""WITH running AS (
  SELECT account_id, security_sk, trade_date,
         SUM(qty) OVER (PARTITION BY account_id, security_sk ORDER BY trade_date
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity
    FROM ({DAILY}))
SELECT COUNT(*)
  FROM CORE.FACT_DAILY_POSITION p
  ASOF JOIN running r
    MATCH_CONDITION (p.position_date >= r.trade_date)
    ON p.account_id = r.account_id AND p.security_sk = r.security_sk
 WHERE p.position_quantity{{offset}} <> COALESCE(r.running_quantity, 0)"""


def layout(cur) -> dict:
    out = {}
    for table, key in (("CORE.FACT_TRADE", "(trade_date)"), ("CORE.FACT_DAILY_POSITION", "(position_date)")):
        info = json.loads(cur.execute(f"SELECT SYSTEM$CLUSTERING_INFORMATION('{table}', '{key}')").fetchone()[0])
        rows, size = cur.execute(f"""SELECT row_count, bytes FROM INFORMATION_SCHEMA.TABLES
                                      WHERE table_schema || '.' || table_name = '{table}'""").fetchone()
        out[table] = {"rows": rows, "bytes": size, "partitions": info["total_partition_count"]}
    return out


def timed(cur, sql: str, runs: int = 3) -> dict:
    cur.execute(sql)  # warm-up (warehouse resume, metadata)
    ids, result = [], None
    for _ in range(runs):
        result = cur.execute(sql).fetchone()[0]
        ids.append(cur.sfqid)
    rows = cur.execute(f"""SELECT total_elapsed_time, execution_time, compilation_time, bytes_scanned
                             FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION(RESULT_LIMIT => 100))
                            WHERE query_id IN ({', '.join(f"'{i}'" for i in ids)})""").fetchall()
    return {"result": result, "median_ms": statistics.median(r[0] for r in rows), "runs_ms": sorted(r[0] for r in rows),
            "median_execution_ms": statistics.median(r[1] for r in rows),
            "median_compilation_ms": statistics.median(r[2] for r in rows), "bytes_scanned": rows[0][3]}


def main() -> None:
    with snowflake_connect(database=env("SNOWFLAKE_DATABASE")) as conn:
        cur = conn.cursor()
        cur.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
        print("layout:", json.dumps(layout(cur)))
        for name, sql in (("before", BEFORE), ("after", AFTER)):
            print(name, json.dumps(timed(cur, sql.format(offset=""))))
        # Equivalence on a wrong comparison: both must flag every snapshot row.
        flagged = [cur.execute(sql.format(offset=" + 1")).fetchone()[0] for sql in (BEFORE, AFTER)]
        print("equivalence (+1 offset flags every row):", flagged)


if __name__ == "__main__":
    main()
