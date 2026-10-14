"""Snowpark (Python DataFrame API) data-quality check: trade price outliers ("fat-finger" detection).

For every trade, compare its price with the volume-weighted average price (VWAP) of the OTHER trades in the
same security on the same day (leave-one-out, so a bad price cannot hide by pulling the average toward
itself). A trade is an outlier when it deviates by more than `threshold_pct`. Days where the security has
fewer than `min_peers` other trades are not judged: there is nothing reliable to compare against.

The DataFrame operations are compiled to SQL and run inside Snowflake (no trade data is pulled to the
client); only the outlier rows and one summary row are written back:
  AUDIT.PRICE_OUTLIERS   one row per flagged trade per run
  AUDIT.DQ_RESULTS       check SNOWPARK_PRICE_OUTLIERS: PASS when none, WARN when some (a suspicious price
                         is not necessarily wrong, so it is surfaced for review rather than failing the load)

Usage:
  python -m snowpark.dq_checks [--threshold 5] [--min-peers 2]
"""

from __future__ import annotations

import argparse
import uuid

from snowflake.snowpark import DataFrame, Session, Window
from snowflake.snowpark import functions as F

CHECK_NAME = "SNOWPARK_PRICE_OUTLIERS"
OUTLIER_COLUMNS = ["TRADE_ID", "TRADE_DATE", "SECURITY_SK", "SECURITY_ID", "SYMBOL", "SIDE", "QUANTITY",
                   "PRICE", "REFERENCE_VWAP", "DEVIATION_PCT", "PEER_TRADES"]


def score_trades(trades: DataFrame, min_peers: int = 2) -> DataFrame:
    """Every trade with enough same-day peers, with its leave-one-out reference VWAP and % deviation."""
    day = Window.partition_by("SECURITY_SK", "TRADE_DATE")
    scored = trades.select(
        "TRADE_ID", "TRADE_DATE", "SECURITY_SK", "SIDE", "QUANTITY", "PRICE",
        (F.count("TRADE_ID").over(day) - 1).alias("PEER_TRADES"),
        ((F.sum("NOTIONAL").over(day) - F.col("NOTIONAL"))
         / (F.sum("QUANTITY").over(day) - F.col("QUANTITY"))).alias("REFERENCE_VWAP"),
    )
    scored = scored.filter(F.col("PEER_TRADES") >= min_peers)
    return scored.with_column(
        "DEVIATION_PCT",
        F.abs(F.col("PRICE") - F.col("REFERENCE_VWAP")) / F.col("REFERENCE_VWAP") * 100,  # stored as NUMBER(38,4)
    )


def find_price_outliers(trades: DataFrame, securities: DataFrame, threshold_pct: float = 5.0,
                        min_peers: int = 2) -> DataFrame:
    """Trades whose price deviates from the same-day peer VWAP by more than threshold_pct."""
    outliers = score_trades(trades, min_peers).filter(F.col("DEVIATION_PCT") > threshold_pct)
    return (outliers.join(securities.select("SECURITY_SK", "SECURITY_ID", "SYMBOL"), "SECURITY_SK")
                    .select(*OUTLIER_COLUMNS))


def run(session: Session, threshold_pct: float = 5.0, min_peers: int = 2) -> dict:
    run_id = str(uuid.uuid4())
    trades, securities = session.table("CORE.FACT_TRADE"), session.table("CORE.DIM_SECURITY")

    scored = score_trades(trades, min_peers)
    evaluated, max_deviation = scored.select(F.count("TRADE_ID"), F.max("DEVIATION_PCT")).collect()[0]

    outliers = (find_price_outliers(trades, securities, threshold_pct, min_peers)
                .with_column("RUN_ID", F.lit(run_id))
                .with_column("THRESHOLD_PCT", F.lit(threshold_pct)))
    outliers.write.mode("append").save_as_table("AUDIT.PRICE_OUTLIERS", column_order="name")
    flagged = session.table("AUDIT.PRICE_OUTLIERS").filter(F.col("RUN_ID") == run_id).count()

    status = "PASS" if flagged == 0 else "WARN"
    details = (f"{flagged} of {evaluated} trades deviate > {threshold_pct}% from same-day peer VWAP "
               f"(min {min_peers} peers); max deviation {max_deviation}%")
    session.sql(
        "INSERT INTO AUDIT.DQ_RESULTS (run_id, check_name, scope, expected, actual, status, details) "
        "VALUES (?, ?, 'GLOBAL', 0, ?, ?, ?)",
        params=[run_id, CHECK_NAME, flagged, status, details],
    ).collect()
    return {"run_id": run_id, "evaluated": evaluated, "flagged": flagged, "status": status,
            "max_deviation_pct": None if max_deviation is None else float(max_deviation)}


def main() -> None:
    from common.connections import snowpark_session

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--threshold", type=float, default=5.0, help="max allowed deviation from peer VWAP, in %%")
    parser.add_argument("--min-peers", type=int, default=2, help="min other same-day trades needed to judge a price")
    args = parser.parse_args()

    session = snowpark_session()
    try:
        print(run(session, args.threshold, args.min_peers))
    finally:
        session.close()


if __name__ == "__main__":
    main()
