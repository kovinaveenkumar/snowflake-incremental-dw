-- Step 11: SQL performance tuning (measured by scripts/tuning.py; results in docs/results.md).
--
-- Clustering key on CORE.FACT_TRADE(trade_date): NOT applied. Both fact tables are a single micro-partition
-- at this data size (FACT_TRADE 59,853 rows / 1.9 MB; FACT_DAILY_POSITION 648,618 rows / 10.7 MB), so there
-- is nothing to prune and automatic clustering would only add serverless cost. At production volume
-- (hundreds of partitions) the key to use is the date most queries filter on:
--   ALTER TABLE CORE.FACT_TRADE CLUSTER BY (trade_date);
--   ALTER TABLE CORE.FACT_DAILY_POSITION CLUSTER BY (position_date);
-- and the check is SYSTEM$CLUSTERING_INFORMATION before/after plus partitions scanned in the query profile.
--
-- Query rewrite measured on the real data: the DQ check "every position equals the running sum of its
-- trades". The range join (each snapshot row x every earlier trade day of its pair) was rewritten as a
-- window running sum + ASOF JOIN to the latest trade day on or before the snapshot day. Same result
-- (also on a deliberately wrong comparison), 46 % fewer bytes scanned, no measurable elapsed-time change
-- at this size; the deployed check in 09_data_quality.sql was left as is.

USE ROLE DW_ROLE;
USE DATABASE BROKER_DW;

WITH daily AS (
  SELECT a.account_id, f.security_sk, f.trade_date, SUM(f.signed_quantity) AS qty
    FROM CORE.FACT_TRADE f JOIN CORE.DIM_ACCOUNT a ON a.account_sk = f.account_sk
   GROUP BY 1, 2, 3
),
running AS (
  SELECT account_id, security_sk, trade_date,
         SUM(qty) OVER (PARTITION BY account_id, security_sk ORDER BY trade_date
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity
    FROM daily
)
SELECT COUNT(*) AS positions_not_equal_to_running_sum
  FROM CORE.FACT_DAILY_POSITION p
  ASOF JOIN running r
    MATCH_CONDITION (p.position_date >= r.trade_date)
    ON p.account_id = r.account_id AND p.security_sk = r.security_sk
 WHERE p.position_quantity <> COALESCE(r.running_quantity, 0);
