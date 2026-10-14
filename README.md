# Snowflake Incremental Data Warehouse

Work in progress. A broker's daily account and trade files land in a Snowflake internal stage, are loaded by Snowpipe (REST API), and flow through Streams and Tasks into an SCD Type 2 account dimension and trade/position fact tables, with data-quality reconciliation and a PL/SQL (Oracle 23ai) comparison.

All data in this project is **synthetic**.
