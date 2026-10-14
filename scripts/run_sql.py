"""Run a Snowflake .sql file statement by statement, printing each result.

{{NAME}} placeholders are filled from .env. Usage:
  python -m scripts.run_sql snowflake/00_setup.sql [--dry-run]
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

from common.connections import env, snowflake_connect

PLACEHOLDER = re.compile(r"\{\{(\w+)\}\}")


def render(sql: str) -> str:
    return PLACEHOLDER.sub(lambda m: env(m.group(1)), sql)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("sql_file", type=Path)
    parser.add_argument("--dry-run", action="store_true", help="print the rendered SQL, run nothing")
    args = parser.parse_args()

    sql = render(args.sql_file.read_text())
    if args.dry_run:
        print(sql)
        return
    with snowflake_connect() as conn:
        execute(conn, sql, verbose=True)


def execute(conn, sql: str, verbose: bool = False) -> None:
    """Run every statement in an already-rendered SQL script on an open connection."""
    for cur in conn.execute_string(sql, remove_comments=True):
        if verbose:
            print(f"> {cur.query.strip().splitlines()[0][:100]}")
            if cur.description:
                cols = [c[0] for c in cur.description]
                for row in cur.fetchall():
                    print("  ", dict(zip(cols, row)))


if __name__ == "__main__":
    main()
