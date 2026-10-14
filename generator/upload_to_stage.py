"""Upload one day of broker files to the Snowflake stage and notify Snowpipe through its REST API.

For each file: PUT to @RAW.LANDING/<feed>/ (gzip-compressed), then insertFiles on that feed's pipe.
With --wait, poll COPY_HISTORY until every announced file shows a final load status.

Usage:
  python -m generator.upload_to_stage --date 2026-09-01 --wait
  python -m generator.upload_to_stage --date 2026-09-01 --reference --wait   # also the security master
  python -m generator.upload_to_stage --date 2026-09-03 --through 2026-10-12  # every generated day in range
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from common.connections import ROOT, env, snowflake_connect

STAGE = "RAW.LANDING"


@dataclass(frozen=True)
class Feed:
    name: str
    folder: str  # sub-folder of the stage
    pipe: str
    table: str

    def local_file(self, data_dir: Path, day: date | None) -> Path:
        return data_dir / (f"{self.name}_{day:%Y%m%d}.csv" if day else f"{self.name}.csv")

    def staged_path(self, local_file: Path) -> str:
        """Path relative to the stage root, as PUT (auto-compress) stores it and the REST API expects it."""
        return f"{self.folder}/{local_file.name}.gz"


DAILY_FEEDS = [
    Feed("accounts", "accounts", "RAW.PIPE_ACCOUNTS", "RAW.ACCOUNTS"),
    Feed("trades", "trades", "RAW.PIPE_TRADES", "RAW.TRADES"),
    Feed("control", "control", "RAW.PIPE_CONTROL_TOTALS", "RAW.CONTROL_TOTALS"),
]
REFERENCE_FEED = Feed("securities", "reference", "RAW.PIPE_SECURITIES", "RAW.SECURITIES")


def files_for(day: date, data_dir: Path, reference: bool) -> list[tuple[Feed, Path]]:
    plan = [(feed, feed.local_file(data_dir, day)) for feed in DAILY_FEEDS]
    if reference:
        plan.insert(0, (REFERENCE_FEED, REFERENCE_FEED.local_file(data_dir, None)))
    missing = [str(p) for _, p in plan if not p.exists()]
    if missing:
        raise SystemExit(f"Missing files (run generator/generate_files.py first): {missing}")
    return plan


def days_in_range(data_dir: Path, first: date, last: date) -> list[date]:
    """Generated business days between first and last (inclusive), from the generator's manifest."""
    manifest = json.loads((data_dir / "manifest.json").read_text())
    days = [date.fromisoformat(d["file_date"]) for d in manifest["days"]]
    return [d for d in days if first <= d <= last] or [first]


def put_files(cur, plan: list[tuple[Feed, Path]]) -> None:
    for feed, path in plan:
        cur.execute(f"PUT 'file://{path.resolve()}' @{STAGE}/{feed.folder}/ AUTO_COMPRESS = TRUE OVERWRITE = FALSE")
        source, target, _, _, _, _, status, *_ = cur.fetchone()
        print(f"PUT {source} -> @{STAGE}/{feed.folder}/{target}: {status}")
    cur.execute(f"ALTER STAGE {STAGE} REFRESH")  # keep the directory table in step with the stage


def notify_pipes(plan: list[tuple[Feed, Path]]) -> None:
    from snowflake.ingest import SimpleIngestManager, StagedFile

    private_key = Path(env("SNOWFLAKE_PRIVATE_KEY_PATH")).expanduser().read_text()
    database = env("SNOWFLAKE_DATABASE")
    for feed, path in plan:
        manager = SimpleIngestManager(
            account=env("SNOWFLAKE_ACCOUNT"),
            host=f"{env('SNOWFLAKE_ACCOUNT')}.snowflakecomputing.com",
            user=env("SNOWFLAKE_USER"),
            pipe=f"{database}.{feed.pipe}",
            private_key=private_key,
        )
        response = manager.ingest_files([StagedFile(feed.staged_path(path), None)])
        print(f"insertFiles {feed.pipe} <- {feed.staged_path(path)}: {response['responseCode']}")


def wait_for_loads(cur, plan: list[tuple[Feed, Path]], started: float, timeout_s: int = 300) -> list[dict]:
    """Poll COPY_HISTORY until each announced file has a final status. Returns one row per file."""
    pending = {(feed.table, feed.staged_path(path)) for feed, path in plan}
    results: dict[tuple[str, str], dict] = {}
    deadline = time.time() + timeout_s
    while pending and time.time() < deadline:
        time.sleep(10)
        for table, staged in sorted(pending):
            cur.execute(
                f"""SELECT file_name, status, row_count, row_parsed, error_count, first_error_message,
                           pipe_received_time, last_load_time
                    FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
                        TABLE_NAME => '{table}',
                        START_TIME => TO_TIMESTAMP_LTZ({int(started) - 60})))
                    WHERE file_name = '{staged}'
                    ORDER BY last_load_time DESC LIMIT 1"""
            )
            row = cur.fetchone()
            if row and row[1] in ("Loaded", "Load failed", "Partially loaded"):
                cols = [d[0].lower() for d in cur.description]
                results[(table, staged)] = {"table": table, **dict(zip(cols, row))}
                pending.discard((table, staged))
    for table, staged in sorted(pending):
        results[(table, staged)] = {"table": table, "file_name": staged, "status": "NOT LOADED (timed out)"}
    return [results[k] for k in sorted(results)]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    parser.add_argument("--through", type=date.fromisoformat, help="last date of a range (days from manifest.json)")
    parser.add_argument("--data-dir", type=Path, default=ROOT / "data" / "raw")
    parser.add_argument("--reference", action="store_true", help="also upload securities.csv")
    parser.add_argument("--wait", action="store_true", help="poll COPY_HISTORY until the files are loaded")
    args = parser.parse_args()

    plan = []
    for i, day in enumerate(days_in_range(args.data_dir, args.date, args.through or args.date)):
        plan += files_for(day, args.data_dir, args.reference and i == 0)
    started = time.time()
    with snowflake_connect(database=env("SNOWFLAKE_DATABASE")) as conn:
        cur = conn.cursor()
        put_files(cur, plan)
        notify_pipes(plan)
        if args.wait:
            for r in wait_for_loads(cur, plan, started):
                print(f"{r['table']:<20} {r['file_name']:<40} {r['status']:<12} "
                      f"rows={r.get('row_count')} errors={r.get('error_count')} "
                      f"received={r.get('pipe_received_time')} loaded={r.get('last_load_time')}")


if __name__ == "__main__":
    main()
