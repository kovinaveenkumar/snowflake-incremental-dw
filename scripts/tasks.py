"""Control the load Task graph (snowflake/08_tasks.sql).

  python -m scripts.tasks status            task states + the last graph runs
  python -m scripts.tasks resume            start the schedule (root + all dependents)
  python -m scripts.tasks suspend           stop the schedule (root only; dependents keep their state)
  python -m scripts.tasks run-once [--wait] run the graph once now without starting the schedule
"""

from __future__ import annotations

import argparse
import json
import time

from common.connections import env, snowflake_connect

ROOT = "CORE.TASK_ROOT"


def graph_tasks(cur, root: str = ROOT) -> list[str]:
    """Root first, then dependents, as fully qualified names."""
    cur.execute(f"SELECT database_name, schema_name, name FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS("
                f"TASK_NAME => '{root}', RECURSIVE => TRUE))")
    return [f"{db}.{schema}.{name}" for db, schema, name in cur.fetchall()]


def task_states(cur) -> list[tuple[str, str, str]]:
    cur.execute("SHOW TASKS IN SCHEMA CORE")
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return [(r["name"], r["state"], r["schedule"] or "after " + ", ".join(json.loads(r["predecessors"] or "[]")))
            for r in rows]


def graph_runs(cur, limit: int = 10) -> list[dict]:
    cur.execute(f"""SELECT run_id, state, scheduled_time, query_start_time, completed_time,
                           first_error_task_name, first_error_message
                      FROM TABLE(INFORMATION_SCHEMA.COMPLETE_TASK_GRAPHS(
                           ROOT_TASK_NAME => '{ROOT.split('.')[-1]}', RESULT_LIMIT => {limit}))
                     WHERE database_name = CURRENT_DATABASE()
                     ORDER BY scheduled_time DESC""")
    cols = [d[0].lower() for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def task_runs(cur, run_id) -> list[dict]:
    """Per-task history of one graph run."""
    cur.execute(f"""SELECT name, state, query_start_time, completed_time,
                           DATEDIFF('ms', query_start_time, completed_time) AS ms, return_value, error_message
                      FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
                           SCHEDULED_TIME_RANGE_START => DATEADD('day', -1, CURRENT_TIMESTAMP()),
                           RESULT_LIMIT => 1000))
                     WHERE run_id = {int(run_id)} AND database_name = CURRENT_DATABASE()
                     ORDER BY query_start_time""")
    cols = [d[0].lower() for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def resume(cur) -> None:
    cur.execute(f"SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('{ROOT}')")


def suspend(cur) -> None:
    cur.execute(f"ALTER TASK {ROOT} SUSPEND")


def run_once(cur, wait: bool = True, timeout_s: int = 600) -> dict | None:
    """Resume the child tasks (not the root, so no schedule starts), execute the root, optionally wait."""
    for task in graph_tasks(cur)[1:]:
        cur.execute(f"ALTER TASK {task} RESUME")
    started = cur.execute("SELECT CURRENT_TIMESTAMP()").fetchone()[0]
    cur.execute(f"EXECUTE TASK {ROOT}")
    if not wait:
        return None
    deadline = time.time() + timeout_s
    root_run = None
    while time.time() < deadline:
        time.sleep(10)
        if root_run is None:
            root_run = _root_run_since(cur, started)
            # A root whose WHEN condition is false is SKIPPED and never shows in COMPLETE_TASK_GRAPHS.
            if root_run and root_run["state"] in ("SKIPPED", "FAILED", "CANCELLED"):
                return {"run_id": root_run["run_id"], "state": root_run["state"],
                        "scheduled_time": root_run["scheduled_time"], "first_error_task_name": None,
                        "first_error_message": root_run["error_message"]}
        if root_run is not None:
            done = [r for r in graph_runs(cur, 20) if r["run_id"] == root_run["run_id"]]
            if done:
                return done[0]
    raise TimeoutError(f"Task graph did not finish within {timeout_s} s")


def _root_run_since(cur, started) -> dict | None:
    """The root task's first run scheduled at or after `started` in this database, once it has a final state."""
    cur.execute(f"""SELECT run_id, state, scheduled_time, error_message
                      FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
                           TASK_NAME => '{ROOT.split('.')[-1]}',
                           SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())))
                     WHERE database_name = CURRENT_DATABASE() AND scheduled_time >= %s
                       AND state IN ('SUCCEEDED', 'SKIPPED', 'FAILED', 'CANCELLED')
                     ORDER BY scheduled_time LIMIT 1""", (started,))
    row = cur.fetchone()
    return dict(zip(["run_id", "state", "scheduled_time", "error_message"], row)) if row else None


def _print_status(cur) -> None:
    for name, state, schedule in task_states(cur):
        print(f"{name:<34} {state:<10} {schedule}")
    print("\nLast graph runs:")
    for r in graph_runs(cur):
        print(f"  {r['scheduled_time']:%Y-%m-%d %H:%M:%S}  {r['state']:<10} "
              f"{r['first_error_task_name'] or ''} {r['first_error_message'] or ''}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["status", "resume", "suspend", "run-once"])
    parser.add_argument("--wait", action="store_true", help="run-once: wait for the graph to finish")
    args = parser.parse_args()

    with snowflake_connect(database=env("SNOWFLAKE_DATABASE")) as conn:
        cur = conn.cursor()
        if args.command == "resume":
            resume(cur)
        elif args.command == "suspend":
            suspend(cur)
        elif args.command == "run-once":
            run = run_once(cur, wait=args.wait)
            if run:
                print(f"Graph run {run['run_id']}: {run['state']} "
                      f"({run['first_error_task_name'] or ''} {run['first_error_message'] or ''})")
                for t in task_runs(cur, run["run_id"]):
                    print(f"  {t['name']:<32} {t['state']:<10} {t['ms']} ms  {t['return_value'] or t['error_message'] or ''}")
        _print_status(cur)


if __name__ == "__main__":
    main()
