"""Unit tests for generator/upload_to_stage.py (no Snowflake connection needed)."""

import re
from datetime import date
from pathlib import Path

import pytest

from generator.generate_files import GeneratorConfig, generate, write_files
from generator.upload_to_stage import DAILY_FEEDS, REFERENCE_FEED, files_for

PIPES_SQL = Path(__file__).resolve().parents[1] / "snowflake" / "03_pipes.sql"
DAY = date(2026, 9, 1)


def _pipe_patterns() -> dict[str, str]:
    """pipe name -> PATTERN regex, read from 03_pipes.sql."""
    sql = PIPES_SQL.read_text()
    pipes = re.findall(r"CREATE PIPE IF NOT EXISTS (RAW\.\w+).*?PATTERN = '([^']+)'", sql, flags=re.S)
    return dict(pipes)


@pytest.fixture(scope="module")
def data_dir(tmp_path_factory):
    out = tmp_path_factory.mktemp("raw")
    write_files(generate(GeneratorConfig(n_accounts=20, n_days=2, n_securities=10, trades_per_day=(50, 60))), out)
    return out


def test_every_feed_has_a_pipe():
    assert set(_pipe_patterns()) == {f.pipe for f in DAILY_FEEDS + [REFERENCE_FEED]}


def test_staged_paths_match_only_their_own_pipe(data_dir):
    patterns = _pipe_patterns()
    for feed, path in files_for(DAY, data_dir, reference=True):
        staged = feed.staged_path(path)
        matches = [pipe for pipe, rx in patterns.items() if re.fullmatch(rx, staged)]
        assert matches == [feed.pipe], (staged, matches)


def test_staged_path_layout(data_dir):
    plan = {feed.name: feed.staged_path(path) for feed, path in files_for(DAY, data_dir, reference=True)}
    assert plan == {
        "securities": "reference/securities.csv.gz",
        "accounts": "accounts/accounts_20260901.csv.gz",
        "trades": "trades/trades_20260901.csv.gz",
        "control": "control/control_20260901.csv.gz",
    }


def test_reference_is_optional(data_dir):
    assert [f.name for f, _ in files_for(DAY, data_dir, reference=False)] == ["accounts", "trades", "control"]


def test_missing_day_fails_loudly(data_dir):
    with pytest.raises(SystemExit, match="Missing files"):
        files_for(date(2027, 1, 1), data_dir, reference=False)
