"""Snowflake connections built from .env (credentials never live in code)."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")


def env(name: str, required: bool = True) -> str:
    value = os.getenv(name, "").strip()
    if required and not value:
        raise SystemExit(f"Missing {name} in .env (see .env.example)")
    return value


def snowflake_connect(**overrides):
    import snowflake.connector

    params = {
        "account": env("SNOWFLAKE_ACCOUNT"),
        "user": env("SNOWFLAKE_USER"),
        "private_key_file": str(Path(env("SNOWFLAKE_PRIVATE_KEY_PATH")).expanduser()),
        "role": env("SNOWFLAKE_ROLE"),
        "warehouse": env("SNOWFLAKE_WAREHOUSE", required=False) or None,
        "session_parameters": {"QUERY_TAG": "snowflake-incremental-dw"},
    }
    params.update(overrides)
    return snowflake.connector.connect(**params)

