"""
Shared Snowflake connection for the loader and the live stream.

Reads settings from .env in the project root and connects with key-pair
authentication, so there is no password to store and no MFA prompt.

Check the connection (costs nothing: no warehouse is started):
    python generator/snowflake_conn.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
REQUIRED = (
    "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PRIVATE_KEY_FILE",
    "SNOWFLAKE_ROLE", "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA",
)


def connect() -> snowflake.connector.SnowflakeConnection:
    if not (ROOT / ".env").exists():
        sys.exit("No .env file found. Copy .env.example to .env and fill it in.")
    load_dotenv(ROOT / ".env")
    missing = [k for k in REQUIRED if not os.getenv(k)]
    if missing:
        sys.exit(f"Missing in .env: {', '.join(missing)}")
    key_file = Path(os.environ["SNOWFLAKE_PRIVATE_KEY_FILE"]).expanduser()
    if not key_file.exists():
        sys.exit(f"Private key not found: {key_file}")

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        authenticator="SNOWFLAKE_JWT",
        private_key_file=str(key_file),
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        session_parameters={"TIMEZONE": "UTC", "QUERY_TAG": "fleet-project"},
    )


if __name__ == "__main__":
    with connect() as conn:
        # CURRENT_* functions run in the cloud services layer: no warehouse, no credits.
        row = conn.cursor().execute(
            "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), "
            "CURRENT_DATABASE(), CURRENT_SCHEMA()"
        ).fetchone()
        labels = ("user", "role", "warehouse", "database", "schema")
        print("Connected to Snowflake")
        for label, value in zip(labels, row):
            print(f"  {label:<10}{value}")
