"""Lightweight Supabase activity ping.

Supabase pauses free-plan projects after roughly a week without activity. A
portfolio project rarely gets organic traffic, so this module issues a cheap
read against Postgres (and optionally the REST gateway) on a schedule to keep
the project marked active. Nothing is written and no schema is required.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from urllib.parse import urlsplit

from app.config import load_dotenv
from app.retry_utils import RetryPolicy, run_with_retry

CONNECT_TIMEOUT_SECONDS = 20
REST_TIMEOUT_SECONDS = 20
KEEPALIVE_POLICY = RetryPolicy(max_attempts=4, base_delay_seconds=2.0, max_delay_seconds=20.0)


def describe_target(dsn: str) -> str:
    """Host and port only, so no credentials reach the CI log."""
    parts = urlsplit(dsn)
    if not parts.hostname:
        return "unparsed-dsn"
    return f"{parts.hostname}:{parts.port or 5432}"


def uses_direct_connection(dsn: str) -> bool:
    """True for db.<ref>.supabase.co, which resolves to IPv6 only.

    GitHub-hosted runners have no IPv6 route, so a direct-connection DSN fails
    there. The Supabase pooler host works from both CI and local machines.
    """
    hostname = urlsplit(dsn).hostname or ""
    return hostname.startswith("db.") and hostname.endswith(".supabase.co")


def ping_database(dsn: str) -> str:
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError("psycopg package is required for the Supabase keepalive ping") from exc

    def _query() -> str:
        with psycopg.connect(
            dsn,
            autocommit=True,
            prepare_threshold=None,
            connect_timeout=CONNECT_TIMEOUT_SECONDS,
        ) as conn:
            row = conn.execute("select now()").fetchone()
        return str(row[0]) if row else "unknown"

    return run_with_retry(_query, should_retry=lambda exc: True, policy=KEEPALIVE_POLICY)


def ping_rest(base_url: str, anon_key: str) -> int:
    """Hit the PostgREST root. Any answer proves the request reached the project."""
    try:
        import requests
    except ImportError as exc:
        raise RuntimeError("requests package is required for the Supabase REST ping") from exc

    endpoint = f"{base_url.rstrip('/')}/rest/v1/"

    def _request() -> int:
        response = requests.get(
            endpoint,
            headers={"apikey": anon_key, "Authorization": f"Bearer {anon_key}"},
            timeout=REST_TIMEOUT_SECONDS,
        )
        if response.status_code >= 500:
            raise RuntimeError(f"REST endpoint returned {response.status_code}")
        return response.status_code

    return run_with_retry(_request, should_retry=lambda exc: True, policy=KEEPALIVE_POLICY)


def main() -> int:
    load_dotenv()
    dsn = os.environ.get("POSTGRES_DSN", "").strip()
    if not dsn:
        raise SystemExit("POSTGRES_DSN is not configured")

    print(f"Keepalive started at {datetime.now(timezone.utc).isoformat()}")
    print(f"Database target: {describe_target(dsn)}")
    if uses_direct_connection(dsn):
        print(
            "Warning: this DSN points at the direct (IPv6-only) Supabase host. "
            "Use the pooler host if this run is on a GitHub-hosted runner."
        )

    server_time = ping_database(dsn)
    print(f"Database ping succeeded, server time {server_time}")

    base_url = os.environ.get("SUPABASE_URL", "").strip()
    anon_key = os.environ.get("SUPABASE_ANON_KEY", "").strip()
    if base_url and anon_key:
        status = ping_rest(base_url, anon_key)
        print(f"REST ping succeeded with status {status}")
    else:
        print("REST ping skipped (SUPABASE_URL and SUPABASE_ANON_KEY not both set)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
