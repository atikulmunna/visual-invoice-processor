from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# A server where the connecting role may create databases. Tests that need one are
# skipped without it; each run creates and drops its own databases.
TEST_POSTGRES_DSN = os.getenv("TEST_POSTGRES_DSN", "").strip()


def _create_database() -> str:
    if not TEST_POSTGRES_DSN:
        pytest.skip("TEST_POSTGRES_DSN is not set")
    psycopg = pytest.importorskip("psycopg")
    name = f"ledgerly_test_{uuid4().hex[:12]}"
    with psycopg.connect(TEST_POSTGRES_DSN, autocommit=True) as conn:
        conn.execute(f'CREATE DATABASE "{name}"')
        # Supabase roles referenced by the lockdown migrations.
        for role in ("anon", "authenticated"):
            conn.execute(
                f"DO $$ BEGIN CREATE ROLE {role} NOLOGIN; "
                "EXCEPTION WHEN duplicate_object THEN NULL; END $$"
            )
    return psycopg.conninfo.make_conninfo(TEST_POSTGRES_DSN, dbname=name)


def _drop_database(dsn: str) -> None:
    import psycopg

    name = psycopg.conninfo.conninfo_to_dict(dsn)["dbname"]
    with psycopg.connect(TEST_POSTGRES_DSN, autocommit=True) as conn:
        conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


@pytest.fixture
def empty_database() -> Iterator[str]:
    """A new database with no tables, dropped after the test."""
    database = _create_database()
    try:
        yield database
    finally:
        _drop_database(database)


@pytest.fixture(scope="module")
def dsn() -> Iterator[str]:
    """A fully migrated database shared by the tests of one module."""
    from app.db_migrate import apply_migrations

    database = _create_database()
    try:
        apply_migrations(database)
        yield database
    finally:
        _drop_database(database)
