from __future__ import annotations

import argparse
import os
from pathlib import Path

from app.config import load_dotenv


DEFAULT_MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations"


def migration_files(directory: Path = DEFAULT_MIGRATIONS_DIR) -> list[Path]:
    return sorted(path for path in directory.glob("[0-9][0-9][0-9]_*.sql") if path.is_file())


def apply_migrations(dsn: str, directory: Path = DEFAULT_MIGRATIONS_DIR) -> list[str]:
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError("psycopg package is required for database migrations") from exc

    applied: list[str] = []
    with psycopg.connect(dsn, autocommit=True, prepare_threshold=None) as conn:
        conn.execute(
            """
            create table if not exists public.schema_migrations (
              version text primary key,
              applied_at_utc timestamptz not null default now()
            )
            """
        )
        for path in migration_files(directory):
            version = path.name
            exists = conn.execute(
                "select 1 from public.schema_migrations where version = %s",
                (version,),
            ).fetchone()
            if exists:
                continue
            with conn.transaction():
                conn.execute(path.read_text(encoding="utf-8"), prepare=False)
                conn.execute(
                    "insert into public.schema_migrations(version) values (%s)",
                    (version,),
                )
            applied.append(version)
    return applied


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Apply ordered PostgreSQL migrations")
    parser.add_argument("--migrations-dir", type=Path, default=DEFAULT_MIGRATIONS_DIR)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    load_dotenv()
    dsn = os.environ.get("POSTGRES_DSN", "").strip()
    if not dsn:
        raise SystemExit("POSTGRES_DSN is not configured")
    applied = apply_migrations(dsn, args.migrations_dir)
    if applied:
        print("Applied migrations:")
        for version in applied:
            print(f"- {version}")
    else:
        print("Database schema is already current.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
