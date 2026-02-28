from __future__ import annotations

from typing import Any

from app.storage_service import PostgresStorageService


class _FakeCursor:
    def __init__(self, duplicate: bool = False) -> None:
        self.duplicate = duplicate
        self.queries: list[tuple[str, tuple[Any, ...] | None]] = []
        self._fetch = None

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None

    def execute(self, query: str, params: tuple[Any, ...] | None = None) -> None:
        self.queries.append((query, params))
        if "RETURNING id" in query:
            self._fetch = None if self.duplicate else (101,)

    def fetchone(self) -> tuple[int] | None:
        return self._fetch


class _FakeConn:
    def __init__(self, duplicate: bool = False) -> None:
        self.duplicate = duplicate
        self.commits = 0
        self.cursor_obj = _FakeCursor(duplicate=duplicate)

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None

    def cursor(self) -> _FakeCursor:
        return self.cursor_obj

    def commit(self) -> None:
        self.commits += 1


class _TestPostgresStorageService(PostgresStorageService):
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn
        super().__init__(dsn="postgres://unused", table_name="ledger_records")

    def _connect(self) -> _FakeConn:
        return self._conn


def _record() -> dict[str, Any]:
    return {
        "document_type": "invoice",
        "vendor_name": "Acme",
        "total_amount": 10.0,
    }


def _metadata() -> dict[str, Any]:
    return {
        "drive_file_id": "file-1",
        "file_hash": "hash-1",
        "status": "STORED",
    }


def test_postgres_append_returns_row_id_for_new_record() -> None:
    service = _TestPostgresStorageService(conn=_FakeConn(duplicate=False))
    result = service.append_record(_record(), _metadata())
    assert result["status"] == "appended"
    assert result["backend"] == "postgres"
    assert result["row_id"] == 101


def test_postgres_append_skips_duplicate() -> None:
    service = _TestPostgresStorageService(conn=_FakeConn(duplicate=True))
    result = service.append_record(_record(), _metadata())
    assert result["status"] == "skipped_duplicate"
    assert result["backend"] == "postgres"

