from __future__ import annotations

from pathlib import Path

import app.review_queue as review_queue


def test_queue_backend_uses_filesystem_for_custom_path(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("REVIEW_QUEUE_BACKEND", raising=False)
    assert review_queue._queue_backend(tmp_path / "review_queue") == "filesystem"


def test_queue_backend_prefers_postgres_when_auto_and_postgres_enabled(monkeypatch) -> None:
    monkeypatch.setenv("REVIEW_QUEUE_BACKEND", "auto")

    class _Settings:
        ledger_backend = "postgres"
        postgres_dsn = "postgresql://example"

    monkeypatch.setattr(review_queue, "_settings_or_none", lambda: _Settings())
    assert review_queue._queue_backend("review_queue") == "postgres"
