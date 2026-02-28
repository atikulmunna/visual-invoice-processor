from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from app.idempotency_store import DocumentClaimStore


def test_reprocessing_same_file_returns_already_claimed(tmp_path: Path) -> None:
    store = DocumentClaimStore(db_path=tmp_path / "claims.db")
    first = store.claim_document("file-1", "hash-1", owner_id="worker-a")
    second = store.claim_document("file-1", "hash-1", owner_id="worker-b")

    assert first.status == "claimed"
    assert second.status == "already_claimed"


def test_concurrent_claim_attempts_only_one_winner(tmp_path: Path) -> None:
    store = DocumentClaimStore(db_path=tmp_path / "claims.db")

    def _claim(worker: str) -> str:
        return store.claim_document("file-2", "hash-2", owner_id=worker).status

    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(_claim, [f"w{i}" for i in range(6)]))

    assert results.count("claimed") == 1
    assert results.count("already_claimed") == 5


def test_already_processed_after_stored_status(tmp_path: Path) -> None:
    store = DocumentClaimStore(db_path=tmp_path / "claims.db")
    store.claim_document("file-3", "hash-3", owner_id="worker-a")
    store.mark_status("file-3", "hash-3", "STORED")

    result = store.claim_document("file-3", "hash-3", owner_id="worker-b")
    assert result.status == "already_processed"

