from __future__ import annotations

import json
from pathlib import Path

from app.dead_letter import DeadLetterStore
from app.idempotency_store import DocumentClaimStore
from app.replay import replay_failures


def test_replay_skips_already_processed_and_writes_audit(tmp_path: Path) -> None:
    dead_path = tmp_path / "dead.jsonl"
    audit_path = tmp_path / "replay_audit.jsonl"
    db_path = tmp_path / "claims.db"

    dead = DeadLetterStore(file_path=dead_path)
    dead.write_failure(
        {
            "document_id": "doc-1",
            "drive_file_id": "file-1",
            "file_hash": "hash-1",
            "status": "FAILED",
        }
    )
    dead.write_failure(
        {
            "document_id": "doc-2",
            "drive_file_id": "file-2",
            "file_hash": "hash-2",
            "status": "FAILED",
        }
    )

    claim_store = DocumentClaimStore(db_path=db_path)
    claim_store.claim_document("file-1", "hash-1", owner_id="worker-1")
    claim_store.mark_status("file-1", "hash-1", "STORED")

    summary = replay_failures(
        status="FAILED",
        dead_letter_path=dead_path,
        audit_path=audit_path,
        claim_db_path=db_path,
        owner_id="replay-worker",
    )

    assert summary["queued"] == 1
    assert summary["skipped_processed"] == 1
    assert summary["skipped_invalid"] == 0

    lines = audit_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    payloads = [json.loads(line) for line in lines]
    assert any(p["outcome"] == "queued_for_replay" for p in payloads)
    assert any(p["outcome"] == "skipped_processed" for p in payloads)


def test_replay_skips_invalid_dead_letter_records(tmp_path: Path) -> None:
    dead_path = tmp_path / "dead.jsonl"
    dead = DeadLetterStore(file_path=dead_path)
    dead.write_failure({"document_id": "doc-x", "status": "FAILED"})

    summary = replay_failures(
        status="FAILED",
        dead_letter_path=dead_path,
        audit_path=tmp_path / "audit.jsonl",
        claim_db_path=tmp_path / "claims.db",
    )

    assert summary["queued"] == 0
    assert summary["skipped_invalid"] == 1

