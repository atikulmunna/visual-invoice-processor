from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.dead_letter import DeadLetterStore
from app.idempotency_store import DocumentClaimStore


def replay_failures(
    *,
    status: str,
    dead_letter_path: str | Path = "logs/dead_letter.jsonl",
    audit_path: str | Path = "logs/replay_audit.jsonl",
    claim_db_path: str | Path = "data/metadata.db",
    owner_id: str = "replay-worker",
) -> dict[str, int]:
    dead = DeadLetterStore(file_path=dead_letter_path)
    claim_store = DocumentClaimStore(db_path=claim_db_path)
    audit_file = Path(audit_path)
    audit_file.parent.mkdir(parents=True, exist_ok=True)

    entries = dead.list_failures(status=status)
    summary = {"queued": 0, "skipped_processed": 0, "skipped_invalid": 0}

    with audit_file.open("a", encoding="utf-8") as fh:
        for item in entries:
            drive_file_id = item.get("drive_file_id")
            file_hash = item.get("file_hash")
            document_id = item.get("document_id")
            if not drive_file_id or not file_hash or not document_id:
                summary["skipped_invalid"] += 1
                _write_audit(
                    fh,
                    document_id=document_id,
                    outcome="skipped_invalid",
                    status=status,
                    reason="missing drive_file_id/file_hash/document_id",
                )
                continue

            claim_result = claim_store.claim_document(
                drive_file_id=drive_file_id,
                file_hash=file_hash,
                owner_id=owner_id,
            )
            if claim_result.status == "already_processed":
                summary["skipped_processed"] += 1
                _write_audit(
                    fh,
                    document_id=document_id,
                    outcome="skipped_processed",
                    status=status,
                    reason="already_processed",
                )
                continue

            summary["queued"] += 1
            _write_audit(
                fh,
                document_id=document_id,
                outcome="queued_for_replay",
                status=status,
                reason="claim_acquired",
            )

    return summary


def _write_audit(
    fh: Any,
    *,
    document_id: str | None,
    outcome: str,
    status: str,
    reason: str,
) -> None:
    event = {
        "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
        "document_id": document_id,
        "status": status,
        "outcome": outcome,
        "reason": reason,
    }
    fh.write(json.dumps(event, ensure_ascii=True) + "\n")

