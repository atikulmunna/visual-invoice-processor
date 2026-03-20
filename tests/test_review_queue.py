from __future__ import annotations

import json
from pathlib import Path

from app.review_queue import (
    decide_review_status,
    dismiss_review_item,
    list_review_items,
    load_review_item,
    mark_review_resolved,
    route_to_review_queue,
)


def test_decide_review_status_requires_review_on_invalid() -> None:
    decision = decide_review_status(is_valid=False, model_confidence=0.95)
    assert decision.status == "REVIEW_REQUIRED"
    assert "validation_failed" in decision.reason_codes


def test_decide_review_status_requires_review_on_low_confidence() -> None:
    decision = decide_review_status(is_valid=True, model_confidence=0.5, confidence_threshold=0.8)
    assert decision.status == "REVIEW_REQUIRED"
    assert "low_confidence" in decision.reason_codes


def test_decide_review_status_validated_when_high_confidence_and_valid() -> None:
    decision = decide_review_status(is_valid=True, model_confidence=0.95)
    assert decision.status == "VALIDATED"
    assert not decision.reason_codes


def test_route_to_review_queue_writes_reason_record_and_moves_file(tmp_path: Path) -> None:
    src = tmp_path / "invoice.pdf"
    src.write_bytes(b"pdf")
    queue = tmp_path / "Needs_Review"

    result = route_to_review_queue(
        document_id="doc-9",
        reason_codes=["low_confidence"],
        queue_dir=queue,
        source_file=src,
        metadata={"error_code": "low_confidence"},
    )

    assert result["status"] == "REVIEW_REQUIRED"
    assert result["source_file_moved_to"] is not None
    assert (queue / "invoice.pdf").exists()
    assert not src.exists()

    record_file = queue / "doc-9.json"
    payload = json.loads(record_file.read_text(encoding="utf-8"))
    assert payload["reason_codes"] == ["low_confidence"]


def test_list_and_load_review_items(tmp_path: Path) -> None:
    queue = tmp_path / "review_queue"
    route_to_review_queue(
        document_id="doc-10",
        reason_codes=["validation_failed"],
        queue_dir=queue,
        metadata={"file_hash": "hash-10"},
    )

    items = list_review_items(queue_dir=queue)
    assert len(items) == 1
    assert items[0]["document_id"] == "doc-10"

    loaded = load_review_item("doc-10", queue_dir=queue)
    assert loaded["status"] == "REVIEW_REQUIRED"
    assert loaded["metadata"]["file_hash"] == "hash-10"


def test_mark_review_resolved_updates_record(tmp_path: Path) -> None:
    queue = tmp_path / "review_queue"
    route_to_review_queue(
        document_id="doc-11",
        reason_codes=["low_confidence"],
        queue_dir=queue,
        metadata={"file_hash": "hash-11"},
    )

    updated = mark_review_resolved(
        "doc-11",
        queue_dir=queue,
        resolution_status="RESOLVED_STORED",
        resolved_record={"vendor_name": "Acme"},
        storage_result={"status": "appended", "row_id": 5},
        note="reviewed manually",
    )

    assert updated["status"] == "RESOLVED_STORED"
    assert updated["resolved_record"]["vendor_name"] == "Acme"
    assert updated["storage_result"]["row_id"] == 5
    assert updated["resolution_note"] == "reviewed manually"


def test_dismiss_review_item_marks_rejected(tmp_path: Path) -> None:
    queue = tmp_path / "review_queue"
    route_to_review_queue(
        document_id="doc-12",
        reason_codes=["validation_failed"],
        queue_dir=queue,
        metadata={"file_hash": "hash-12"},
    )

    updated = dismiss_review_item(
        "doc-12",
        queue_dir=queue,
        resolution_status="REJECTED",
        note="not a valid business document",
    )

    assert updated["review_item"]["status"] == "REJECTED"
    assert updated["storage_result"]["action"] == "REJECTED"
