from __future__ import annotations

import json
from pathlib import Path

from app.review_queue import decide_review_status, route_to_review_queue


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

