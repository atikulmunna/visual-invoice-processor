from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from app.alpha_store import AlphaStore
from app.review_queue import (
    decide_review_status,
    dismiss_review_item,
    load_review_item,
    pending_review_items,
    resolve_review_item,
    route_to_review_queue,
)
from app.storage_service import append_record

PASSWORD = "A-strong-alpha-password"
RECORD = {
    "document_type": "invoice", "vendor_name": "Acme", "invoice_number": "INV-7", "invoice_date": "2026-03-02",
    "currency": "BDT", "subtotal": 100.0, "tax_amount": 15.0, "total_amount": 115.0, "model_confidence": 0.4,
    "validation_score": 0.5, "line_items": [],
}


def test_decide_review_status_requires_review_on_invalid() -> None:
    decision = decide_review_status(is_valid=False, model_confidence=0.95)
    assert decision.status == "REVIEW_REQUIRED"
    assert "validation_failed" in decision.reason_codes


def test_values_checked_against_the_document_text_overrule_the_model_score() -> None:
    unchecked = decide_review_status(is_valid=True, model_confidence=0.3, confidence_threshold=0.5)
    checked = decide_review_status(is_valid=True, model_confidence=0.3, confidence_threshold=0.5, checked_against_text=True)
    invalid = decide_review_status(is_valid=False, model_confidence=0.3, confidence_threshold=0.5, checked_against_text=True)

    assert unchecked.reason_codes == ("low_confidence",)
    assert checked.status == "VALIDATED"
    assert invalid.reason_codes == ("validation_failed",)


def test_decide_review_status_requires_review_on_low_confidence() -> None:
    decision = decide_review_status(is_valid=True, model_confidence=0.5, confidence_threshold=0.8)
    assert decision.status == "REVIEW_REQUIRED"
    assert "low_confidence" in decision.reason_codes


def test_decide_review_status_validated_when_high_confidence_and_valid() -> None:
    decision = decide_review_status(is_valid=True, model_confidence=0.95)
    assert decision.status == "VALIDATED"
    assert not decision.reason_codes


def _org(dsn: str) -> str:
    return AlphaStore(dsn).create_user(f"queue-{uuid4().hex[:8]}", PASSWORD, max_users=1000).org_id


def _queue(dsn: str, org_id: str, record: dict[str, Any] | None = None, reasons: list[str] | None = None) -> str:
    document_id = uuid4().hex
    route_to_review_queue(
        dsn,
        document_id=document_id,
        reason_codes=reasons or ["low_confidence"],
        metadata={"source_file_id": f"inbox/{org_id}/{document_id}.pdf", "file_hash": document_id,
                  "normalized_record": record or RECORD},
        org_id=org_id,
    )
    return document_id


def test_items_wait_oldest_first_and_only_for_their_organization(dsn: str) -> None:
    org, other = _org(dsn), _org(dsn)
    first, second = _queue(dsn, org), _queue(dsn, org, reasons=["possible_duplicate"])
    theirs = _queue(dsn, other)

    pending = pending_review_items(dsn, org_id=org)

    assert [item["document_id"] for item in pending] == [first, second]
    assert pending[1]["reason_codes"] == ["possible_duplicate"]
    assert pending[0]["metadata"]["normalized_record"]["vendor_name"] == "Acme"
    assert load_review_item(dsn, theirs, org_id=other)["status"] == "REVIEW_REQUIRED"
    with pytest.raises(FileNotFoundError):
        load_review_item(dsn, theirs, org_id=org)
    with pytest.raises(FileNotFoundError):
        dismiss_review_item(dsn, theirs, resolution_status="REJECTED", org_id=org)
    with pytest.raises(FileNotFoundError):
        resolve_review_item(dsn, theirs, org_id=org)
    # An unscoped operator sees every organization.
    assert {first, second, theirs} <= {item["document_id"] for item in pending_review_items(dsn, org_id=None)}


def test_an_item_whose_file_was_stored_another_way_is_no_longer_waiting(dsn: str) -> None:
    org = _org(dsn)
    document_id = _queue(dsn, org)
    append_record(dsn, record=RECORD, metadata={"drive_file_id": "inbox/other-copy.pdf", "file_hash": document_id,
                                                  "org_id": org})

    assert pending_review_items(dsn, org_id=org) == []


def test_approving_stores_the_record_as_read_or_as_corrected(dsn: str) -> None:
    org = _org(dsn)
    as_read, corrected = _queue(dsn, org), _queue(dsn, org)

    first = resolve_review_item(dsn, as_read, note="looks right", org_id=org)
    second = resolve_review_item(dsn, corrected, record_override={**RECORD, "vendor_name": "Acme Ltd"}, org_id=org)

    assert first["storage_result"]["status"] == "appended"
    assert first["review_item"]["status"] == "RESOLVED_STORED"
    assert first["review_item"]["resolution_note"] == "looks right"
    assert first["resolved_record"]["vendor_name"] == "Acme"
    assert first["resolved_record"]["needs_review"] is False
    assert second["review_item"]["resolved_record"]["vendor_name"] == "Acme Ltd"
    assert pending_review_items(dsn, org_id=org) == []
    with pytest.raises(ValueError, match="not active"):
        resolve_review_item(dsn, as_read, org_id=org)


def test_approval_waits_until_required_fields_are_filled_and_valid(dsn: str) -> None:
    org = _org(dsn)
    document_id = _queue(dsn, org, record={**RECORD, "vendor_name": None, "total_amount": None})

    with pytest.raises(ValueError, match="Fill in the"):
        resolve_review_item(dsn, document_id, org_id=org)
    with pytest.raises(ValidationError):
        resolve_review_item(dsn, document_id, record_override={**RECORD, "currency": "taka"}, org_id=org)
    assert load_review_item(dsn, document_id, org_id=org)["status"] == "REVIEW_REQUIRED"


def test_rejected_and_duplicate_items_leave_the_queue_without_a_record(dsn: str) -> None:
    org = _org(dsn)
    rejected, duplicate = _queue(dsn, org), _queue(dsn, org)

    dismiss_review_item(dsn, rejected, resolution_status="REJECTED", note="not ours", org_id=org)
    result = dismiss_review_item(dsn, duplicate, resolution_status="RESOLVED_DUPLICATE_MANUAL", org_id=org)

    assert load_review_item(dsn, rejected, org_id=org)["resolution_note"] == "not ours"
    assert result["review_item"]["status"] == "RESOLVED_DUPLICATE_MANUAL"
    assert result["storage_result"] == {"status": "dismissed", "action": "RESOLVED_DUPLICATE_MANUAL"}
    assert pending_review_items(dsn, org_id=org) == []
    with pytest.raises(ValueError, match="Unsupported dismissal"):
        dismiss_review_item(dsn, rejected, resolution_status="STORED", org_id=org)


def test_storing_the_same_file_twice_keeps_one_record(dsn: str) -> None:
    org = _org(dsn)
    metadata = {"drive_file_id": f"inbox/{org}/a.pdf", "file_hash": uuid4().hex, "org_id": org}

    first = append_record(dsn, record=RECORD, metadata=metadata)
    second = append_record(dsn, record=RECORD, metadata=metadata)

    assert first["status"] == "appended" and isinstance(first["row_id"], int)
    assert second["status"] == "skipped_duplicate"
