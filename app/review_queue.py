from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ReviewDecision:
    status: str
    reason_codes: tuple[str, ...]


def decide_review_status(
    is_valid: bool,
    model_confidence: float,
    *,
    confidence_threshold: float = 0.85,
) -> ReviewDecision:
    reasons: list[str] = []
    if not is_valid:
        reasons.append("validation_failed")
    if model_confidence < confidence_threshold:
        reasons.append("low_confidence")
    if reasons:
        return ReviewDecision(status="REVIEW_REQUIRED", reason_codes=tuple(reasons))
    return ReviewDecision(status="VALIDATED", reason_codes=tuple())


def route_to_review_queue(
    document_id: str,
    reason_codes: list[str],
    *,
    queue_dir: str | Path = "review_queue",
    source_file: str | Path | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    target_dir = Path(queue_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    moved_file = None
    if source_file is not None:
        source_path = Path(source_file)
        destination = target_dir / source_path.name
        if source_path.exists():
            shutil.move(str(source_path), str(destination))
            moved_file = str(destination)

    record = {
        "document_id": document_id,
        "status": "REVIEW_REQUIRED",
        "reason_codes": reason_codes,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_file_moved_to": moved_file,
    }
    if metadata:
        record["metadata"] = metadata

    record_file = target_dir / f"{document_id}.json"
    record_file.write_text(json.dumps(record, ensure_ascii=True, indent=2), encoding="utf-8")
    return record


def list_review_items(queue_dir: str | Path = "review_queue") -> list[dict[str, Any]]:
    target_dir = Path(queue_dir)
    if not target_dir.exists():
        return []

    items: list[dict[str, Any]] = []
    for record_file in sorted(target_dir.glob("*.json")):
        if not record_file.is_file():
            continue
        try:
            payload = json.loads(record_file.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        payload["_record_path"] = str(record_file)
        items.append(payload)
    return items


def load_review_item(document_id: str, queue_dir: str | Path = "review_queue") -> dict[str, Any]:
    record_file = Path(queue_dir) / f"{document_id}.json"
    if not record_file.exists():
        raise FileNotFoundError(f"Review item not found: {document_id}")
    payload = json.loads(record_file.read_text(encoding="utf-8"))
    payload["_record_path"] = str(record_file)
    return payload


def mark_review_resolved(
    document_id: str,
    *,
    queue_dir: str | Path = "review_queue",
    resolution_status: str,
    resolved_record: dict[str, Any],
    storage_result: dict[str, Any],
    note: str | None = None,
) -> dict[str, Any]:
    record_file = Path(queue_dir) / f"{document_id}.json"
    if not record_file.exists():
        raise FileNotFoundError(f"Review item not found: {document_id}")

    payload = json.loads(record_file.read_text(encoding="utf-8"))
    payload["status"] = resolution_status
    payload["resolved_at_utc"] = datetime.now(timezone.utc).isoformat()
    payload["resolved_record"] = resolved_record
    payload["storage_result"] = storage_result
    if note:
        payload["resolution_note"] = note

    record_file.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    return payload


def _load_resolution_record(review_item: dict[str, Any], record_path: str | None) -> dict[str, Any]:
    if record_path:
        payload = json.loads(Path(record_path).read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("Resolved review record JSON must be an object")
        return payload

    metadata = review_item.get("metadata", {}) if isinstance(review_item.get("metadata"), dict) else {}
    payload = metadata.get("normalized_record")
    if not isinstance(payload, dict):
        raise ValueError(
            "Review item does not contain a normalized_record. Supply a corrected JSON record instead."
        )
    return payload


def resolve_review_item(
    document_id: str,
    *,
    queue_dir: str | Path = "review_queue",
    record_path: str | None = None,
    note: str | None = None,
) -> dict[str, Any]:
    from datetime import datetime, timezone

    from app.storage_service import append_record
    from app.validation import validate_and_score

    review_item = load_review_item(document_id=document_id, queue_dir=queue_dir)
    if review_item.get("status") != "REVIEW_REQUIRED":
        raise ValueError(f"Review item {document_id} is not active; current status={review_item.get('status')}")

    record = _load_resolution_record(review_item, record_path)
    validation = validate_and_score(record)
    resolved_record = validation["record"].model_dump(mode="json")
    resolved_record["validation_score"] = validation["validation_score"]
    resolved_record["needs_review"] = False

    metadata = review_item.get("metadata", {}) if isinstance(review_item.get("metadata"), dict) else {}
    drive_file_id = metadata.get("source_file_id") or metadata.get("drive_file_id")
    file_hash = metadata.get("file_hash")
    if not drive_file_id or not file_hash:
        raise ValueError("Review item metadata is missing source_file_id/drive_file_id or file_hash")

    append_metadata = {
        "document_id": document_id,
        "drive_file_id": drive_file_id,
        "file_hash": file_hash,
        "status": "STORED",
        "processed_at_utc": datetime.now(timezone.utc).isoformat(),
        "needs_review": False,
        "used_provider": metadata.get("used_provider", "manual_review"),
        "resolution_source": "manual_review",
    }

    append_result = append_record(record=resolved_record, metadata=append_metadata)
    resolution_status = "RESOLVED_STORED" if append_result.get("status") == "appended" else "RESOLVED_DUPLICATE"
    updated = mark_review_resolved(
        document_id=document_id,
        queue_dir=queue_dir,
        resolution_status=resolution_status,
        resolved_record=resolved_record,
        storage_result=append_result,
        note=note,
    )
    return {
        "review_item": updated,
        "storage_result": append_result,
        "resolved_record": resolved_record,
    }
