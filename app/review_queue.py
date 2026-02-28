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

