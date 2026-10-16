"""Turns one uploaded document into a stored record or a review item.

The S3-triggered worker calls process_document for each upload. It downloads the file, claims it so
the same bytes are never processed twice in an organization, has the model read it, normalizes and
checks the result, and then either stores the record or sends it to review with plain reasons.
"""

from __future__ import annotations

import hashlib
import logging
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from pydantic import ValidationError

from app.alpha_store import AlphaStore
from app.config import Settings
from app.duplicates import check_for_duplicate
from app.evidence import assess_fields, evidence_reasons
from app.extraction_service import ExtractionError, extract_document
from app.normalization_engine import NormalizationRuleEngine
from app.object_storage_service import ObjectStorageService
from app.review_queue import decide_review_status, route_to_review_queue
from app.storage_service import append_record
from app.validation import review_reason_codes, validate_and_score
from app.vendor_memory import hints_for_worker, trusted_for_worker

logger = logging.getLogger(__name__)

_TMP_DIR = (
    Path(tempfile.gettempdir()) / "invoice-processor" if os.getenv("AWS_LAMBDA_FUNCTION_NAME") else Path("tmp")
)


class DocumentRejectedError(RuntimeError):
    def __init__(self, message: str, code: str = "document_rejected") -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class Thresholds:
    review_confidence: float
    store_review_score: float


@dataclass
class Reading:
    """What the model read from one document, normalized and checked."""

    document_id: str
    source_id: str
    file_hash: str
    org_id: str | None
    extracted: dict[str, Any]
    record: dict[str, Any]
    used_provider: str
    evidence: dict[str, Any]
    duplicate: dict[str, Any] | None

    @property
    def extra_reasons(self) -> list[str]:
        """Review reasons that come from checks beyond validation."""
        return evidence_reasons(self.evidence) + (["possible_duplicate"] if self.duplicate else [])


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def process_document(
    candidate: dict[str, str],
    *,
    settings: Settings,
    storage: ObjectStorageService,
    store: AlphaStore,
    normalization_engine: NormalizationRuleEngine,
    extraction_model: str,
    thresholds: Thresholds,
    worker_id: str,
    before_extract: Callable[[Path, str], dict[str, Any] | None] | None = None,
) -> dict[str, Any]:
    """One upload, start to finish. Never raises: failures come back as a FAILED or REJECTED result."""
    source_id = candidate["id"]
    _TMP_DIR.mkdir(parents=True, exist_ok=True)
    local_path = _TMP_DIR / f"{uuid4().hex}_{candidate.get('name', 'document')}"
    try:
        storage.download_file(object_key=source_id, out_path=local_path)
        file_hash = file_sha256(local_path)
        claim = store.claim_document(source_id, file_hash, owner_id=worker_id)
        if claim.status != "claimed":
            return {"source_id": source_id, "status": "SKIPPED_DUPLICATE", "file_hash": file_hash}
        checks = before_extract(local_path, file_hash) if before_extract else None
        reading = _read(local_path, candidate, file_hash, settings, normalization_engine, extraction_model)
        return _route(reading, settings=settings, storage=storage, store=store, thresholds=thresholds, checks=checks)
    except DocumentRejectedError as exc:
        return _fail(store, candidate, local_path, status="REJECTED", code=exc.code, message=str(exc))
    except ExtractionError as exc:
        logger.exception("Extraction failed for source_id=%s", source_id)
        return _fail(store, candidate, local_path, status="FAILED", code=exc.code, message=str(exc))
    except Exception as exc:  # noqa: BLE001
        logger.exception("Pipeline failed for source_id=%s", source_id)
        return _fail(store, candidate, local_path, status="FAILED", code="pipeline_error", message=str(exc))
    finally:
        local_path.unlink(missing_ok=True)


def _read(
    local_path: Path,
    candidate: dict[str, str],
    file_hash: str,
    settings: Settings,
    normalization_engine: NormalizationRuleEngine,
    extraction_model: str,
) -> Reading:
    org_id = candidate.get("org_id")
    extracted = extract_document(
        file_path=local_path,
        provider="openrouter",
        model_name=extraction_model,
        hints=hints_for_worker(settings, local_path, org_id),
    )
    used_provider = str(extracted.get("_provider", "unknown"))
    logger.info("Extraction provider=%s source_id=%s", used_provider, candidate["id"])
    record = normalization_engine.coerce_payload(extracted, base_currency=candidate.get("base_currency"))
    return Reading(
        document_id=str(uuid4()),
        source_id=candidate["id"],
        file_hash=file_hash,
        org_id=org_id,
        extracted=extracted,
        record=record,
        used_provider=used_provider,
        # Each value checked against the document's own text, where it has one.
        evidence=assess_fields(record, extracted.get("_ocr_text"), datetime.now(timezone.utc).date()),
        # A different file of an invoice already stored: never stored twice without a person deciding.
        duplicate=check_for_duplicate(settings, record, org_id),
    )


def _route(
    reading: Reading,
    *,
    settings: Settings,
    storage: ObjectStorageService,
    store: AlphaStore,
    thresholds: Thresholds,
    checks: dict[str, Any] | None,
) -> dict[str, Any]:
    try:
        validation = validate_and_score(reading.record)
    except ValidationError as exc:
        reasons = review_reason_codes(reading.record, exc) + reading.extra_reasons
        return _send_to_review(reading, reasons, settings, store, {"error": str(exc), "raw_extracted": reading.extracted})

    decision = decide_review_status(
        is_valid=validation["is_valid"],
        model_confidence=float(validation["record"].model_confidence),
        confidence_threshold=thresholds.review_confidence,
        checked_against_text=reading.evidence["text_available"],
    )
    reasons = list(decision.reason_codes) + reading.extra_reasons
    # A vendor whose recent reviews all needed no changes outweighs the model doubting itself, and nothing else.
    vouched = reasons == ["low_confidence"] and trusted_for_worker(settings, reading.record, reading.org_id)
    if reasons and not vouched:
        return _send_to_review(reading, reasons, settings, store, {"violations": validation["violations"]})
    result = _store(reading, validation, settings, storage, store, thresholds=thresholds, vouched=vouched)
    return {**result, **(checks or {})}


def _send_to_review(
    reading: Reading, reasons: list[str], settings: Settings, store: AlphaStore, details: dict[str, Any]
) -> dict[str, Any]:
    route_to_review_queue(
        settings.postgres_dsn or "",
        document_id=reading.document_id,
        reason_codes=reasons,
        metadata={
            "source_file_id": reading.source_id,
            "file_hash": reading.file_hash,
            "normalized_record": reading.record,
            "used_provider": reading.used_provider,
            "duplicate_of": reading.duplicate,
            "field_evidence": reading.evidence,
            **details,
        },
        org_id=reading.org_id,
    )
    store.mark_status(reading.source_id, reading.file_hash, "REVIEW_REQUIRED")
    logger.info("Document %s sent to review: %s", reading.document_id, ", ".join(reasons))
    return {
        "source_id": reading.source_id,
        "document_id": reading.document_id,
        "status": "REVIEW_REQUIRED",
        "reason_codes": reasons,
        "file_hash": reading.file_hash,
        "record": reading.record,
    }


def _store(
    reading: Reading,
    validation: dict[str, Any],
    settings: Settings,
    storage: ObjectStorageService,
    store: AlphaStore,
    *,
    thresholds: Thresholds,
    vouched: bool,
) -> dict[str, Any]:
    record = validation["record"].model_dump(mode="json")
    record["validation_score"] = validation["validation_score"]
    needs_review = validation["validation_score"] < thresholds.store_review_score
    record["needs_review"] = needs_review
    record["field_evidence"] = reading.evidence
    metadata = {
        "document_id": reading.document_id,
        "drive_file_id": reading.source_id,
        "file_hash": reading.file_hash,
        "status": "STORED",
        "processed_at_utc": datetime.now(timezone.utc).isoformat(),
        "needs_review": needs_review,
        "used_provider": reading.used_provider,
        "org_id": reading.org_id,
    }
    if vouched:
        metadata["resolution_source"] = "vendor_track_record"
    append_result = append_record(settings.postgres_dsn or "", record=record, metadata=metadata)
    store.mark_status(reading.source_id, reading.file_hash, "STORED")
    storage.move_to_archive(object_key=reading.source_id)
    store.mark_status(reading.source_id, reading.file_hash, "ARCHIVED")
    logger.info("Stored document_id=%s source_id=%s result=%s", reading.document_id, reading.source_id,
                append_result.get("status"))
    return {
        "source_id": reading.source_id,
        "document_id": reading.document_id,
        "status": "STORED",
        "append_result": append_result,
        "file_hash": reading.file_hash,
        "used_provider": reading.used_provider,
        "needs_review": needs_review,
        "record": record,
    }


def _fail(
    store: AlphaStore, candidate: dict[str, str], local_path: Path, *, status: str, code: str, message: str
) -> dict[str, Any]:
    """Records a rejected or failed document in the audit log and releases its claim for a retry."""
    file_hash = file_sha256(local_path) if local_path.exists() else ""
    store.write_failure(
        {
            "job_id": candidate.get("job_id"),
            "document_id": str(uuid4()),
            "drive_file_id": candidate["id"],
            "file_hash": file_hash,
            "status": status,
            "error_code": code,
            "error_message": message,
        }
    )
    if file_hash:
        store.mark_status(candidate["id"], file_hash, status)
    return {"source_id": candidate["id"], "status": status, "error_code": code, "error_message": message}
