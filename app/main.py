from __future__ import annotations

import argparse
import hashlib
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from app.auth import get_google_credentials
from app.config import Settings, load_dotenv
from app.dead_letter import DeadLetterStore
from app.drive_service import DriveService
from app.extraction_service import ExtractionError, extract_document
from app.idempotency_store import DocumentClaimStore
from app.logger import configure_logging
from app.metrics import JsonlMetricsSink, MetricsCollector
from app.normalization_engine import NormalizationRuleEngine
from app.object_storage_service import ObjectStorageService
from app.review_queue import (
    decide_review_status,
    list_review_items,
    resolve_review_item,
    route_to_review_queue,
)
from app.replay import replay_failures
from app.storage_service import append_record
from app.validation import review_reason_codes, validate_and_score
from pydantic import ValidationError

_TMP_DIR = (
    Path(tempfile.gettempdir()) / "invoice-processor"
    if os.getenv("AWS_LAMBDA_FUNCTION_NAME")
    else Path("tmp")
)


class DocumentRejectedError(RuntimeError):
    def __init__(self, message: str, code: str = "document_rejected") -> None:
        super().__init__(message)
        self.code = code


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _download_candidate(settings: Settings, backend: object, candidate: dict[str, str], out_path: Path) -> Path:
    file_id = candidate["id"]
    if settings.ingestion_backend == "drive":
        assert isinstance(backend, DriveService)
        return backend.download_file(file_id=file_id, out_path=out_path)
    assert isinstance(backend, ObjectStorageService)
    return backend.download_file(object_key=file_id, out_path=out_path)


def _archive_candidate(settings: Settings, backend: object, candidate: dict[str, str]) -> None:
    if settings.ingestion_backend == "s3":
        assert isinstance(backend, ObjectStorageService)
        backend.move_to_archive(object_key=candidate["id"])


def run_poll_once() -> int:
    load_dotenv()
    settings = Settings.from_env()
    configure_logging(settings.log_level)
    logger = logging.getLogger(__name__)

    if settings.ingestion_backend == "drive":
        credentials = get_google_credentials(settings)
        drive = DriveService.from_credentials(credentials, settings)
        backend: object = drive
        files = drive.list_inbox_files()
    else:
        object_storage = ObjectStorageService.from_settings(settings)
        backend = object_storage
        files = object_storage.list_inbox_files()

    logger.info(
        "Found %d candidate files in %s inbox",
        len(files),
        settings.ingestion_backend,
    )

    _TMP_DIR.mkdir(parents=True, exist_ok=True)
    claim_store = DocumentClaimStore()
    dead_letter = DeadLetterStore()
    metrics = MetricsCollector()
    metrics_sink = JsonlMetricsSink()
    normalization_engine = NormalizationRuleEngine.from_path(settings.normalization_rules_path)

    extraction_provider = "openrouter"
    extraction_model = os.getenv("EXTRACTION_MODEL", "auto")
    worker_id = os.getenv("WORKER_ID", "poll-once")
    review_threshold = float(os.getenv("REVIEW_CONFIDENCE_THRESHOLD", "0.5"))
    store_review_score_threshold = float(os.getenv("STORE_REVIEW_SCORE_THRESHOLD", "0.6"))

    for candidate in files:
        _process_candidate(
            candidate=candidate,
            settings=settings,
            backend=backend,
            claim_store=claim_store,
            dead_letter=dead_letter,
            metrics=metrics,
            normalization_engine=normalization_engine,
            extraction_provider=extraction_provider,
            extraction_model=extraction_model,
            worker_id=worker_id,
            review_threshold=review_threshold,
            store_review_score_threshold=store_review_score_threshold,
            archive_on_success=True,
        )

    snapshot = metrics.snapshot()
    for key, value in snapshot.items():
        if isinstance(value, int):
            metrics_sink.emit({"metric": key, "value": value, "stage": "poll_once"})
    logger.info("Poll summary: %s", snapshot)
    return 0


def _process_candidate(
    *,
    candidate: dict[str, str],
    settings: Settings,
    backend: object,
    claim_store: DocumentClaimStore,
    dead_letter: DeadLetterStore,
    metrics: MetricsCollector,
    normalization_engine: NormalizationRuleEngine,
    extraction_provider: str,
    extraction_model: str,
    worker_id: str,
    review_threshold: float,
    store_review_score_threshold: float,
    archive_on_success: bool,
    before_extract: Callable[[Path, str], dict[str, Any] | None] | None = None,
) -> dict[str, Any]:
    logger = logging.getLogger(__name__)
    metrics.increment("documents_processed_total")
    file_id = candidate["id"]
    file_name = candidate.get("name", "document")
    org_id = candidate.get("org_id")
    _TMP_DIR.mkdir(parents=True, exist_ok=True)
    local_path = _TMP_DIR / f"{uuid4().hex}_{file_name}"
    result: dict[str, Any] = {"source_id": file_id, "status": "UNKNOWN"}

    try:
        _download_candidate(settings, backend, candidate, local_path)
        file_hash = _sha256(local_path)
        claim = claim_store.claim_document(file_id, file_hash, owner_id=worker_id)
        if claim.status != "claimed":
            metrics.increment("documents_duplicate_skipped_total")
            return {"source_id": file_id, "status": "SKIPPED_DUPLICATE", "file_hash": file_hash}

        validation_metadata = before_extract(local_path, file_hash) if before_extract else None

        document_id = str(uuid4())
        result["document_id"] = document_id
        extracted = extract_document(
            file_path=local_path,
            provider=extraction_provider,
            model_name=extraction_model,
        )
        used_provider = str(extracted.get("_provider", "unknown"))
        logger.info("Extraction provider=%s source_id=%s", used_provider, file_id)
        normalized_payload = normalization_engine.coerce_payload(
            extracted,
            base_currency=candidate.get("base_currency"),
        )
        try:
            validation = validate_and_score(normalized_payload)
        except ValidationError as exc:
            reason_codes = review_reason_codes(normalized_payload, exc)
            route_to_review_queue(
                document_id=document_id,
                reason_codes=reason_codes,
                metadata={
                    "source_file_id": file_id,
                    "file_hash": file_hash,
                    "error": str(exc),
                    "raw_extracted": extracted,
                    "normalized_record": normalized_payload,
                    "used_provider": used_provider,
                },
                org_id=org_id,
            )
            dead_letter.write_failure(
                {
                    "document_id": document_id,
                    "drive_file_id": file_id,
                    "file_hash": file_hash,
                    "status": "REVIEW_REQUIRED",
                    "error_code": ",".join(reason_codes),
                    "error_message": str(exc),
                    "used_provider": used_provider,
                }
            )
            claim_store.mark_status(file_id, file_hash, "REVIEW_REQUIRED")
            metrics.increment("documents_review_total")
            logger.info("Document %s sent to review: %s", document_id, ", ".join(reason_codes))
            return {
                "source_id": file_id,
                "document_id": document_id,
                "status": "REVIEW_REQUIRED",
                "reason_codes": reason_codes,
                "file_hash": file_hash,
                "record": normalized_payload,
            }
        decision = decide_review_status(
            is_valid=validation["is_valid"],
            model_confidence=float(validation["record"].model_confidence),
            confidence_threshold=review_threshold,
        )

        if decision.status == "REVIEW_REQUIRED":
            route_to_review_queue(
                document_id=document_id,
                reason_codes=list(decision.reason_codes),
                metadata={
                    "source_file_id": file_id,
                    "file_hash": file_hash,
                    "normalized_record": normalized_payload,
                    "violations": validation["violations"],
                    "used_provider": used_provider,
                },
                org_id=org_id,
            )
            dead_letter.write_failure(
                {
                    "document_id": document_id,
                    "drive_file_id": file_id,
                    "file_hash": file_hash,
                    "status": "REVIEW_REQUIRED",
                    "error_code": ",".join(decision.reason_codes),
                    "error_message": "Routed to review queue",
                    "used_provider": used_provider,
                }
            )
            claim_store.mark_status(file_id, file_hash, "REVIEW_REQUIRED")
            metrics.increment("documents_review_total")
            logger.info("Document %s routed to review", document_id)
            return {
                "source_id": file_id,
                "document_id": document_id,
                "status": "REVIEW_REQUIRED",
                "reason_codes": list(decision.reason_codes),
                "file_hash": file_hash,
                "record": normalized_payload,
            }

        record = validation["record"].model_dump(mode="json")
        record["validation_score"] = validation["validation_score"]
        needs_review = validation["validation_score"] < store_review_score_threshold
        record["needs_review"] = needs_review
        metadata = {
            "document_id": document_id,
            "drive_file_id": file_id,
            "file_hash": file_hash,
            "status": "STORED",
            "processed_at_utc": datetime.now(timezone.utc).isoformat(),
            "needs_review": needs_review,
            "used_provider": used_provider,
            "org_id": org_id,
        }
        append_result = append_record(record=record, metadata=metadata)
        claim_store.mark_status(file_id, file_hash, "STORED")
        if archive_on_success:
            _archive_candidate(settings, backend, candidate)
            claim_store.mark_status(file_id, file_hash, "ARCHIVED")
        metrics.increment("documents_success_total")
        logger.info(
            "Stored document_id=%s source_id=%s result=%s",
            document_id,
            file_id,
            append_result.get("status"),
        )
        success_result = {
            "source_id": file_id,
            "document_id": document_id,
            "status": "STORED",
            "append_result": append_result,
            "file_hash": file_hash,
            "used_provider": used_provider,
            "needs_review": needs_review,
            "record": record,
        }
        if validation_metadata:
            success_result.update(validation_metadata)
        return success_result

    except DocumentRejectedError as exc:
        metrics.increment("documents_failed_total")
        payload = {
            "job_id": candidate.get("job_id"),
            "document_id": str(uuid4()),
            "drive_file_id": file_id,
            "file_hash": _sha256(local_path) if local_path.exists() else "",
            "status": "REJECTED",
            "error_code": exc.code,
            "error_message": str(exc),
        }
        dead_letter.write_failure(payload)
        if local_path.exists():
            claim_store.mark_status(file_id, payload["file_hash"], "REJECTED")
        return {
            "source_id": file_id,
            "status": "REJECTED",
            "error_code": exc.code,
            "error_message": str(exc),
        }
    except ExtractionError as exc:
        metrics.increment("documents_failed_total")
        dead_letter.write_failure(
            {
                "job_id": candidate.get("job_id"),
                "document_id": str(uuid4()),
                "drive_file_id": file_id,
                "file_hash": _sha256(local_path) if local_path.exists() else "",
                "status": "FAILED",
                "error_code": exc.code,
                "error_message": str(exc),
            }
        )
        if local_path.exists():
            claim_store.mark_status(file_id, _sha256(local_path), "FAILED")
        logger.exception("Extraction failed for source_id=%s", file_id)
        return {"source_id": file_id, "status": "FAILED", "error_code": exc.code, "error_message": str(exc)}
    except Exception as exc:  # noqa: BLE001
        metrics.increment("documents_failed_total")
        dead_letter.write_failure(
            {
                "job_id": candidate.get("job_id"),
                "document_id": str(uuid4()),
                "drive_file_id": file_id,
                "file_hash": _sha256(local_path) if local_path.exists() else "",
                "status": "FAILED",
                "error_code": "pipeline_error",
                "error_message": str(exc),
            }
        )
        if local_path.exists():
            claim_store.mark_status(file_id, _sha256(local_path), "FAILED")
        logger.exception("Pipeline failed for source_id=%s", file_id)
        return {"source_id": file_id, "status": "FAILED", "error_code": "pipeline_error", "error_message": str(exc)}
    finally:
        if local_path.exists():
            local_path.unlink(missing_ok=True)


def run_review_list(queue_dir: str) -> int:
    load_dotenv()
    settings = Settings.from_env()
    configure_logging(settings.log_level)

    items = list_review_items(queue_dir=queue_dir)
    active_items = [item for item in items if item.get("status") == "REVIEW_REQUIRED"]

    if not active_items:
        print("No active review items.")
        return 0

    for item in active_items:
        metadata = item.get("metadata", {}) if isinstance(item.get("metadata"), dict) else {}
        source_id = metadata.get("source_file_id") or metadata.get("drive_file_id") or "-"
        reasons = ",".join(item.get("reason_codes", [])) or "-"
        print(
            f"{item.get('document_id')} | source={source_id} | reasons={reasons} | "
            f"created_at={item.get('created_at_utc')}"
        )
    return 0


def run_review_resolve(document_id: str, *, queue_dir: str, record_path: str | None, note: str | None) -> int:
    load_dotenv()
    settings = Settings.from_env()
    configure_logging(settings.log_level)
    logger = logging.getLogger(__name__)

    resolution = resolve_review_item(
        document_id=document_id,
        queue_dir=queue_dir,
        record_path=record_path,
        note=note,
    )
    logger.info(
        "Resolved review item document_id=%s result=%s",
        document_id,
        resolution["storage_result"].get("status"),
    )
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Visual Invoice Processor")
    subparsers = parser.add_subparsers(dest="command", required=True)

    poll = subparsers.add_parser("poll-once", help="Run one poll cycle")
    _ = poll

    replay = subparsers.add_parser("replay", help="Replay dead-letter items")
    replay.add_argument("--status", required=True, choices=["FAILED", "REVIEW_REQUIRED"])
    replay.add_argument("--dead-letter-path", default="logs/dead_letter.jsonl")
    replay.add_argument("--audit-path", default="logs/replay_audit.jsonl")
    replay.add_argument("--claim-db-path", default="data/metadata.db")

    review_list = subparsers.add_parser("review-list", help="List active review queue items")
    review_list.add_argument("--queue-dir", default="review_queue")

    review_resolve = subparsers.add_parser("review-resolve", help="Resolve a review queue item into storage")
    review_resolve.add_argument("--document-id", required=True)
    review_resolve.add_argument("--queue-dir", default="review_queue")
    review_resolve.add_argument(
        "--record-path",
        default=None,
        help="Optional path to corrected JSON record. If omitted, uses metadata.normalized_record.",
    )
    review_resolve.add_argument("--note", default=None)
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    if args.command == "poll-once":
        return run_poll_once()
    if args.command == "replay":
        summary = replay_failures(
            status=args.status,
            dead_letter_path=args.dead_letter_path,
            audit_path=args.audit_path,
            claim_db_path=args.claim_db_path,
        )
        logging.getLogger(__name__).info(
            "Replay summary queued=%d skipped_processed=%d skipped_invalid=%d",
            summary["queued"],
            summary["skipped_processed"],
            summary["skipped_invalid"],
        )
        return 0
    if args.command == "review-list":
        return run_review_list(queue_dir=args.queue_dir)
    if args.command == "review-resolve":
        return run_review_resolve(
            document_id=args.document_id,
            queue_dir=args.queue_dir,
            record_path=args.record_path,
            note=args.note,
        )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
