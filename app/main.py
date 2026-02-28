from __future__ import annotations

import argparse
import hashlib
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from app.auth import get_google_credentials
from app.config import Settings, load_dotenv
from app.dead_letter import DeadLetterStore
from app.drive_service import DriveService
from app.extraction_service import ExtractionError, extract_document
from app.idempotency_store import DocumentClaimStore
from app.logger import configure_logging
from app.metrics import JsonlMetricsSink, MetricsCollector
from app.r2_service import R2Service
from app.review_queue import decide_review_status, route_to_review_queue
from app.replay import replay_failures
from app.storage_service import append_record
from app.validation import validate_and_score
from pydantic import ValidationError

_TMP_DIR = Path("tmp")


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
    assert isinstance(backend, R2Service)
    return backend.download_file(object_key=file_id, out_path=out_path)


def _archive_candidate(settings: Settings, backend: object, candidate: dict[str, str]) -> None:
    if settings.ingestion_backend == "r2":
        assert isinstance(backend, R2Service)
        backend.move_to_archive(object_key=candidate["id"])


def _pick(data: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in data and data[key] not in (None, ""):
            return data[key]
    return default


def _normalize_date(value: Any) -> str | None:
    if not value:
        return None
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _extract_date_from_ocr_text(text: str) -> str | None:
    candidates = re.findall(r"\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}[/-]\d{1,2}[/-]\d{1,2})\b", text)
    for candidate in candidates:
        normalized = _normalize_date(candidate)
        if normalized:
            return normalized
    return None


def _normalize_payment_method(value: Any) -> str:
    text = str(value or "").strip().lower()
    if "card" in text:
        return "card"
    if "cash" in text:
        return "cash"
    if "bank" in text or "transfer" in text:
        return "bank"
    return "unknown"


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_line_items(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    items: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        quantity = _safe_float(_pick(item, "quantity", "qty"), 1.0)
        unit_price = _safe_float(_pick(item, "unit_price", "price"), 0.0)
        line_total = _safe_float(_pick(item, "line_total", "total"), quantity * unit_price)
        items.append(
            {
                "description": str(_pick(item, "description", "name", "title", default="item")).strip(),
                "quantity": max(quantity, 0.0001),
                "unit_price": max(unit_price, 0.0),
                "line_total": max(line_total, 0.0),
                "category": _pick(item, "category"),
            }
        )
    return items


def _line_items_have_amounts(items: list[dict[str, Any]]) -> bool:
    return any(_safe_float(item.get("line_total"), 0.0) > 0 for item in items)


def _extract_line_items_from_ocr_text(text: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for line in text.splitlines():
        compact = line.strip()
        if len(compact) < 8:
            continue
        # Common line format: "<desc> <qty> <unit_price> <line_total>"
        m = re.match(
            r"^(?P<desc>.+?)\s+(?P<qty>\d+(?:\.\d+)?)\s+(?P<unit>\d[\d,]*(?:\.\d+)?)\s+(?P<total>\d[\d,]*(?:\.\d+)?)$",
            compact,
        )
        if not m:
            # Fallback: "<desc> ... <line_total>"
            m2 = re.match(r"^(?P<desc>.+?)\s+(?P<total>\d[\d,]*(?:\.\d+)?)$", compact)
            if not m2:
                continue
            desc = m2.group("desc").strip()
            total = _safe_float(m2.group("total").replace(",", ""), 0.0)
            if total <= 0:
                continue
            items.append(
                {
                    "description": desc,
                    "quantity": 1.0,
                    "unit_price": total,
                    "line_total": total,
                    "category": None,
                }
            )
            continue

        desc = m.group("desc").strip()
        qty = _safe_float(m.group("qty"), 1.0)
        unit = _safe_float(m.group("unit").replace(",", ""), 0.0)
        total = _safe_float(m.group("total").replace(",", ""), qty * unit)
        if total <= 0:
            continue
        items.append(
            {
                "description": desc,
                "quantity": max(qty, 0.0001),
                "unit_price": max(unit, 0.0),
                "line_total": max(total, 0.0),
                "category": None,
            }
        )
    return items


def _coerce_extraction_payload(raw: dict[str, Any]) -> dict[str, Any]:
    ocr_text = str(_pick(raw, "_ocr_text", default="") or "")
    total = _safe_float(_pick(raw, "total_amount", "total", "order_total", "grand_total"), 0.0)
    subtotal = _safe_float(_pick(raw, "subtotal", "sub_total"), total)
    tax_amount = _safe_float(_pick(raw, "tax_amount", "tax", "vat"), max(total - subtotal, 0.0))

    confidence = _safe_float(_pick(raw, "model_confidence", "confidence", "confidence_score"), 0.8)
    confidence = max(0.0, min(confidence, 1.0))

    normalized_date = _normalize_date(_pick(raw, "invoice_date", "order_date", "date"))
    if not normalized_date and ocr_text:
        normalized_date = _extract_date_from_ocr_text(ocr_text)

    line_items = _normalize_line_items(_pick(raw, "line_items", "items", "products", default=[]))
    if (not line_items or not _line_items_have_amounts(line_items)) and ocr_text:
        recovered = _extract_line_items_from_ocr_text(ocr_text)
        if recovered:
            line_items = recovered

    payload = {
        "document_type": str(_pick(raw, "document_type", default="invoice")).lower(),
        "vendor_name": _pick(raw, "vendor_name", "vendor", "merchant_name", default="Unknown Vendor"),
        "vendor_tax_id": _pick(raw, "vendor_tax_id", "tax_id", "vat_id"),
        "invoice_number": _pick(raw, "invoice_number", "order_id", "invoice_id"),
        "invoice_date": normalized_date or datetime.now(
            timezone.utc
        ).strftime("%Y-%m-%d"),
        "due_date": _normalize_date(_pick(raw, "due_date")),
        "currency": str(_pick(raw, "currency", default="BDT")).upper(),
        "subtotal": max(subtotal, 0.0),
        "tax_amount": max(tax_amount, 0.0),
        "total_amount": max(total, 0.0),
        "payment_method": _normalize_payment_method(_pick(raw, "payment_method")),
        "line_items": line_items,
        "model_confidence": confidence,
        "validation_score": confidence,
    }
    if payload["document_type"] not in {"invoice", "receipt"}:
        payload["document_type"] = "invoice"
    if len(payload["currency"]) != 3:
        payload["currency"] = "BDT"
    return payload


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
        r2 = R2Service.from_settings(settings)
        backend = r2
        files = r2.list_inbox_files()

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

    extraction_provider = os.getenv("EXTRACTION_PROVIDER", "auto")
    extraction_model = os.getenv("EXTRACTION_MODEL", "auto")
    worker_id = os.getenv("WORKER_ID", "poll-once")
    review_threshold = float(os.getenv("REVIEW_CONFIDENCE_THRESHOLD", "0.5"))
    store_review_score_threshold = float(os.getenv("STORE_REVIEW_SCORE_THRESHOLD", "0.6"))

    for candidate in files:
        metrics.increment("documents_processed_total")
        file_id = candidate["id"]
        file_name = candidate.get("name", "document")
        local_path = _TMP_DIR / f"{uuid4().hex}_{file_name}"

        try:
            _download_candidate(settings, backend, candidate, local_path)
            file_hash = _sha256(local_path)
            claim = claim_store.claim_document(file_id, file_hash, owner_id=worker_id)
            if claim.status != "claimed":
                metrics.increment("documents_duplicate_skipped_total")
                continue

            document_id = str(uuid4())
            extracted = extract_document(
                file_path=local_path,
                provider=extraction_provider,
                model_name=extraction_model,
            )
            used_provider = str(extracted.get("_provider", "unknown"))
            logger.info("Extraction provider=%s source_id=%s", used_provider, file_id)
            normalized_payload = _coerce_extraction_payload(extracted)
            try:
                validation = validate_and_score(normalized_payload)
            except ValidationError as exc:
                route_to_review_queue(
                    document_id=document_id,
                    reason_codes=["schema_validation_failed"],
                    metadata={
                        "source_file_id": file_id,
                        "file_hash": file_hash,
                        "error": str(exc),
                        "raw_extracted": extracted,
                        "used_provider": used_provider,
                    },
                )
                dead_letter.write_failure(
                    {
                        "document_id": document_id,
                        "drive_file_id": file_id,
                        "file_hash": file_hash,
                        "status": "REVIEW_REQUIRED",
                        "error_code": "schema_validation_failed",
                        "error_message": str(exc),
                        "used_provider": used_provider,
                    }
                )
                claim_store.mark_status(file_id, file_hash, "REVIEW_REQUIRED")
                metrics.increment("documents_review_total")
                logger.info("Document %s sent to review due to schema mismatch", document_id)
                continue
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
                        "violations": validation["violations"],
                        "used_provider": used_provider,
                    },
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
                continue

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
            }
            append_result = append_record(record=record, metadata=metadata)
            claim_store.mark_status(file_id, file_hash, "STORED")
            _archive_candidate(settings, backend, candidate)
            claim_store.mark_status(file_id, file_hash, "ARCHIVED")
            metrics.increment("documents_success_total")
            logger.info(
                "Stored document_id=%s source_id=%s result=%s",
                document_id,
                file_id,
                append_result.get("status"),
            )

        except ExtractionError as exc:
            metrics.increment("documents_failed_total")
            dead_letter.write_failure(
                {
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
        except Exception as exc:  # noqa: BLE001
            metrics.increment("documents_failed_total")
            dead_letter.write_failure(
                {
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
        finally:
            if local_path.exists():
                local_path.unlink(missing_ok=True)

    snapshot = metrics.snapshot()
    for key, value in snapshot.items():
        if isinstance(value, int):
            metrics_sink.emit({"metric": key, "value": value, "stage": "poll_once"})
    logger.info("Poll summary: %s", snapshot)
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
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
