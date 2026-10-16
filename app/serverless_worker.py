from __future__ import annotations

import logging
import os
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import unquote_plus

from app.alpha_store import AlphaQuotaError, AlphaStore
from app.config import Settings, load_dotenv
from app.logger import configure_logging
from app.normalization_engine import NormalizationRuleEngine
from app.object_storage_service import ObjectStorageService
from app.pipeline import DocumentRejectedError, Thresholds, process_document

logger = logging.getLogger(__name__)

PDF_LEADING_WHITESPACE_LIMIT = 16
PDF_LEADING_WHITESPACE = b" \t\n\f\r"


def inspect_document(path: Path, *, max_bytes: int, max_pdf_pages: int) -> tuple[str, int]:
    size = path.stat().st_size
    if size < 1 or size > max_bytes:
        raise DocumentRejectedError(
            f"File must be between 1 and {max_bytes} bytes",
            code="invalid_file_size",
        )
    with path.open("rb") as fh:
        header = fh.read(32)
    normalized_pdf_header = header.lstrip(PDF_LEADING_WHITESPACE)
    leading_whitespace = len(header) - len(normalized_pdf_header)
    if (
        leading_whitespace <= PDF_LEADING_WHITESPACE_LIMIT
        and normalized_pdf_header.startswith(b"%PDF-")
    ):
        try:
            from pypdf import PdfReader
        except ImportError as exc:
            raise RuntimeError("pypdf is required to validate PDF page limits") from exc
        try:
            pdf_bytes = path.read_bytes()[leading_whitespace:]
            page_count = len(PdfReader(BytesIO(pdf_bytes)).pages)
        except Exception as exc:  # noqa: BLE001
            raise DocumentRejectedError("The uploaded PDF is invalid", code="invalid_pdf") from exc
        if page_count < 1 or page_count > max_pdf_pages:
            raise DocumentRejectedError(
                f"PDFs may contain at most {max_pdf_pages} pages",
                code="page_limit_exceeded",
            )
        return "application/pdf", page_count
    if header.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png", 1
    if header.startswith(b"\xff\xd8\xff"):
        return "image/jpeg", 1
    raise DocumentRejectedError("The uploaded file signature is unsupported", code="unsupported_signature")


# How a pipeline result is recorded on the upload job; anything unexpected counts as a failure.
JOB_STATUS = {
    "STORED": "STORED",
    "REVIEW_REQUIRED": "REVIEW_REQUIRED",
    "SKIPPED_DUPLICATE": "DUPLICATE",
    "REJECTED": "REJECTED",
    "FAILED": "FAILED",
}


def _check_upload(path: Path, job: dict[str, Any], settings: Settings, store: AlphaStore) -> dict[str, Any]:
    """The file must be what the browser declared, and its pages must fit the shared allowance."""
    detected_type, page_count = inspect_document(
        path, max_bytes=settings.max_upload_bytes, max_pdf_pages=settings.max_pdf_pages
    )
    if detected_type != job["content_type"]:
        raise DocumentRejectedError("Declared content type does not match the file", code="content_type_mismatch")
    try:
        store.reserve_pages(job["id"], page_count, global_limit=settings.alpha_global_page_limit)
    except AlphaQuotaError as exc:
        raise DocumentRejectedError(str(exc), code="global_page_limit_exceeded") from exc
    return {"page_count": page_count}


def _finish_job(
    result: dict[str, Any], job_id: str, object_key: str, storage: ObjectStorageService, store: AlphaStore
) -> None:
    """Keeps the file only while someone may still need it, then records the outcome on the job."""
    status = JOB_STATUS.get(str(result.get("status")), "FAILED")
    try:
        if status == "REVIEW_REQUIRED":
            storage.move_to_archive(object_key)
        elif status in {"DUPLICATE", "REJECTED"}:
            storage.delete_object(object_key)
    except Exception:  # noqa: BLE001
        logger.exception("Could not clean up S3 object %s after status %s", object_key, status)
    store.complete_job(
        job_id, status=status, result=result, error_code=result.get("error_code"),
        error_message=result.get("error_message"),
    )
    logger.info("Completed S3 job %s with status %s", job_id, status)


def process_s3_object(
    object_key: str,
    *,
    store: AlphaStore | None = None,
    storage: ObjectStorageService | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    load_dotenv()
    active_settings = settings or Settings.from_env()
    if not object_key.startswith(active_settings.s3_inbox_prefix.rstrip("/") + "/"):
        return {"status": "IGNORED", "object_key": object_key}
    active_store = store or AlphaStore(active_settings.postgres_dsn or "")
    job = active_store.claim_job(object_key)
    if job is None:
        return {"status": "IGNORED", "object_key": object_key}
    active_storage = storage or ObjectStorageService.from_settings(active_settings)

    result = process_document(
        {"id": object_key, "name": job["original_name"], "job_id": job["id"], "org_id": job["org_id"],
         "base_currency": job["base_currency"]},
        settings=active_settings,
        storage=active_storage,
        store=active_store,
        normalization_engine=NormalizationRuleEngine.from_path(active_settings.normalization_rules_path),
        extraction_model=os.getenv("EXTRACTION_MODEL", "auto"),
        thresholds=Thresholds(
            review_confidence=float(os.getenv("REVIEW_CONFIDENCE_THRESHOLD", "0.5")),
            store_review_score=float(os.getenv("STORE_REVIEW_SCORE_THRESHOLD", "0.6")),
        ),
        worker_id=f"lambda:{job['id']}",
        before_extract=lambda path, _: _check_upload(path, job, active_settings, active_store),
    )
    _finish_job(result, job["id"], object_key, active_storage, active_store)
    return result


def handle_s3_event(event: dict[str, Any]) -> list[dict[str, Any]]:
    load_dotenv()
    configure_logging(os.getenv("LOG_LEVEL", "INFO"))
    results: list[dict[str, Any]] = []
    for record in event.get("Records", []):
        if record.get("eventSource") != "aws:s3":
            continue
        encoded_key = record.get("s3", {}).get("object", {}).get("key", "")
        if encoded_key:
            results.append(process_s3_object(unquote_plus(encoded_key)))
    return results
