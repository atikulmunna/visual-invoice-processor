from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any
from urllib.parse import unquote_plus

from app.alpha_store import AlphaQuotaError, AlphaStore
from app.config import Settings, load_dotenv
from app.logger import configure_logging
from app.main import DocumentRejectedError, _process_candidate
from app.metrics import MetricsCollector
from app.normalization_engine import NormalizationRuleEngine
from app.object_storage_service import ObjectStorageService


def inspect_document(path: Path, *, max_bytes: int, max_pdf_pages: int) -> tuple[str, int]:
    size = path.stat().st_size
    if size < 1 or size > max_bytes:
        raise DocumentRejectedError(
            f"File must be between 1 and {max_bytes} bytes",
            code="invalid_file_size",
        )
    with path.open("rb") as fh:
        header = fh.read(12)
    if header.startswith(b"%PDF-"):
        try:
            from pypdf import PdfReader
        except ImportError as exc:
            raise RuntimeError("pypdf is required to validate PDF page limits") from exc
        try:
            page_count = len(PdfReader(str(path)).pages)
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


def process_s3_object(
    object_key: str,
    *,
    store: AlphaStore | None = None,
    storage: ObjectStorageService | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    load_dotenv()
    active_settings = settings or Settings.from_env()
    if active_settings.ingestion_backend != "s3":
        raise ValueError("The serverless worker requires INGESTION_BACKEND=s3")
    inbox_prefix = active_settings.s3_inbox_prefix.rstrip("/") + "/"
    if not object_key.startswith(inbox_prefix):
        return {"status": "IGNORED", "object_key": object_key}

    active_store = store or AlphaStore(active_settings.postgres_dsn or "")
    job = active_store.claim_job(object_key)
    if job is None:
        return {"status": "IGNORED", "object_key": object_key}
    active_storage = storage or ObjectStorageService.from_settings(active_settings)
    job_id = job["id"]

    def before_extract(path: Path, _: str) -> dict[str, Any]:
        detected_type, page_count = inspect_document(
            path,
            max_bytes=active_settings.max_upload_bytes,
            max_pdf_pages=active_settings.max_pdf_pages,
        )
        if detected_type != job["content_type"]:
            raise DocumentRejectedError(
                "Declared content type does not match the file",
                code="content_type_mismatch",
            )
        try:
            active_store.reserve_pages(
                job_id,
                page_count,
                global_limit=active_settings.alpha_global_page_limit,
            )
        except AlphaQuotaError as exc:
            raise DocumentRejectedError(str(exc), code="global_page_limit_exceeded") from exc
        return {"page_count": page_count}

    metrics = MetricsCollector()
    normalization_engine = NormalizationRuleEngine.from_path(active_settings.normalization_rules_path)
    provider = os.getenv("EXTRACTION_PROVIDER", "mistral").strip().lower()
    if provider == "auto":
        provider = "mistral"
    candidate = {
        "id": object_key,
        "name": job["original_name"],
        "mimeType": job["content_type"],
        "size": str(job["declared_size"]),
        "job_id": job_id,
    }
    result = _process_candidate(
        candidate=candidate,
        settings=active_settings,
        backend=active_storage,
        claim_store=active_store,
        dead_letter=active_store,
        metrics=metrics,
        normalization_engine=normalization_engine,
        extraction_provider=provider,
        extraction_model=os.getenv("EXTRACTION_MODEL", "auto"),
        worker_id=f"lambda:{job_id}",
        review_threshold=float(os.getenv("REVIEW_CONFIDENCE_THRESHOLD", "0.5")),
        store_review_score_threshold=float(os.getenv("STORE_REVIEW_SCORE_THRESHOLD", "0.6")),
        archive_on_success=True,
        before_extract=before_extract,
    )
    status_map = {
        "STORED": "STORED",
        "REVIEW_REQUIRED": "REVIEW_REQUIRED",
        "SKIPPED_DUPLICATE": "DUPLICATE",
        "REJECTED": "REJECTED",
        "FAILED": "FAILED",
    }
    final_status = status_map.get(str(result.get("status")), "FAILED")
    try:
        if final_status == "REVIEW_REQUIRED":
            active_storage.move_to_archive(object_key)
        elif final_status in {"DUPLICATE", "REJECTED"}:
            active_storage.delete_object(object_key)
    except Exception:  # noqa: BLE001
        logging.getLogger(__name__).exception(
            "Could not clean up S3 object %s after status %s",
            object_key,
            final_status,
        )
    active_store.complete_job(
        job_id,
        status=final_status,
        result=result,
        error_code=result.get("error_code"),
        error_message=result.get("error_message"),
    )
    logging.getLogger(__name__).info("Completed S3 job %s with status %s", job_id, final_status)
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
