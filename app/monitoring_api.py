from __future__ import annotations

import html as html_lib
import json
import os
import secrets
from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi import Cookie, Depends, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel

from app.alpha_store import (
    AlphaAuthenticationError,
    AlphaNotFoundError,
    AlphaQuotaError,
    AlphaStore,
    AlphaUser,
)
from app.config import Settings, load_dotenv
from app.drive_service import is_supported_mime_type
from app.main import process_r2_object_now
from app.object_storage_service import ObjectStorageService
from app.r2_service import R2Service
from app.review_queue import dismiss_review_item, list_review_items, resolve_review_item


class ReviewResolveRequest(BaseModel):
    action: str = "approve"
    note: str | None = None
    corrected_record: dict[str, Any] | None = None


class PresignUploadRequest(BaseModel):
    filename: str
    content_type: str
    size: int


SESSION_COOKIE_NAME = "invoice_alpha_session"
SESSION_MAX_AGE_SECONDS = 7 * 24 * 60 * 60


def _build_auth_dependency(postgres_dsn: str | None = None):
    security = HTTPBasic(auto_error=False)

    def _auth(
        credentials: HTTPBasicCredentials | None = Depends(security),
        session_token: str | None = Cookie(default=None, alias=SESSION_COOKIE_NAME),
    ) -> str | AlphaUser:
        if (os.getenv("ALPHA_AUTH_ENABLED") or "").strip().lower() in {"1", "true", "yes", "on"}:
            if not postgres_dsn:
                raise HTTPException(
                    status_code=401,
                    detail="Unauthorized",
                )
            store = AlphaStore(postgres_dsn)
            if session_token:
                try:
                    return store.authenticate_session(session_token)
                except AlphaAuthenticationError:
                    pass
            if credentials is None:
                raise HTTPException(status_code=401, detail="Unauthorized")
            try:
                return store.authenticate(credentials.username, credentials.password)
            except AlphaAuthenticationError as exc:
                raise HTTPException(status_code=401, detail="Unauthorized") from exc

        username = (os.getenv("DASHBOARD_BASIC_AUTH_USERNAME") or "").strip()
        password = (os.getenv("DASHBOARD_BASIC_AUTH_PASSWORD") or "").strip()
        if not username and not password:
            return "anonymous"

        if credentials is None:
            raise HTTPException(
                status_code=401,
                detail="Unauthorized",
                headers={"WWW-Authenticate": "Basic"},
            )

        valid_user = secrets.compare_digest(credentials.username, username)
        valid_pass = secrets.compare_digest(credentials.password, password)
        if not (valid_user and valid_pass):
            raise HTTPException(
                status_code=401,
                detail="Unauthorized",
                headers={"WWW-Authenticate": "Basic"},
            )
        return credentials.username

    return _auth


def create_monitoring_app(
    *,
    metrics_path: str | Path = "logs/metrics.jsonl",
    dead_letter_path: str | Path = "logs/dead_letter.jsonl",
    review_queue_dir: str | Path = "review_queue",
    postgres_dsn: str | None = None,
) -> FastAPI:
    app = FastAPI(title="Invoice Processor Monitoring API", version="0.1.0")
    active_postgres_dsn = postgres_dsn or os.getenv("POSTGRES_DSN")
    require_dashboard_auth = _build_auth_dependency(active_postgres_dsn)

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/", include_in_schema=False)
    def root() -> RedirectResponse:
        if (os.getenv("ALPHA_AUTH_ENABLED") or "").strip().lower() in {"1", "true", "yes", "on"}:
            return RedirectResponse(url="/login", status_code=307)
        return RedirectResponse(url="/dashboard", status_code=307)

    @app.get("/login", response_class=HTMLResponse, include_in_schema=False)
    def login_page(
        session_token: str | None = Cookie(default=None, alias=SESSION_COOKIE_NAME),
    ) -> Response:
        if session_token and active_postgres_dsn:
            try:
                AlphaStore(active_postgres_dsn).authenticate_session(session_token)
                return RedirectResponse(url="/dashboard", status_code=307)
            except AlphaAuthenticationError:
                pass
        return HTMLResponse(_login_html())

    @app.post("/login", response_class=HTMLResponse, include_in_schema=False)
    def create_login_session(
        username: str = Form(...),
        password: str = Form(...),
    ) -> Response:
        if not active_postgres_dsn:
            return HTMLResponse(_login_html("Authentication is not configured."), status_code=503)
        store = AlphaStore(active_postgres_dsn)
        try:
            user = store.authenticate(username, password)
        except AlphaAuthenticationError:
            return HTMLResponse(_login_html("The username or password is incorrect."), status_code=401)
        token = store.create_session(user)
        response = RedirectResponse(url="/dashboard", status_code=303)
        response.set_cookie(
            key=SESSION_COOKIE_NAME,
            value=token,
            max_age=SESSION_MAX_AGE_SECONDS,
            httponly=True,
            secure=True,
            samesite="lax",
            path="/",
        )
        return response

    @app.post("/logout", include_in_schema=False)
    def logout(
        session_token: str | None = Cookie(default=None, alias=SESSION_COOKIE_NAME),
    ) -> RedirectResponse:
        if session_token and active_postgres_dsn:
            AlphaStore(active_postgres_dsn).delete_session(session_token)
        response = RedirectResponse(url="/login", status_code=303)
        response.delete_cookie(SESSION_COOKIE_NAME, path="/", secure=True, samesite="lax")
        return response

    @app.get("/stats")
    def stats(_: str = Depends(require_dashboard_auth)) -> dict[str, Any]:
        metric_events = _read_jsonl(metrics_path)
        resolved_hashes = _resolved_file_hashes(active_postgres_dsn)
        dead_letters = _active_dead_letters(dead_letter_path, resolved_hashes)
        queue_size = _active_review_queue_size(review_queue_dir, resolved_hashes)
        counters = _aggregate_metrics(metric_events)
        counters["dead_letter_total"] = len(dead_letters)
        counters["review_queue_total"] = queue_size
        return counters

    @app.get("/failures")
    def failures(limit: int = 50, _: str = Depends(require_dashboard_auth)) -> dict[str, Any]:
        items = _read_jsonl(dead_letter_path)
        return {"count": len(items), "items": items[-limit:]}

    @app.get("/backlog")
    def backlog(_: str = Depends(require_dashboard_auth)) -> dict[str, Any]:
        resolved_hashes = _resolved_file_hashes(active_postgres_dsn)
        queue_size = _active_review_queue_size(review_queue_dir, resolved_hashes)
        dead_letters = len(_active_dead_letters(dead_letter_path, resolved_hashes))
        return {
            "review_queue_total": queue_size,
            "dead_letter_total": dead_letters,
            "attention_total": queue_size + dead_letters,
        }

    @app.get("/dashboard/data")
    def dashboard_data(limit: int = 20, _: str = Depends(require_dashboard_auth)) -> dict[str, Any]:
        data = _query_dashboard_data(active_postgres_dsn, limit=limit)
        resolved_hashes = _resolved_file_hashes(active_postgres_dsn)
        review_items = _active_review_items(review_queue_dir, resolved_hashes)
        review_history = _review_history_items(review_queue_dir, limit=limit)
        dead_letters = _active_dead_letters(dead_letter_path, resolved_hashes)
        data["review_queue_total"] = _active_review_queue_size(review_queue_dir, resolved_hashes)
        data["dead_letter_total"] = len(dead_letters)
        data["activity_feed"] = _activity_feed_items(
            recent_records=data.get("recent_records", []),
            review_items=review_items,
            review_history=review_history,
            dead_letters=dead_letters,
            limit=limit,
        )
        return data

    @app.get("/review-items")
    def review_items(_: str = Depends(require_dashboard_auth)) -> dict[str, Any]:
        resolved_hashes = _resolved_file_hashes(active_postgres_dsn)
        items = _active_review_items(review_queue_dir, resolved_hashes)
        return {"count": len(items), "items": items}

    @app.get("/review-history")
    def review_history(limit: int = 20, _: str = Depends(require_dashboard_auth)) -> dict[str, Any]:
        items = _review_history_items(review_queue_dir, limit=limit)
        return {"count": len(items), "items": items}

    @app.post("/review-items/{document_id}/resolve")
    def review_resolve(
        document_id: str,
        payload: ReviewResolveRequest | None = None,
        _: str = Depends(require_dashboard_auth),
    ) -> dict[str, Any]:
        try:
            action = (payload.action if payload else "approve").strip().lower()
            if action == "approve":
                result = resolve_review_item(
                    document_id=document_id,
                    queue_dir=review_queue_dir,
                    record_override=payload.corrected_record if payload else None,
                    note=payload.note if payload else None,
                )
            elif action == "reject":
                result = dismiss_review_item(
                    document_id=document_id,
                    queue_dir=review_queue_dir,
                    resolution_status="REJECTED",
                    note=payload.note if payload else None,
                )
            elif action == "duplicate":
                result = dismiss_review_item(
                    document_id=document_id,
                    queue_dir=review_queue_dir,
                    resolution_status="RESOLVED_DUPLICATE_MANUAL",
                    note=payload.note if payload else None,
                )
            else:
                raise ValueError(f"Unsupported review action: {action}")
            return {
                "status": "ok",
                "document_id": document_id,
                "action": action,
                "storage_result": result["storage_result"],
                "review_status": result["review_item"].get("status"),
            }
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/upload")
    async def upload_and_process(
        file: UploadFile = File(...),
        _: str = Depends(require_dashboard_auth),
    ) -> dict[str, Any]:
        load_dotenv()
        settings = Settings.from_env()
        if settings.ingestion_backend == "s3":
            raise HTTPException(status_code=410, detail="Use /uploads/presign for S3 uploads")
        if settings.ingestion_backend != "r2":
            raise HTTPException(status_code=400, detail="Dashboard upload requires INGESTION_BACKEND=r2")

        content_type = file.content_type or "application/octet-stream"
        if not is_supported_mime_type(content_type, settings.allowed_mime_types):
            raise HTTPException(status_code=400, detail=f"Unsupported file type: {content_type}")

        original_name = Path(file.filename or f"upload-{uuid4().hex}.bin").name
        content = await file.read()
        if not content:
            raise HTTPException(status_code=400, detail="Uploaded file is empty")

        inbox_prefix = settings.r2_inbox_prefix.rstrip("/")
        object_key = f"{inbox_prefix}/{uuid4().hex}_{original_name}"

        try:
            r2_service = R2Service.from_settings(settings)
            r2_service.upload_bytes(object_key, content, content_type=content_type)
            result = process_r2_object_now(
                {
                    "id": object_key,
                    "name": original_name,
                    "mimeType": content_type,
                    "size": str(len(content)),
                }
            )
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        return {
            "status": "ok",
            "uploaded_object_key": object_key,
            "processing_result": result,
        }

    @app.post("/uploads/presign")
    def presign_upload(
        payload: PresignUploadRequest,
        principal: str | AlphaUser = Depends(require_dashboard_auth),
    ) -> dict[str, Any]:
        load_dotenv()
        settings = Settings.from_env()
        if settings.ingestion_backend != "s3":
            raise HTTPException(status_code=400, detail="Presigned uploads require INGESTION_BACKEND=s3")
        if not isinstance(principal, AlphaUser):
            raise HTTPException(status_code=403, detail="Private-alpha authentication is required")
        if payload.size < 1 or payload.size > settings.max_upload_bytes:
            raise HTTPException(status_code=400, detail=f"File must be between 1 and {settings.max_upload_bytes} bytes")
        if not is_supported_mime_type(payload.content_type, settings.allowed_mime_types):
            raise HTTPException(status_code=400, detail=f"Unsupported file type: {payload.content_type}")

        original_name = Path(payload.filename).name
        if not original_name:
            raise HTTPException(status_code=400, detail="A filename is required")
        job_id = str(uuid4())
        object_key = (
            f"{settings.s3_inbox_prefix.rstrip('/')}/{principal.id}/{job_id}/{original_name}"
        )
        try:
            store = AlphaStore(active_postgres_dsn or "")
            store.authorize_upload(
                principal,
                object_key=object_key,
                original_name=original_name,
                content_type=payload.content_type,
                declared_size=payload.size,
            )
            storage = ObjectStorageService.from_settings(settings)
            upload = storage.create_presigned_upload(
                object_key,
                content_type=payload.content_type,
                max_bytes=settings.max_upload_bytes,
                expires_seconds=300,
            )
        except AlphaQuotaError as exc:
            raise HTTPException(status_code=429, detail=str(exc)) from exc
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {
            "job_id": job_id,
            "object_key": object_key,
            "upload": upload,
            "expires_in": 300,
            "documents_remaining": max(principal.documents_remaining - 1, 0),
        }

    @app.get("/uploads/{job_id}")
    def upload_status(
        job_id: str,
        principal: str | AlphaUser = Depends(require_dashboard_auth),
    ) -> dict[str, Any]:
        if not isinstance(principal, AlphaUser):
            raise HTTPException(status_code=403, detail="Private-alpha authentication is required")
        try:
            return AlphaStore(active_postgres_dsn or "").get_job(job_id, user_id=principal.id)
        except AlphaNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/uploads/{job_id}/retry")
    def retry_upload(
        job_id: str,
        principal: str | AlphaUser = Depends(require_dashboard_auth),
    ) -> dict[str, str]:
        if not isinstance(principal, AlphaUser):
            raise HTTPException(status_code=403, detail="Private-alpha authentication is required")
        load_dotenv()
        settings = Settings.from_env()
        store = AlphaStore(active_postgres_dsn or "")
        try:
            object_key = store.retry_job(job_id, user_id=principal.id)
            ObjectStorageService.from_settings(settings).retrigger_object(object_key)
        except AlphaQuotaError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except Exception as exc:  # noqa: BLE001
            store.complete_job(
                job_id,
                status="FAILED",
                error_code="retry_trigger_failed",
                error_message=str(exc),
            )
            raise HTTPException(status_code=400, detail="Retry could not be scheduled") from exc
        return {"job_id": job_id, "status": "AUTHORIZED"}

    @app.get("/dashboard", response_class=HTMLResponse)
    def dashboard(principal: str | AlphaUser = Depends(require_dashboard_auth)) -> str:
        return _dashboard_html(os.getenv("INGESTION_BACKEND", "drive"), principal)

    return app


def _read_jsonl(path: str | Path) -> list[dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in p.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rows.append(json.loads(line))
    return rows


def _review_queue_size(path: str | Path) -> int:
    p = Path(path)
    if not p.exists():
        return 0
    return len([x for x in p.glob("*.json") if x.is_file()])


def _active_review_queue_size(path: str | Path, resolved_hashes: set[str]) -> int:
    p = Path(path)
    if not p.exists():
        return 0
    total = 0
    for file_path in p.glob("*.json"):
        if not file_path.is_file():
            continue
        try:
            payload = json.loads(file_path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            continue
        if payload.get("status") != "REVIEW_REQUIRED":
            continue
        metadata = payload.get("metadata", {}) if isinstance(payload.get("metadata"), dict) else {}
        file_hash = str(metadata.get("file_hash", "") or "")
        if file_hash and file_hash in resolved_hashes:
            continue
        total += 1
    return total


def _active_review_items(path: str | Path, resolved_hashes: set[str]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for payload in list_review_items(queue_dir=path):
        if payload.get("status") != "REVIEW_REQUIRED":
            continue
        metadata = payload.get("metadata", {}) if isinstance(payload.get("metadata"), dict) else {}
        file_hash = str(metadata.get("file_hash", "") or "")
        if file_hash and file_hash in resolved_hashes:
            continue
        normalized = metadata.get("normalized_record") if isinstance(metadata.get("normalized_record"), dict) else {}
        items.append(
            {
                "document_id": payload.get("document_id"),
                "status": payload.get("status"),
                "reason_codes": payload.get("reason_codes", []),
                "created_at_utc": payload.get("created_at_utc"),
                "source_file_id": metadata.get("source_file_id") or metadata.get("drive_file_id"),
                "file_hash": file_hash,
                "used_provider": metadata.get("used_provider", "unknown"),
                "vendor_name": normalized.get("vendor_name"),
                "invoice_number": normalized.get("invoice_number"),
                "invoice_date": normalized.get("invoice_date"),
                "currency": normalized.get("currency"),
                "total_amount": normalized.get("total_amount"),
                "normalized_record": normalized,
            }
        )
    return items


def _review_history_items(path: str | Path, *, limit: int = 20) -> list[dict[str, Any]]:
    history: list[dict[str, Any]] = []
    for payload in list_review_items(queue_dir=path):
        status = str(payload.get("status", "") or "")
        if status == "REVIEW_REQUIRED":
            continue
        metadata = payload.get("metadata", {}) if isinstance(payload.get("metadata"), dict) else {}
        resolved_record = payload.get("resolved_record") if isinstance(payload.get("resolved_record"), dict) else {}
        history.append(
            {
                "document_id": payload.get("document_id"),
                "status": status,
                "created_at_utc": payload.get("created_at_utc"),
                "resolved_at_utc": payload.get("resolved_at_utc"),
                "source_file_id": metadata.get("source_file_id") or metadata.get("drive_file_id"),
                "used_provider": metadata.get("used_provider", "unknown"),
                "vendor_name": resolved_record.get("vendor_name") or metadata.get("vendor_name") or "Unknown",
                "invoice_number": resolved_record.get("invoice_number") or "-",
                "total_amount": resolved_record.get("total_amount"),
                "currency": resolved_record.get("currency") or "NA",
                "resolution_note": payload.get("resolution_note"),
            }
        )
    history.sort(key=lambda item: str(item.get("resolved_at_utc") or item.get("created_at_utc") or ""), reverse=True)
    return history[:limit]


def _activity_feed_items(
    *,
    recent_records: list[dict[str, Any]],
    review_items: list[dict[str, Any]],
    review_history: list[dict[str, Any]],
    dead_letters: list[dict[str, Any]],
    limit: int = 20,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []

    for record in recent_records:
        status = "STORED_REVIEW_FLAG" if record.get("needs_review") else "STORED"
        events.append(
            {
                "recorded_at_utc": record.get("processed_at_utc"),
                "status": status,
                "document_id": record.get("document_id"),
                "source_file_id": record.get("drive_file_id"),
                "vendor_name": record.get("vendor_name"),
                "invoice_number": record.get("invoice_number"),
                "currency": record.get("currency"),
                "total_amount": record.get("total_amount"),
                "used_provider": record.get("used_provider"),
                "message": "Stored in ledger" if status == "STORED" else "Stored with needs_review flag",
            }
        )

    for item in review_items:
        reason_text = ", ".join(item.get("reason_codes", [])) or "routed to review"
        events.append(
            {
                "recorded_at_utc": item.get("created_at_utc"),
                "status": "REVIEW_REQUIRED",
                "document_id": item.get("document_id"),
                "source_file_id": item.get("source_file_id"),
                "vendor_name": item.get("vendor_name"),
                "invoice_number": item.get("invoice_number"),
                "currency": item.get("currency"),
                "total_amount": item.get("total_amount"),
                "used_provider": item.get("used_provider"),
                "message": reason_text,
            }
        )

    for item in review_history:
        events.append(
            {
                "recorded_at_utc": item.get("resolved_at_utc") or item.get("created_at_utc"),
                "status": item.get("status"),
                "document_id": item.get("document_id"),
                "source_file_id": item.get("source_file_id"),
                "vendor_name": item.get("vendor_name"),
                "invoice_number": item.get("invoice_number"),
                "currency": item.get("currency"),
                "total_amount": item.get("total_amount"),
                "used_provider": item.get("used_provider"),
                "message": item.get("resolution_note") or "Review decision recorded",
            }
        )

    for item in dead_letters:
        if str(item.get("status", "") or "") != "FAILED":
            continue
        events.append(
            {
                "recorded_at_utc": item.get("recorded_at_utc"),
                "status": "FAILED",
                "document_id": item.get("document_id"),
                "source_file_id": item.get("drive_file_id"),
                "vendor_name": None,
                "invoice_number": None,
                "currency": None,
                "total_amount": None,
                "used_provider": item.get("used_provider", "unknown"),
                "message": item.get("error_message") or item.get("error_code") or "Processing failed",
            }
        )

    events.sort(key=lambda item: str(item.get("recorded_at_utc") or ""), reverse=True)
    return events[:limit]


def _aggregate_metrics(events: list[dict[str, Any]]) -> dict[str, Any]:
    counters: dict[str, int] = {}
    for event in events:
        name = event.get("metric")
        value = event.get("value")
        if isinstance(name, str) and isinstance(value, int):
            counters[name] = counters.get(name, 0) + value
    return counters


def _active_dead_letters(path: str | Path, resolved_hashes: set[str]) -> list[dict[str, Any]]:
    events = _read_jsonl(path)
    latest_by_key: dict[str, dict[str, Any]] = {}
    for event in events:
        status = str(event.get("status", "") or "")
        if status not in {"FAILED", "REVIEW_REQUIRED"}:
            continue
        file_hash = str(event.get("file_hash", "") or "")
        if file_hash and file_hash in resolved_hashes:
            continue
        key = (
            str(event.get("document_id", "") or "")
            or (str(event.get("drive_file_id", "") or "") + "|" + file_hash)
            or str(hash(json.dumps(event, sort_keys=True)))
        )
        latest_by_key[key] = event
    return list(latest_by_key.values())


def _resolved_file_hashes(postgres_dsn: str | None) -> set[str]:
    if not postgres_dsn:
        return set()
    try:
        import psycopg
    except ImportError:
        return set()

    try:
        with psycopg.connect(postgres_dsn, prepare_threshold=None) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select distinct file_hash
                    from public.ledger_records
                    where status in ('STORED', 'ARCHIVED')
                    """
                )
                return {str(row[0]) for row in cur.fetchall() if row and row[0]}
    except Exception:  # noqa: BLE001
        return set()


def _query_dashboard_data(postgres_dsn: str | None, *, limit: int) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "kpis": {
            "records_total": 0,
            "stored_total": 0,
            "needs_review_total": 0,
            "total_amount_display": "0",
        },
        "daily_summary": [],
        "currency_totals": [],
        "vendor_spend": [],
        "provider_mix": [],
        "recent_records": [],
        "activity_feed": [],
        "error": None,
    }
    if not postgres_dsn:
        payload["error"] = "POSTGRES_DSN not configured"
        return payload

    try:
        import psycopg
    except ImportError:
        payload["error"] = "psycopg not installed"
        return payload

    try:
        with psycopg.connect(postgres_dsn, prepare_threshold=None) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select
                      count(*)::int as records_total,
                      count(*) filter (where row_status = 'STORED')::int as stored_total,
                      count(*) filter (where needs_review = true)::int as needs_review_total
                    from public.ledger_records_flat
                    """
                )
                row = cur.fetchone()
                if row:
                    payload["kpis"] = {
                        "records_total": row[0],
                        "stored_total": row[1],
                        "needs_review_total": row[2],
                        "total_amount_display": "0",
                    }

                cur.execute(
                    """
                    select coalesce(currency, 'NA') as currency, coalesce(sum(total_amount), 0)::float as total_amount_sum
                    from public.ledger_records_flat
                    group by 1
                    order by total_amount_sum desc, currency asc
                    """
                )
                payload["currency_totals"] = [
                    {"currency": r[0], "total_amount_sum": r[1]}
                    for r in cur.fetchall()
                ]
                payload["kpis"]["total_amount_display"] = _format_currency_total_display(payload["currency_totals"])

                cur.execute(
                    """
                    select processing_date::text, records_total::int, stored_total::int, needs_review_total::int, coalesce(total_amount_sum,0)::float
                    from public.ledger_daily_summary
                    order by processing_date desc
                    limit 14
                    """
                )
                payload["daily_summary"] = [
                    {
                        "processing_date": r[0],
                        "records_total": r[1],
                        "stored_total": r[2],
                        "needs_review_total": r[3],
                        "total_amount_sum": r[4],
                    }
                    for r in cur.fetchall()
                ]

                cur.execute(
                    """
                    select coalesce(vendor_name, 'Unknown') as vendor_name, count(*)::int as invoices, coalesce(sum(total_amount), 0)::float as total_spend
                    from public.ledger_records_flat
                    group by 1
                    order by total_spend desc
                    limit 10
                    """
                )
                payload["vendor_spend"] = [
                    {"vendor_name": r[0], "invoices": r[1], "total_spend": r[2]}
                    for r in cur.fetchall()
                ]

                cur.execute(
                    """
                    select coalesce(used_provider, 'unknown') as used_provider, count(*)::int as records_total
                    from public.ledger_records_flat
                    group by 1
                    order by records_total desc
                    """
                )
                payload["provider_mix"] = [
                    {"used_provider": r[0], "records_total": r[1]} for r in cur.fetchall()
                ]

                cur.execute(
                    """
                    select
                      processed_at_utc::text,
                      coalesce(document_id, '') as document_id,
                      coalesce(drive_file_id, '') as drive_file_id,
                      coalesce(vendor_name, 'Unknown') as vendor_name,
                      coalesce(currency, 'NA') as currency,
                      coalesce(total_amount, 0)::float as total_amount,
                      coalesce(invoice_number, '-') as invoice_number,
                      coalesce(used_provider, 'unknown') as used_provider,
                      coalesce(needs_review, false) as needs_review
                    from public.ledger_records_flat
                    order by processed_at_utc desc
                    limit %s
                    """,
                    (limit,),
                )
                payload["recent_records"] = [
                    {
                        "processed_at_utc": r[0],
                        "document_id": r[1],
                        "drive_file_id": r[2],
                        "vendor_name": r[3],
                        "currency": r[4],
                        "total_amount": r[5],
                        "invoice_number": r[6],
                        "used_provider": r[7],
                        "needs_review": bool(r[8]),
                    }
                    for r in cur.fetchall()
                ]
    except Exception as exc:  # noqa: BLE001
        payload["error"] = str(exc)
    return payload


def _format_currency_total_display(currency_totals: list[dict[str, Any]]) -> str:
    if not currency_totals:
        return "0"
    if len(currency_totals) == 1:
        item = currency_totals[0]
        return f"{item.get('currency', 'NA')} {float(item.get('total_amount_sum', 0.0)):,.2f}"
    return f"{len(currency_totals)} currencies"


def _login_html(error: str | None = None) -> str:
    error_markup = ""
    if error:
        error_markup = f'<div class="error" role="alert">{html_lib.escape(error)}</div>'
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Sign in · Ledgerly</title>
  <style>
    :root {{
      --ink-950: #111110;
      --ink-900: #1d1c17;
      --graphite: #403e3a;
      --stone: #cbc5b9;
      --paper: #f4efe6;
      --accent: #f15a24;
      --accent-bright: #ff6b32;
      --danger: #ff8b75;
    }}
    * {{ box-sizing: border-box; }}
    html, body {{ min-height: 100%; }}
    body {{
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      padding: 24px;
      color: var(--paper);
      background:
        radial-gradient(circle at 78% 8%, rgba(241, 90, 36, 0.15), transparent 30rem),
        radial-gradient(circle at 12% 90%, rgba(203, 197, 185, 0.06), transparent 28rem),
        var(--ink-950);
      font-family: Inter, ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}
    .shell {{
      width: min(980px, 100%);
      min-height: 600px;
      display: grid;
      grid-template-columns: 1.08fr 0.92fr;
      overflow: hidden;
      border: 1px solid rgba(203, 197, 185, 0.14);
      border-radius: 24px;
      background: var(--ink-900);
      box-shadow: 0 34px 90px rgba(0, 0, 0, 0.42);
    }}
    .story {{
      position: relative;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
      padding: clamp(34px, 6vw, 64px);
      background:
        linear-gradient(145deg, rgba(203, 197, 185, 0.05), transparent 52%),
        var(--graphite);
    }}
    .story::after {{
      content: "";
      position: absolute;
      right: -70px;
      bottom: -110px;
      width: 260px;
      height: 260px;
      border: 54px solid rgba(241, 90, 36, 0.85);
      border-radius: 50%;
      pointer-events: none;
    }}
    .brand {{ position: relative; z-index: 1; display: flex; align-items: center; gap: 12px; }}
    .mark {{
      width: 44px;
      height: 44px;
      display: grid;
      place-items: center;
      border-radius: 12px;
      background: var(--accent);
      color: #161410;
      font-size: 1.1rem;
      font-weight: 900;
      letter-spacing: -0.06em;
    }}
    .brand strong {{ display: block; font-size: 1rem; }}
    .brand span {{ color: rgba(244, 239, 230, 0.56); font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.13em; }}
    .story-copy {{ position: relative; z-index: 1; max-width: 470px; margin: 72px 0; }}
    .eyebrow {{ margin: 0 0 15px; color: var(--accent); font-size: 0.7rem; font-weight: 750; text-transform: uppercase; letter-spacing: 0.16em; }}
    h1 {{ margin: 0; font-size: clamp(2.3rem, 5vw, 4.4rem); line-height: 0.98; letter-spacing: -0.06em; }}
    .story-copy p:last-child {{ max-width: 410px; margin: 22px 0 0; color: rgba(244, 239, 230, 0.65); font-size: 0.92rem; line-height: 1.65; }}
    .secure {{ position: relative; z-index: 1; color: rgba(244, 239, 230, 0.48); font-size: 0.72rem; }}
    .secure::before {{ content: ""; display: inline-block; width: 7px; height: 7px; margin-right: 8px; border-radius: 50%; background: #8fbf91; }}
    .login {{ display: flex; flex-direction: column; justify-content: center; padding: clamp(34px, 6vw, 64px); }}
    .login h2 {{ margin: 0; font-size: 1.65rem; letter-spacing: -0.04em; }}
    .login-intro {{ margin: 9px 0 30px; color: rgba(203, 197, 185, 0.58); font-size: 0.82rem; line-height: 1.6; }}
    label {{ display: block; margin: 0 0 8px; color: rgba(203, 197, 185, 0.66); font-size: 0.7rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.11em; }}
    input {{
      width: 100%;
      height: 48px;
      margin-bottom: 19px;
      padding: 0 14px;
      border: 1px solid rgba(203, 197, 185, 0.18);
      border-radius: 10px;
      background: rgba(17, 17, 16, 0.52);
      color: var(--paper);
      font: inherit;
      transition: border-color 150ms ease, box-shadow 150ms ease;
    }}
    input:focus {{ outline: 0; border-color: var(--accent); box-shadow: 0 0 0 3px rgba(241, 90, 36, 0.13); }}
    button {{
      width: 100%;
      height: 49px;
      margin-top: 4px;
      border: 0;
      border-radius: 10px;
      background: var(--accent);
      color: #161410;
      font: 750 0.84rem/1 Inter, ui-sans-serif, sans-serif;
      cursor: pointer;
      transition: background 150ms ease, transform 150ms ease;
    }}
    button:hover {{ background: var(--accent-bright); transform: translateY(-1px); }}
    button:focus-visible {{ outline: 3px solid rgba(241, 90, 36, 0.32); outline-offset: 3px; }}
    .error {{ margin: -8px 0 20px; padding: 11px 12px; border: 1px solid rgba(255, 139, 117, 0.25); border-radius: 9px; background: rgba(255, 139, 117, 0.08); color: var(--danger); font-size: 0.76rem; }}
    .help {{ margin: 20px 0 0; color: rgba(203, 197, 185, 0.42); font-size: 0.7rem; text-align: center; line-height: 1.5; }}
    @media (max-width: 760px) {{
      body {{ padding: 0; place-items: stretch; }}
      .shell {{ min-height: 100vh; grid-template-columns: 1fr; border: 0; border-radius: 0; }}
      .story {{ min-height: 285px; padding: 28px; }}
      .story-copy {{ margin: 46px 0 22px; }}
      .story-copy p:last-child {{ display: none; }}
      .secure {{ display: none; }}
      .login {{ padding: 38px 28px 48px; }}
    }}
  </style>
</head>
<body>
  <main class="shell">
    <section class="story">
      <div class="brand"><span class="mark">L</span><div><strong>Ledgerly</strong><span>Invoice intelligence</span></div></div>
      <div class="story-copy"><p class="eyebrow">Private alpha</p><h1>Every invoice, understood.</h1><p>Upload documents directly to encrypted storage and turn them into structured, reviewable records in one secure workspace.</p></div>
      <div class="secure">Protected by encrypted sessions and private storage</div>
    </section>
    <section class="login">
      <h2>Welcome back</h2>
      <p class="login-intro">Sign in with the private-alpha credentials provided to you.</p>
      {error_markup}
      <form method="post" action="/login">
        <label for="username">Username</label>
        <input id="username" name="username" type="text" autocomplete="username" required autofocus />
        <label for="password">Password</label>
        <input id="password" name="password" type="password" autocomplete="current-password" required />
        <button type="submit">Enter workspace</button>
      </form>
      <p class="help">Access is limited to approved testers. Sessions expire automatically after seven days.</p>
    </section>
  </main>
</body>
</html>"""


def _dashboard_html(upload_mode: str = "r2", principal: str | AlphaUser = "Operator") -> str:
    html = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Invoice Operations Dashboard · Ledgerly</title>
  <style>
    :root {
      --c-light: #c1c1c1;
      --c-ink: #2c4251;
      --c-warn: #d16666;
      --c-good: #b6c649;
      --c-white: #ffffff;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Segoe UI", Tahoma, sans-serif;
      color: var(--c-ink);
      background:
        radial-gradient(circle at 15% 20%, rgba(182,198,73,0.12) 0, transparent 45%),
        radial-gradient(circle at 85% 5%, rgba(209,102,102,0.10) 0, transparent 35%),
        var(--c-white);
    }
    .wrap { max-width: 1160px; margin: 0 auto; padding: 28px 16px 40px; }
    .head {
      display: flex; align-items: baseline; justify-content: space-between; gap: 8px; margin-bottom: 18px;
      border-bottom: 1px solid rgba(44,66,81,0.15); padding-bottom: 10px;
    }
    .head h1 { margin: 0; font-size: 1.25rem; letter-spacing: 0.3px; }
    .muted { color: rgba(44,66,81,0.75); font-size: 0.9rem; }
    .grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 14px;
    }
    .card {
      background: var(--c-white);
      border: 1px solid rgba(44,66,81,0.16);
      border-radius: 10px;
      padding: 12px;
      box-shadow: 0 3px 12px rgba(44,66,81,0.06);
    }
    .card h3 { margin: 0 0 6px; font-size: 0.8rem; color: rgba(44,66,81,0.8); font-weight: 600; }
    .value { font-size: 1.3rem; font-weight: 700; color: var(--c-ink); }
    .value.good { color: #5f7421; }
    .value.warn { color: #9f4040; }
    .pane-grid {
      display: grid;
      grid-template-columns: 1.2fr 0.8fr;
      gap: 12px;
      margin-bottom: 12px;
    }
    .table-wrap { overflow: auto; max-height: 380px; }
    table { width: 100%; border-collapse: collapse; font-size: 0.9rem; }
    th, td { text-align: left; padding: 8px 7px; border-bottom: 1px solid rgba(44,66,81,0.08); white-space: nowrap; }
    th { font-size: 0.78rem; color: rgba(44,66,81,0.76); text-transform: uppercase; letter-spacing: 0.05em; }
    .tag {
      display: inline-block;
      border-radius: 999px;
      padding: 2px 8px;
      font-size: 0.74rem;
      font-weight: 600;
      border: 1px solid rgba(44,66,81,0.2);
      color: var(--c-ink);
      background: rgba(193,193,193,0.2);
    }
    .tag.good { border-color: rgba(182,198,73,0.6); background: rgba(182,198,73,0.22); }
    .tag.warn { border-color: rgba(209,102,102,0.6); background: rgba(209,102,102,0.22); }
    .bar-list { display: flex; flex-direction: column; gap: 8px; }
    .bar-row { display: grid; grid-template-columns: 90px 1fr 54px; gap: 8px; align-items: center; font-size: 0.86rem; }
    .bar-track { height: 10px; border-radius: 99px; background: rgba(193,193,193,0.35); overflow: hidden; }
    .bar-fill { height: 100%; background: linear-gradient(90deg, #2c4251, #b6c649); }
    .warn-box {
      margin-top: 10px; border: 1px solid rgba(209,102,102,0.35); background: rgba(209,102,102,0.08);
      color: #7d2f2f; border-radius: 8px; padding: 10px; font-size: 0.86rem;
    }
    .action-btn {
      border: 1px solid rgba(182,198,73,0.7);
      background: rgba(182,198,73,0.18);
      color: var(--c-ink);
      border-radius: 999px;
      padding: 6px 10px;
      font-size: 0.78rem;
      cursor: pointer;
    }
    .action-btn:hover { background: rgba(182,198,73,0.3); }
    .action-btn.warn {
      border-color: rgba(209,102,102,0.7);
      background: rgba(209,102,102,0.14);
    }
    .json-preview {
      margin-top: 8px;
      padding: 10px;
      border-radius: 8px;
      background: rgba(44,66,81,0.05);
      font-family: Consolas, monospace;
      font-size: 0.76rem;
      white-space: pre-wrap;
      word-break: break-word;
    }
    .editor-wrap { margin-top: 8px; display: grid; gap: 8px; }
    .editor-wrap textarea {
      width: 100%;
      min-height: 220px;
      resize: vertical;
      border-radius: 8px;
      border: 1px solid rgba(44,66,81,0.18);
      background: rgba(44,66,81,0.03);
      padding: 10px;
      font-family: Consolas, monospace;
      font-size: 0.76rem;
      color: var(--c-ink);
    }
    .editor-actions { display: flex; gap: 8px; flex-wrap: wrap; }
    .history-grid {
      display: grid;
      grid-template-columns: 1fr;
      gap: 12px;
      margin-top: 12px;
    }
    .toolbar {
      display: flex;
      justify-content: flex-end;
      margin: 8px 0 10px;
    }
    .toolbar input {
      width: min(320px, 100%);
      border-radius: 999px;
      border: 1px solid rgba(44,66,81,0.18);
      background: rgba(44,66,81,0.03);
      padding: 8px 12px;
      font-size: 0.84rem;
      color: var(--c-ink);
    }
    .upload-card {
      display: grid;
      grid-template-columns: 1.2fr auto;
      gap: 12px;
      align-items: end;
      margin-bottom: 14px;
    }
    .upload-controls {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
    }
    .upload-controls input[type="file"] {
      max-width: 100%;
      font-size: 0.84rem;
    }
    .upload-status {
      min-height: 1.2rem;
      font-size: 0.84rem;
      color: rgba(44,66,81,0.82);
    }
    .feed-list {
      display: grid;
      gap: 10px;
    }
    .feed-item {
      border: 1px solid rgba(44,66,81,0.12);
      border-radius: 10px;
      padding: 10px;
      background: rgba(44,66,81,0.03);
    }
    .feed-head {
      display: flex;
      justify-content: space-between;
      gap: 8px;
      align-items: center;
      margin-bottom: 6px;
      flex-wrap: wrap;
    }
    .feed-title {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
      font-size: 0.86rem;
      font-weight: 600;
    }
    .feed-meta {
      font-size: 0.8rem;
      color: rgba(44,66,81,0.78);
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
    }
    .amount-stack {
      display: grid;
      gap: 4px;
    }
    .amount-breakdown {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      margin-top: 6px;
    }
    .mini-tag {
      display: inline-block;
      border-radius: 999px;
      padding: 2px 8px;
      font-size: 0.72rem;
      border: 1px solid rgba(44,66,81,0.18);
      background: rgba(44,66,81,0.04);
      color: rgba(44,66,81,0.86);
    }
    @media (max-width: 920px) {
      .grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .pane-grid { grid-template-columns: 1fr; }
      .upload-card { grid-template-columns: 1fr; }
    }
    @media (max-width: 540px) {
      .grid { grid-template-columns: 1fr; }
    }

    /* Private alpha workspace — palette supplied by the product owner. */
    :root {
      --ink-950: #111110;
      --ink-900: #1d1c17;
      --ink-850: #242321;
      --ink-800: #2d2c29;
      --graphite: #403e3a;
      --stone: #cbc5b9;
      --paper: #f4efe6;
      --accent: #f15a24;
      --accent-bright: #ff6b32;
      --success: #8fbf91;
      --danger: #ff8b75;
      --line: rgba(203, 197, 185, 0.14);
      --line-strong: rgba(203, 197, 185, 0.24);
      --shadow: 0 22px 54px rgba(0, 0, 0, 0.24);
    }
    html { min-height: 100%; background: var(--ink-950); scroll-behavior: smooth; }
    body {
      min-height: 100vh;
      color: var(--paper);
      background:
        radial-gradient(circle at 72% -12%, rgba(241, 90, 36, 0.12), transparent 32rem),
        var(--ink-950);
      font-family: Inter, ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      line-height: 1.5;
    }
    button, input, textarea { font: inherit; }
    button:focus-visible, input:focus-visible, textarea:focus-visible, [tabindex]:focus-visible {
      outline: 3px solid rgba(241, 90, 36, 0.35);
      outline-offset: 2px;
    }
    .app-shell { display: grid; grid-template-columns: 248px minmax(0, 1fr); min-height: 100vh; }
    .sidebar {
      position: sticky;
      top: 0;
      height: 100vh;
      display: flex;
      flex-direction: column;
      padding: 28px 22px;
      background: rgba(29, 28, 23, 0.96);
      border-right: 1px solid var(--line);
      z-index: 10;
    }
    .brand { display: flex; align-items: center; gap: 12px; color: var(--paper); text-decoration: none; }
    .brand-mark {
      width: 42px;
      height: 42px;
      display: grid;
      place-items: center;
      border-radius: 12px;
      background: var(--accent);
      color: var(--ink-950);
      font-weight: 900;
      font-size: 1.08rem;
      letter-spacing: -0.06em;
      box-shadow: 0 10px 24px rgba(241, 90, 36, 0.22);
    }
    .brand-copy strong { display: block; font-size: 0.98rem; letter-spacing: -0.02em; }
    .brand-copy span { color: rgba(203, 197, 185, 0.62); font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.14em; }
    .side-nav { display: grid; gap: 7px; margin-top: 44px; }
    .side-nav a {
      display: flex;
      align-items: center;
      gap: 11px;
      padding: 10px 12px;
      border-radius: 10px;
      color: rgba(244, 239, 230, 0.68);
      text-decoration: none;
      font-size: 0.86rem;
      transition: background 160ms ease, color 160ms ease;
    }
    .side-nav a:hover, .side-nav a.active { background: rgba(203, 197, 185, 0.08); color: var(--paper); }
    .nav-dot { width: 7px; height: 7px; border-radius: 50%; background: var(--graphite); }
    .side-nav a.active .nav-dot { background: var(--accent); box-shadow: 0 0 0 4px rgba(241, 90, 36, 0.12); }
    .alpha-card {
      margin-top: auto;
      padding: 16px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: rgba(17, 17, 16, 0.42);
    }
    .alpha-label { display: flex; align-items: center; gap: 8px; color: var(--stone); font-size: 0.74rem; text-transform: uppercase; letter-spacing: 0.12em; }
    .live-dot { width: 8px; height: 8px; border-radius: 50%; background: var(--success); box-shadow: 0 0 0 4px rgba(143, 191, 145, 0.11); }
    .alpha-user { margin: 12px 0 3px; font-weight: 650; color: var(--paper); overflow-wrap: anywhere; }
    .alpha-quota { color: rgba(203, 197, 185, 0.58); font-size: 0.76rem; }
    .logout-form { margin-top: 13px; }
    .logout-btn { padding: 0; border: 0; background: transparent; color: rgba(203, 197, 185, 0.52); font-size: 0.72rem; cursor: pointer; }
    .logout-btn:hover { color: var(--accent); }
    .main { min-width: 0; padding: 0 34px 48px; }
    .topbar {
      min-height: 82px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 18px;
      border-bottom: 1px solid var(--line);
    }
    .topbar-title { font-size: 0.82rem; color: rgba(203, 197, 185, 0.6); text-transform: uppercase; letter-spacing: 0.13em; }
    .topbar-meta { display: flex; align-items: center; gap: 12px; }
    .secure-pill, .job-chip {
      display: inline-flex;
      align-items: center;
      gap: 7px;
      padding: 7px 10px;
      border: 1px solid var(--line);
      border-radius: 999px;
      color: rgba(244, 239, 230, 0.74);
      background: rgba(29, 28, 23, 0.72);
      font-size: 0.74rem;
    }
    .secure-pill::before { content: ""; width: 6px; height: 6px; border-radius: 50%; background: var(--success); }
    .muted { color: rgba(203, 197, 185, 0.62); }
    #refreshAt { font-size: 0.76rem; white-space: nowrap; }
    .hero-grid {
      display: grid;
      grid-template-columns: minmax(0, 1.35fr) minmax(340px, 0.65fr);
      gap: 18px;
      margin: 28px 0 18px;
    }
    .upload-panel, .result-panel {
      min-height: 470px;
      border: 1px solid var(--line);
      border-radius: 22px;
      box-shadow: var(--shadow);
      overflow: hidden;
    }
    .upload-panel {
      padding: clamp(24px, 4vw, 48px);
      background:
        linear-gradient(135deg, rgba(203, 197, 185, 0.04), transparent 48%),
        var(--ink-900);
    }
    .eyebrow { margin: 0 0 14px; color: var(--accent); font-size: 0.72rem; font-weight: 750; letter-spacing: 0.16em; text-transform: uppercase; }
    .upload-panel h1 { max-width: 680px; margin: 0; font-size: clamp(2rem, 4vw, 4.2rem); line-height: 0.98; letter-spacing: -0.055em; font-weight: 720; }
    .hero-copy { max-width: 650px; margin: 20px 0 28px; color: rgba(203, 197, 185, 0.7); font-size: clamp(0.92rem, 1.2vw, 1.05rem); }
    .drop-zone {
      position: relative;
      display: grid;
      grid-template-columns: auto 1fr auto;
      align-items: center;
      gap: 16px;
      min-height: 116px;
      padding: 20px;
      border: 1px dashed rgba(203, 197, 185, 0.3);
      border-radius: 16px;
      background: rgba(17, 17, 16, 0.44);
      cursor: pointer;
      transition: border-color 160ms ease, background 160ms ease, transform 160ms ease;
    }
    .drop-zone:hover, .drop-zone.dragging { border-color: var(--accent); background: rgba(241, 90, 36, 0.06); transform: translateY(-1px); }
    .file-glyph { width: 54px; height: 64px; position: relative; border: 1px solid rgba(203, 197, 185, 0.34); border-radius: 9px; background: var(--ink-850); }
    .file-glyph::before { content: ""; position: absolute; top: -1px; right: -1px; border-style: solid; border-width: 0 16px 16px 0; border-color: transparent var(--stone) transparent transparent; opacity: 0.78; }
    .file-glyph::after { content: "↑"; position: absolute; inset: 0; display: grid; place-items: center; color: var(--accent); font-size: 1.3rem; font-weight: 800; }
    .drop-title { color: var(--paper); font-size: 0.94rem; font-weight: 650; }
    .drop-note { margin-top: 3px; color: rgba(203, 197, 185, 0.53); font-size: 0.76rem; }
    .choose-label { color: var(--accent); font-size: 0.79rem; font-weight: 700; }
    #uploadInput { position: absolute; width: 1px; height: 1px; opacity: 0; pointer-events: none; }
    .upload-footer { display: flex; align-items: center; justify-content: space-between; gap: 16px; margin-top: 18px; }
    .pipeline { display: flex; align-items: center; min-width: 0; }
    .pipeline-step { display: flex; align-items: center; gap: 7px; color: rgba(203, 197, 185, 0.42); font-size: 0.73rem; white-space: nowrap; }
    .pipeline-step::before { content: ""; width: 7px; height: 7px; border-radius: 50%; border: 1px solid currentColor; }
    .pipeline-step.active { color: var(--accent); }
    .pipeline-step.complete { color: var(--success); }
    .pipeline-line { width: clamp(12px, 2vw, 38px); height: 1px; margin: 0 8px; background: var(--line-strong); }
    .primary-btn, .action-btn {
      border: 1px solid transparent;
      border-radius: 10px;
      background: var(--accent);
      color: #161410;
      padding: 10px 14px;
      font-size: 0.78rem;
      font-weight: 760;
      cursor: pointer;
      transition: background 150ms ease, transform 150ms ease, opacity 150ms ease;
    }
    .primary-btn { min-width: 142px; padding: 12px 17px; }
    .primary-btn:hover, .action-btn:hover { background: var(--accent-bright); transform: translateY(-1px); }
    .primary-btn:disabled, .action-btn:disabled { opacity: 0.5; cursor: wait; transform: none; }
    .action-btn.warn { color: var(--danger); border-color: rgba(255, 139, 117, 0.28); background: rgba(255, 139, 117, 0.08); }
    .action-btn.warn:hover { background: rgba(255, 139, 117, 0.15); }
    .upload-status { min-height: 21px; margin-top: 14px; color: rgba(203, 197, 185, 0.68); font-size: 0.78rem; }
    .upload-status.error { color: var(--danger); }
    .upload-status.success { color: var(--success); }
    .result-panel { display: flex; flex-direction: column; padding: 24px; background: var(--graphite); }
    .panel-head { display: flex; align-items: center; justify-content: space-between; gap: 12px; padding-bottom: 18px; border-bottom: 1px solid rgba(244, 239, 230, 0.12); }
    .panel-head h2 { margin: 0; font-size: 0.9rem; letter-spacing: -0.01em; }
    .job-chip { max-width: 150px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; background: rgba(17, 17, 16, 0.16); }
    .result-empty { flex: 1; display: grid; place-items: center; text-align: center; padding: 34px; color: rgba(244, 239, 230, 0.55); }
    .result-empty-icon { width: 72px; height: 72px; display: grid; place-items: center; margin: 0 auto 16px; border: 1px solid rgba(244, 239, 230, 0.15); border-radius: 50%; color: var(--accent); font-size: 1.5rem; }
    .result-empty strong { display: block; color: var(--paper); font-size: 0.9rem; margin-bottom: 6px; }
    .result-empty span { font-size: 0.76rem; }
    .result-content { display: grid; gap: 18px; padding-top: 20px; }
    .result-content[hidden], .result-empty[hidden] { display: none; }
    .result-status-row { display: flex; align-items: center; justify-content: space-between; gap: 10px; }
    .result-status-row h3 { margin: 0; font-size: 1.42rem; letter-spacing: -0.03em; }
    .result-summary { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 1px; overflow: hidden; border: 1px solid rgba(244, 239, 230, 0.12); border-radius: 12px; background: rgba(244, 239, 230, 0.12); }
    .result-stat { min-width: 0; padding: 13px; background: rgba(45, 44, 41, 0.92); }
    .result-stat span { display: block; margin-bottom: 4px; color: rgba(203, 197, 185, 0.52); font-size: 0.66rem; text-transform: uppercase; letter-spacing: 0.1em; }
    .result-stat strong { display: block; overflow: hidden; color: var(--paper); font-size: 0.86rem; text-overflow: ellipsis; white-space: nowrap; }
    .result-message { padding: 11px 12px; border-left: 3px solid var(--accent); background: rgba(17, 17, 16, 0.16); color: rgba(244, 239, 230, 0.72); font-size: 0.76rem; }
    details { border-top: 1px solid rgba(244, 239, 230, 0.1); padding-top: 14px; }
    details summary { color: rgba(244, 239, 230, 0.72); font-size: 0.75rem; cursor: pointer; }
    .result-json { max-height: 180px; overflow: auto; margin: 12px 0 0; padding: 12px; border-radius: 10px; background: rgba(17, 17, 16, 0.42); color: var(--stone); font: 0.7rem/1.55 ui-monospace, SFMono-Regular, Menlo, monospace; white-space: pre-wrap; overflow-wrap: anywhere; }
    .section-heading { display: flex; justify-content: space-between; gap: 16px; align-items: end; margin: 34px 0 14px; }
    .section-heading h2 { margin: 0; font-size: 1.16rem; letter-spacing: -0.03em; }
    .section-heading p { margin: 4px 0 0; color: rgba(203, 197, 185, 0.55); font-size: 0.78rem; }
    .grid { grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin-bottom: 18px; }
    .card {
      padding: 18px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: var(--ink-900);
      color: var(--paper);
      box-shadow: none;
    }
    .card h3 { margin: 0 0 12px; color: rgba(203, 197, 185, 0.6); font-size: 0.69rem; font-weight: 650; text-transform: uppercase; letter-spacing: 0.11em; }
    .kpi-card { min-height: 128px; display: flex; flex-direction: column; justify-content: space-between; }
    .kpi-kicker { display: flex; justify-content: space-between; align-items: center; }
    .kpi-icon { color: var(--accent); font-size: 0.92rem; }
    .value { color: var(--paper); font-size: clamp(1.45rem, 2.1vw, 2.1rem); letter-spacing: -0.04em; }
    .value.good { color: var(--success); }
    .value.warn { color: var(--danger); }
    .pane-grid { grid-template-columns: minmax(0, 1.35fr) minmax(300px, 0.65fr); gap: 12px; margin-bottom: 12px; }
    .table-wrap { max-height: 430px; }
    table { font-size: 0.81rem; }
    th, td { padding: 11px 9px; border-bottom-color: rgba(203, 197, 185, 0.08); }
    th { position: sticky; top: 0; z-index: 1; color: rgba(203, 197, 185, 0.5); background: var(--ink-900); font-size: 0.65rem; }
    td { color: rgba(244, 239, 230, 0.78); }
    .tag, .mini-tag { border-color: var(--line-strong); color: var(--stone); background: rgba(203, 197, 185, 0.07); }
    .tag.good { border-color: rgba(143, 191, 145, 0.28); color: var(--success); background: rgba(143, 191, 145, 0.08); }
    .tag.warn { border-color: rgba(255, 139, 117, 0.3); color: var(--danger); background: rgba(255, 139, 117, 0.08); }
    .toolbar { margin-top: -34px; }
    .toolbar input { border-color: var(--line); background: rgba(17, 17, 16, 0.48); color: var(--paper); }
    .toolbar input::placeholder { color: rgba(203, 197, 185, 0.4); }
    .bar-track { background: rgba(203, 197, 185, 0.11); }
    .bar-fill { background: linear-gradient(90deg, var(--accent), #ff8a51); }
    .feed-item { border-color: var(--line); background: rgba(17, 17, 16, 0.3); }
    .json-preview, .editor-wrap textarea { border-color: var(--line); background: rgba(17, 17, 16, 0.42); color: var(--stone); }
    .warn-box { border-color: rgba(255, 139, 117, 0.3); background: rgba(255, 139, 117, 0.08); color: var(--danger); }
    .empty-row { padding: 26px 10px; text-align: center; color: rgba(203, 197, 185, 0.46); }
    .footer-note { margin-top: 22px; color: rgba(203, 197, 185, 0.38); font-size: 0.7rem; text-align: right; }
    @media (max-width: 1180px) {
      .app-shell { grid-template-columns: 86px minmax(0, 1fr); }
      .sidebar { padding: 24px 14px; align-items: center; }
      .brand-copy, .side-nav span:not(.nav-dot), .alpha-card { display: none; }
      .side-nav a { justify-content: center; width: 44px; height: 44px; }
      .side-nav { margin-top: 34px; }
      .hero-grid { grid-template-columns: 1fr; }
      .result-panel { min-height: 390px; }
    }
    @media (max-width: 820px) {
      .app-shell { display: block; }
      .sidebar { position: static; width: 100%; height: auto; flex-direction: row; justify-content: space-between; padding: 14px 18px; border-right: 0; border-bottom: 1px solid var(--line); }
      .brand-copy { display: block; }
      .side-nav { display: none; }
      .main { padding: 0 18px 36px; }
      .topbar { min-height: 68px; }
      .topbar-title { display: none; }
      .hero-grid { margin-top: 18px; }
      .upload-panel, .result-panel { min-height: auto; }
      .upload-panel { padding: 28px 22px; }
      .upload-panel h1 { font-size: clamp(2rem, 10vw, 3.2rem); }
      .pane-grid { grid-template-columns: 1fr; }
      .grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
    @media (max-width: 560px) {
      .secure-pill { display: none; }
      .drop-zone { grid-template-columns: auto 1fr; }
      .choose-label { display: none; }
      .upload-footer { align-items: stretch; flex-direction: column; }
      .pipeline { justify-content: space-between; }
      .pipeline-line { flex: 1; }
      .primary-btn { width: 100%; }
      .grid { grid-template-columns: 1fr; }
      .result-summary { grid-template-columns: 1fr; }
      .section-heading { align-items: start; flex-direction: column; }
      .toolbar { margin: 8px 0 10px; justify-content: stretch; }
      .toolbar input { width: 100%; }
    }
  </style>
</head>
<body>
  <div class="app-shell">
    <aside class="sidebar">
      <a class="brand" href="#upload" aria-label="Ledgerly home">
        <span class="brand-mark">L</span>
        <span class="brand-copy"><strong>Ledgerly</strong><span>Invoice intelligence</span></span>
      </a>
      <nav class="side-nav" aria-label="Workspace navigation">
        <a class="active" href="#upload"><span class="nav-dot"></span><span>Process invoice</span></a>
        <a href="#overview"><span class="nav-dot"></span><span>Overview</span></a>
        <a href="#records"><span class="nav-dot"></span><span>Records</span></a>
        <a href="#reviews"><span class="nav-dot"></span><span>Review queue</span></a>
      </nav>
      <div class="alpha-card">
        <div class="alpha-label"><span class="live-dot"></span>Private alpha</div>
        <div class="alpha-user">__ALPHA_USER__</div>
        <div class="alpha-quota"><span id="documentsRemaining">__DOCUMENTS_REMAINING__</span> document credits remain</div>
        <form class="logout-form" method="post" action="/logout"><button class="logout-btn" type="submit">Sign out</button></form>
      </div>
    </aside>

    <main class="main">
      <header class="topbar">
        <div class="topbar-title">Invoice processing workspace</div>
        <div class="topbar-meta">
          <span class="secure-pill">Private &amp; encrypted</span>
          <span class="muted" id="refreshAt">Loading workspace…</span>
        </div>
      </header>

      <section class="hero-grid" id="upload">
        <article class="upload-panel">
          <p class="eyebrow">Direct to secure S3</p>
          <h1>From invoice to structured data.</h1>
          <p class="hero-copy">Drop a PDF or image here. It uploads directly to private storage, runs through the extraction pipeline, and returns the normalized result without leaving this workspace.</p>

          <label class="drop-zone" id="dropZone" for="uploadInput" tabindex="0">
            <span class="file-glyph" aria-hidden="true"></span>
            <span>
              <span class="drop-title" id="fileName">Drop an invoice here</span>
              <span class="drop-note" id="fileMeta">PDF, PNG or JPG · up to 5 MB · maximum 5 PDF pages</span>
            </span>
            <span class="choose-label">Choose file</span>
            <input id="uploadInput" type="file" accept=".pdf,.png,.jpg,.jpeg,application/pdf,image/png,image/jpeg" />
          </label>

          <div class="upload-footer">
            <div class="pipeline" aria-label="Processing progress">
              <span class="pipeline-step" id="stepAuthorize">Authorize</span><span class="pipeline-line"></span>
              <span class="pipeline-step" id="stepUpload">Upload</span><span class="pipeline-line"></span>
              <span class="pipeline-step" id="stepProcess">Extract</span>
            </div>
            <button class="primary-btn" id="uploadButton" type="button" onclick="uploadAndProcess()">Process invoice</button>
          </div>
          <div class="upload-status" id="uploadStatus" role="status" aria-live="polite">Select a document to begin.</div>
        </article>

        <aside class="result-panel" id="resultPanel" aria-live="polite">
          <div class="panel-head">
            <h2>Latest result</h2>
            <span class="job-chip" id="resultJob">Waiting</span>
          </div>
          <div class="result-empty" id="resultEmpty">
            <div>
              <div class="result-empty-icon" aria-hidden="true">⌁</div>
              <strong>No invoice processed yet</strong>
              <span>Your extraction summary will appear here as soon as processing completes.</span>
            </div>
          </div>
          <div class="result-content" id="resultContent" hidden>
            <div class="result-status-row">
              <h3 id="resultTitle">Processing</h3>
              <span class="tag" id="resultStatus">QUEUED</span>
            </div>
            <div class="result-summary">
              <div class="result-stat"><span>Vendor</span><strong id="resultVendor">—</strong></div>
              <div class="result-stat"><span>Invoice number</span><strong id="resultInvoice">—</strong></div>
              <div class="result-stat"><span>Total</span><strong id="resultAmount">—</strong></div>
              <div class="result-stat"><span>Confidence</span><strong id="resultConfidence">—</strong></div>
            </div>
            <div class="result-message" id="resultMessage">The document is moving through the extraction pipeline.</div>
            <details>
              <summary>View processing payload</summary>
              <pre class="result-json" id="resultJson"></pre>
            </details>
          </div>
        </aside>
      </section>

      <section id="overview">
        <div class="section-heading">
          <div><h2>Workspace overview</h2><p>Live totals from normalized invoice records.</p></div>
        </div>
        <div class="grid">
          <div class="card kpi-card"><div class="kpi-kicker"><h3>Total records</h3><span class="kpi-icon">↗</span></div><div class="value" id="kpiTotal">0</div></div>
          <div class="card kpi-card"><div class="kpi-kicker"><h3>Stored cleanly</h3><span class="kpi-icon">✓</span></div><div class="value good" id="kpiStored">0</div></div>
          <div class="card kpi-card"><div class="kpi-kicker"><h3>Needs review</h3><span class="kpi-icon">!</span></div><div class="value warn" id="kpiReview">0</div></div>
          <div class="card kpi-card"><div class="kpi-kicker"><h3>Processed value</h3><span class="kpi-icon">◇</span></div><div class="amount-stack"><div class="value" id="kpiAmount">0</div><div class="amount-breakdown" id="kpiAmountBreakdown"></div></div></div>
        </div>
      </section>

      <section id="records">
        <div class="section-heading"><div><h2>Records &amp; pipeline</h2><p>Search recent invoices and inspect processing health.</p></div></div>
        <div class="pane-grid">
          <div class="card">
            <h3>Recent records</h3>
            <div class="toolbar"><input id="recentSearch" type="search" placeholder="Search vendor, invoice, provider…" /></div>
            <div class="table-wrap"><table><thead><tr><th>Processed</th><th>Vendor</th><th>Invoice #</th><th>Amount</th><th>Provider</th><th>Status</th></tr></thead><tbody id="recentBody"></tbody></table></div>
          </div>
          <div class="card">
            <h3>Provider mix</h3>
            <div class="bar-list" id="providerBars"></div>
            <h3 style="margin-top:28px;">Attention required</h3>
            <div class="feed-list">
              <div class="feed-item"><div class="feed-head"><span class="muted">Review queue</span><strong id="reviewQueue">0</strong></div></div>
              <div class="feed-item"><div class="feed-head"><span class="muted">Failed jobs</span><strong id="deadLetter">0</strong></div></div>
            </div>
          </div>
        </div>

        <div class="pane-grid">
          <div class="card"><h3>Daily summary</h3><div class="table-wrap"><table><thead><tr><th>Date</th><th>Records</th><th>Stored</th><th>Needs review</th><th>Total amount</th></tr></thead><tbody id="dailyBody"></tbody></table></div></div>
          <div class="card"><h3>Top vendor spend</h3><div class="table-wrap"><table><thead><tr><th>Vendor</th><th>Invoices</th><th>Total spend</th></tr></thead><tbody id="vendorBody"></tbody></table></div></div>
        </div>

        <div class="card"><h3>Processing activity</h3><div class="feed-list" id="activityFeed"></div></div>
      </section>

      <section id="reviews">
        <div class="section-heading"><div><h2>Human review</h2><p>Approve, correct, or reject invoices that need attention.</p></div></div>
        <div class="card">
          <h3>Review queue</h3>
          <div class="toolbar"><input id="reviewSearch" type="search" placeholder="Search document, vendor, source…" /></div>
          <div class="table-wrap"><table><thead><tr><th>Document ID</th><th>Source</th><th>Vendor</th><th>Amount</th><th>Reasons</th><th>Action</th></tr></thead><tbody id="reviewBody"></tbody></table></div>
        </div>
        <div class="history-grid">
          <div class="card">
            <h3>Review history</h3>
            <div class="toolbar"><input id="historySearch" type="search" placeholder="Search status, note, document…" /></div>
            <div class="table-wrap"><table><thead><tr><th>Resolved</th><th>Status</th><th>Document</th><th>Vendor</th><th>Amount</th><th>Note</th></tr></thead><tbody id="reviewHistoryBody"></tbody></table></div>
          </div>
        </div>
      </section>

      <div class="warn-box" id="errorBox" style="display:none;"></div>
      <div class="footer-note">Private alpha · files are uploaded directly to encrypted Amazon S3</div>
    </main>
  </div>
  <script>
    let dashboardCache = {
      recent_records: [],
      activity_feed: [],
      review_items: [],
      review_history: [],
    };
    let selectedUploadFile = null;
    function fmtMoney(v) {
      const n = Number(v || 0);
      return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
    }
    function esc(s) {
      return String(s ?? "").replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',\"'\":'&#39;'}[c]));
    }
    function matchesSearch(value, search) {
      if (!search) return true;
      return String(value ?? '').toLowerCase().includes(search);
    }
    function renderRecentRecords(items, search = '') {
      const recentBody = document.getElementById('recentBody');
      recentBody.innerHTML = '';
      const filtered = (items || []).filter(r =>
        matchesSearch(r.processed_at_utc, search) ||
        matchesSearch(r.vendor_name, search) ||
        matchesSearch(r.invoice_number, search) ||
        matchesSearch(r.used_provider, search) ||
        matchesSearch(r.currency, search)
      );
      for (const r of filtered) {
        const statusTag = r.needs_review ? '<span class="tag warn">review</span>' : '<span class="tag good">ok</span>';
        recentBody.innerHTML += `<tr>
          <td>${esc(r.processed_at_utc)}</td>
          <td>${esc(r.vendor_name)}</td>
          <td>${esc(r.invoice_number)}</td>
          <td>${esc(r.currency)} ${fmtMoney(r.total_amount)}</td>
          <td>${esc(r.used_provider)}</td>
          <td>${statusTag}</td>
        </tr>`;
      }
      if (filtered.length === 0) {
        recentBody.innerHTML = '<tr><td colspan="6" class="muted">No recent records match this search.</td></tr>';
      }
    }
    function renderReviewItems(items, search = '') {
      const reviewBody = document.getElementById('reviewBody');
      reviewBody.innerHTML = '';
      const filtered = (items || []).filter(item =>
        matchesSearch(item.document_id, search) ||
        matchesSearch(item.source_file_id, search) ||
        matchesSearch(item.vendor_name, search) ||
        matchesSearch(item.invoice_number, search) ||
        matchesSearch((item.reason_codes || []).join(', '), search)
      );
      for (const item of filtered) {
        const preview = esc(JSON.stringify(item.normalized_record || {}, null, 2));
        const reasons = esc((item.reason_codes || []).join(', '));
        const rawJson = esc(JSON.stringify(item.normalized_record || {}, null, 2));
        reviewBody.innerHTML += `<tr>
          <td>${esc(item.document_id)}</td>
          <td>${esc(item.source_file_id || '-')}</td>
          <td>${esc(item.vendor_name || 'Unknown')}</td>
          <td>${esc(item.currency || 'NA')} ${fmtMoney(item.total_amount)}</td>
          <td>${reasons}</td>
          <td>
            <button class="action-btn" onclick="submitReviewAction('${esc(item.document_id)}', 'approve')">Approve</button>
            <button class="action-btn warn" onclick="submitReviewAction('${esc(item.document_id)}', 'duplicate')">Duplicate</button>
            <button class="action-btn warn" onclick="submitReviewAction('${esc(item.document_id)}', 'reject')">Reject</button>
          </td>
        </tr>
        <tr>
          <td colspan="6">
            <div class="json-preview">${preview}</div>
            <div class="editor-wrap">
              <textarea id="editor-${esc(item.document_id)}">${rawJson}</textarea>
              <div class="editor-actions">
                <button class="action-btn" onclick="submitReviewAction('${esc(item.document_id)}', 'approve')">Approve Edited Record</button>
              </div>
            </div>
          </td>
        </tr>`;
      }
      if (filtered.length === 0) {
        reviewBody.innerHTML = '<tr><td colspan="6" class="muted">No active review items match this search.</td></tr>';
      }
    }
    function renderReviewHistory(items, search = '') {
      const reviewHistoryBody = document.getElementById('reviewHistoryBody');
      reviewHistoryBody.innerHTML = '';
      const filtered = (items || []).filter(item =>
        matchesSearch(item.document_id, search) ||
        matchesSearch(item.status, search) ||
        matchesSearch(item.vendor_name, search) ||
        matchesSearch(item.resolution_note, search) ||
        matchesSearch(item.invoice_number, search)
      );
      for (const item of filtered) {
        reviewHistoryBody.innerHTML += `<tr>
          <td>${esc(item.resolved_at_utc || item.created_at_utc || '-')}</td>
          <td>${esc(item.status)}</td>
          <td>${esc(item.document_id)}</td>
          <td>${esc(item.vendor_name || 'Unknown')}</td>
          <td>${esc(item.currency || 'NA')} ${fmtMoney(item.total_amount)}</td>
          <td>${esc(item.resolution_note || '-')}</td>
        </tr>`;
      }
      if (filtered.length === 0) {
        reviewHistoryBody.innerHTML = '<tr><td colspan="6" class="muted">No review history items match this search.</td></tr>';
      }
    }
    function renderActivityFeed(items) {
      const feed = document.getElementById('activityFeed');
      feed.innerHTML = '';
      for (const item of items || []) {
        const status = String(item.status || 'UNKNOWN');
        const tagClass = status.includes('FAILED') || status.includes('REJECTED')
          ? 'warn'
          : status.includes('REVIEW')
            ? 'warn'
            : 'good';
        const meta = [
          item.vendor_name ? `Vendor: ${esc(item.vendor_name)}` : '',
          item.invoice_number ? `Invoice: ${esc(item.invoice_number)}` : '',
          item.currency ? `${esc(item.currency)} ${fmtMoney(item.total_amount)}` : '',
          item.used_provider ? `Provider: ${esc(item.used_provider)}` : '',
          item.source_file_id ? `Source: ${esc(item.source_file_id)}` : ''
        ].filter(Boolean).join(' • ');
        feed.innerHTML += `<div class="feed-item">
          <div class="feed-head">
            <div class="feed-title">
              <span class="tag ${tagClass}">${esc(status)}</span>
              <span>${esc(item.message || 'Pipeline event')}</span>
            </div>
            <div class="muted">${esc(item.recorded_at_utc || '-')}</div>
          </div>
          <div class="feed-meta">
            ${meta || '<span class="muted">No additional details.</span>'}
          </div>
        </div>`;
      }
      if ((items || []).length === 0) {
        feed.innerHTML = '<div class="muted">No recent activity yet.</div>';
      }
    }
    function bindSearchInputs() {
      const recentSearch = document.getElementById('recentSearch');
      const reviewSearch = document.getElementById('reviewSearch');
      const historySearch = document.getElementById('historySearch');
      if (recentSearch && !recentSearch.dataset.bound) {
        recentSearch.addEventListener('input', (e) => renderRecentRecords(dashboardCache.recent_records, e.target.value.trim().toLowerCase()));
        recentSearch.dataset.bound = '1';
      }
      if (reviewSearch && !reviewSearch.dataset.bound) {
        reviewSearch.addEventListener('input', (e) => renderReviewItems(dashboardCache.review_items, e.target.value.trim().toLowerCase()));
        reviewSearch.dataset.bound = '1';
      }
      if (historySearch && !historySearch.dataset.bound) {
        historySearch.addEventListener('input', (e) => renderReviewHistory(dashboardCache.review_history, e.target.value.trim().toLowerCase()));
        historySearch.dataset.bound = '1';
      }
    }
    async function submitReviewAction(documentId, action) {
      const note = window.prompt(`Optional note for ${action}:`, '') ?? '';
      const editor = document.getElementById(`editor-${documentId}`);
      let correctedRecord = null;
      if (action === 'approve' && editor) {
        try {
          correctedRecord = JSON.parse(editor.value);
        } catch (err) {
          window.alert(`Invalid JSON for ${documentId}: ${err.message}`);
          return;
        }
      }
      const resp = await fetch(`/review-items/${encodeURIComponent(documentId)}/resolve`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action, note, corrected_record: correctedRecord })
      });
      const payload = await resp.json();
      if (!resp.ok) {
        window.alert(payload.detail || 'Review resolution failed.');
        return;
      }
      await loadData();
      window.alert(`${action} complete for ${documentId} (${payload.review_status})`);
    }
    const uploadMode = '__UPLOAD_MODE__';
    const terminalJobStatuses = new Set(['STORED', 'REVIEW_REQUIRED', 'DUPLICATE', 'REJECTED', 'FAILED']);
    const wait = (milliseconds) => new Promise(resolve => window.setTimeout(resolve, milliseconds));

    function detectContentType(file) {
      if (file.type) return file.type;
      const name = file.name.toLowerCase();
      if (name.endsWith('.pdf')) return 'application/pdf';
      if (name.endsWith('.png')) return 'image/png';
      if (name.endsWith('.jpg') || name.endsWith('.jpeg')) return 'image/jpeg';
      return 'application/octet-stream';
    }
    function formatFileSize(bytes) {
      if (bytes < 1024) return `${bytes} B`;
      if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
      return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
    }
    function showSelectedFile(file) {
      selectedUploadFile = file || null;
      document.getElementById('fileName').textContent = file ? file.name : 'Drop an invoice here';
      document.getElementById('fileMeta').textContent = file
        ? `${formatFileSize(file.size)} · ${detectContentType(file)}`
        : 'PDF, PNG or JPG · up to 5 MB · maximum 5 PDF pages';
      const statusEl = document.getElementById('uploadStatus');
      statusEl.className = 'upload-status';
      statusEl.textContent = file ? 'Ready for secure upload.' : 'Select a document to begin.';
      setPipeline('idle');
    }
    function bindUploadControls() {
      const input = document.getElementById('uploadInput');
      const zone = document.getElementById('dropZone');
      if (!input || !zone || zone.dataset.bound) return;
      input.addEventListener('change', () => showSelectedFile(input.files?.[0] || null));
      for (const eventName of ['dragenter', 'dragover']) {
        zone.addEventListener(eventName, event => {
          event.preventDefault();
          zone.classList.add('dragging');
        });
      }
      for (const eventName of ['dragleave', 'drop']) {
        zone.addEventListener(eventName, event => {
          event.preventDefault();
          zone.classList.remove('dragging');
        });
      }
      zone.addEventListener('drop', event => showSelectedFile(event.dataTransfer?.files?.[0] || null));
      zone.addEventListener('keydown', event => {
        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault();
          input.click();
        }
      });
      zone.dataset.bound = '1';
    }
    function setPipeline(stage) {
      const order = ['authorize', 'upload', 'process'];
      const ids = { authorize: 'stepAuthorize', upload: 'stepUpload', process: 'stepProcess' };
      const activeIndex = order.indexOf(stage);
      for (const [index, name] of order.entries()) {
        const element = document.getElementById(ids[name]);
        element.classList.remove('active', 'complete');
        if (stage === 'complete' || (activeIndex >= 0 && index < activeIndex)) element.classList.add('complete');
        if (name === stage) element.classList.add('active');
      }
    }
    function setUploadMessage(message, kind = '') {
      const statusEl = document.getElementById('uploadStatus');
      statusEl.className = `upload-status${kind ? ` ${kind}` : ''}`;
      statusEl.textContent = message;
    }
    function setUploadBusy(isBusy) {
      const button = document.getElementById('uploadButton');
      button.disabled = isBusy;
      button.textContent = isBusy ? 'Processing…' : 'Process invoice';
    }
    function renderJobResult(job) {
      const result = job.result || {};
      const record = result.record || result.normalized_record || {};
      const status = String(job.status || result.status || 'UNKNOWN');
      const successful = status === 'STORED';
      const warning = ['REVIEW_REQUIRED', 'DUPLICATE', 'REJECTED', 'FAILED'].includes(status);
      document.getElementById('resultEmpty').hidden = true;
      document.getElementById('resultContent').hidden = false;
      document.getElementById('resultJob').textContent = String(job.id || 'Job').slice(0, 13);
      document.getElementById('resultTitle').textContent = job.original_name || 'Invoice result';
      const statusTag = document.getElementById('resultStatus');
      statusTag.textContent = status.replaceAll('_', ' ');
      statusTag.className = `tag${successful ? ' good' : warning ? ' warn' : ''}`;
      document.getElementById('resultVendor').textContent = record.vendor_name || '—';
      document.getElementById('resultInvoice').textContent = record.invoice_number || '—';
      document.getElementById('resultAmount').textContent = record.total_amount != null
        ? `${record.currency || ''} ${fmtMoney(record.total_amount)}`.trim()
        : '—';
      document.getElementById('resultConfidence').textContent = record.model_confidence != null
        ? `${Math.round(Number(record.model_confidence) * 100)}%`
        : '—';
      const messages = {
        STORED: 'Extraction complete. The normalized invoice has been stored and added to your records.',
        REVIEW_REQUIRED: 'Extraction completed, but this invoice needs a quick human review before approval.',
        DUPLICATE: 'This file matches an invoice that has already been processed.',
        REJECTED: job.error_message || result.error_message || 'The file did not pass document validation.',
        FAILED: job.error_message || result.error_message || 'Processing failed. You can retry this job from the workspace.',
      };
      document.getElementById('resultMessage').textContent = messages[status] || 'The document is moving through the extraction pipeline.';
      document.getElementById('resultJson').textContent = JSON.stringify(result, null, 2);
    }
    function renderProcessingJob(jobId, fileName) {
      document.getElementById('resultEmpty').hidden = true;
      document.getElementById('resultContent').hidden = false;
      document.getElementById('resultJob').textContent = String(jobId).slice(0, 13);
      document.getElementById('resultTitle').textContent = fileName;
      const statusTag = document.getElementById('resultStatus');
      statusTag.textContent = 'PROCESSING';
      statusTag.className = 'tag';
      document.getElementById('resultVendor').textContent = 'Extracting…';
      document.getElementById('resultInvoice').textContent = '—';
      document.getElementById('resultAmount').textContent = '—';
      document.getElementById('resultConfidence').textContent = '—';
      document.getElementById('resultMessage').textContent = 'Secure upload complete. The worker is validating and extracting invoice fields.';
      document.getElementById('resultJson').textContent = JSON.stringify({ job_id: jobId, status: 'PROCESSING' }, null, 2);
    }
    async function waitForJob(jobId, fileName) {
      const deadline = Date.now() + 360000;
      renderProcessingJob(jobId, fileName);
      while (Date.now() < deadline) {
        await wait(1800);
        const response = await fetch(`/uploads/${encodeURIComponent(jobId)}`);
        if (!response.ok) throw new Error('Could not read processing status.');
        const job = await response.json();
        if (job.status === 'PROCESSING') setUploadMessage(`Extracting data from ${fileName}…`);
        if (terminalJobStatuses.has(job.status)) {
          renderJobResult(job);
          setPipeline('complete');
          const isFailure = ['REJECTED', 'FAILED'].includes(job.status);
          setUploadMessage(
            isFailure ? `${fileName} could not be processed.` : `${fileName} finished with status ${job.status.replaceAll('_', ' ')}.`,
            isFailure ? 'error' : 'success'
          );
          await loadData();
          return job;
        }
      }
      throw new Error('Processing is taking longer than expected. The job is still running; refresh shortly to see it in activity.');
    }
    async function uploadAndProcess() {
      const input = document.getElementById('uploadInput');
      const file = selectedUploadFile || input?.files?.[0];
      if (!file) {
        setUploadMessage('Choose a PDF, PNG, or JPG before processing.', 'error');
        return;
      }
      setUploadBusy(true);
      setPipeline('authorize');
      setUploadMessage('Authorizing a private upload…');
      try {
        if (uploadMode === 's3') {
          const authResp = await fetch('/uploads/presign', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ filename: file.name, content_type: detectContentType(file), size: file.size })
          });
          const authPayload = await authResp.json();
          if (!authResp.ok) throw new Error(authPayload.detail || 'Upload authorization failed.');
          const remaining = document.getElementById('documentsRemaining');
          if (remaining && authPayload.documents_remaining != null) remaining.textContent = authPayload.documents_remaining;
          setPipeline('upload');
          setUploadMessage(`Uploading ${file.name} directly to encrypted S3…`);
          const form = new FormData();
          for (const [key, value] of Object.entries(authPayload.upload.fields || {})) form.append(key, value);
          form.append('file', file);
          const s3Resp = await fetch(authPayload.upload.url, { method: 'POST', body: form });
          if (!s3Resp.ok) throw new Error('The direct S3 upload failed. Please try again.');
          setPipeline('process');
          setUploadMessage(`Upload complete. Starting extraction for ${file.name}…`);
          input.value = '';
          selectedUploadFile = null;
          await waitForJob(authPayload.job_id, file.name);
          return;
        }
        const form = new FormData();
        form.append('file', file);
        const response = await fetch('/upload', { method: 'POST', body: form });
        const payload = await response.json();
        if (!response.ok) throw new Error(payload.detail || 'Upload failed.');
        const processing = payload.processing_result || {};
        renderJobResult({ id: processing.document_id, original_name: file.name, status: processing.status, result: processing });
        setPipeline('complete');
        setUploadMessage(`${file.name} finished with status ${processing.status || 'completed'}.`, 'success');
        input.value = '';
        selectedUploadFile = null;
        await loadData();
      } catch (error) {
        setUploadMessage(error.message || 'The upload could not be completed.', 'error');
        setPipeline('idle');
      } finally {
        setUploadBusy(false);
      }
    }
    async function loadData() {
      const [dashboardResp, reviewResp, historyResp] = await Promise.all([
        fetch('/dashboard/data?limit=25'),
        fetch('/review-items'),
        fetch('/review-history?limit=20')
      ]);
      const data = await dashboardResp.json();
      const reviewData = await reviewResp.json();
      const historyData = await historyResp.json();
      dashboardCache.recent_records = data.recent_records || [];
      dashboardCache.activity_feed = data.activity_feed || [];
      dashboardCache.review_items = reviewData.items || [];
      dashboardCache.review_history = historyData.items || [];
      document.getElementById('refreshAt').textContent = 'Updated: ' + new Date().toLocaleString();
      document.getElementById('kpiTotal').textContent = data.kpis.records_total ?? 0;
      document.getElementById('kpiStored').textContent = data.kpis.stored_total ?? 0;
      document.getElementById('kpiReview').textContent = data.kpis.needs_review_total ?? 0;
      document.getElementById('kpiAmount').textContent = data.kpis.total_amount_display ?? '0';
      const amountBreakdown = document.getElementById('kpiAmountBreakdown');
      amountBreakdown.innerHTML = '';
      for (const item of data.currency_totals || []) {
        amountBreakdown.innerHTML += `<span class="mini-tag">${esc(item.currency)} ${fmtMoney(item.total_amount_sum)}</span>`;
      }
      if ((data.currency_totals || []).length === 0) {
        amountBreakdown.innerHTML = '<span class="muted">No stored amounts yet.</span>';
      }
      document.getElementById('reviewQueue').textContent = data.review_queue_total ?? 0;
      document.getElementById('deadLetter').textContent = data.dead_letter_total ?? 0;
      bindSearchInputs();
      renderRecentRecords(
        dashboardCache.recent_records,
        document.getElementById('recentSearch')?.value.trim().toLowerCase() || ''
      );

      const dailyBody = document.getElementById('dailyBody');
      dailyBody.innerHTML = '';
      for (const d of data.daily_summary || []) {
        dailyBody.innerHTML += `<tr>
          <td>${esc(d.processing_date)}</td>
          <td>${esc(d.records_total)}</td>
          <td>${esc(d.stored_total)}</td>
          <td>${esc(d.needs_review_total)}</td>
          <td>${fmtMoney(d.total_amount_sum)}</td>
        </tr>`;
      }

      const vendorBody = document.getElementById('vendorBody');
      vendorBody.innerHTML = '';
      for (const v of data.vendor_spend || []) {
        vendorBody.innerHTML += `<tr>
          <td>${esc(v.vendor_name)}</td>
          <td>${esc(v.invoices)}</td>
          <td>${fmtMoney(v.total_spend)}</td>
        </tr>`;
      }

      const bars = document.getElementById('providerBars');
      bars.innerHTML = '';
      const maxVal = Math.max(...(data.provider_mix || []).map(x => x.records_total), 1);
      for (const p of data.provider_mix || []) {
        const w = Math.round((p.records_total / maxVal) * 100);
        bars.innerHTML += `<div class="bar-row">
          <div>${esc(p.used_provider)}</div>
          <div class="bar-track"><div class="bar-fill" style="width:${w}%"></div></div>
          <div>${esc(p.records_total)}</div>
        </div>`;
      }

      renderActivityFeed(dashboardCache.activity_feed);
      renderReviewItems(
        dashboardCache.review_items,
        document.getElementById('reviewSearch')?.value.trim().toLowerCase() || ''
      );
      renderReviewHistory(
        dashboardCache.review_history,
        document.getElementById('historySearch')?.value.trim().toLowerCase() || ''
      );

      const errorBox = document.getElementById('errorBox');
      if (data.error) {
        errorBox.style.display = 'block';
        errorBox.textContent = 'Dashboard query warning: ' + data.error;
      } else {
        errorBox.style.display = 'none';
      }
    }
    bindUploadControls();
    loadData();
    setInterval(loadData, 30000);
  </script>
</body>
</html>"""
    if isinstance(principal, AlphaUser):
        alpha_user = principal.username
        documents_remaining = str(principal.documents_remaining)
    else:
        alpha_user = str(principal or "Operator")
        documents_remaining = "—"
    safe_upload_mode = upload_mode if upload_mode in {"s3", "r2"} else "disabled"
    return (
        html.replace("__UPLOAD_MODE__", safe_upload_mode)
        .replace("__ALPHA_USER__", html_lib.escape(alpha_user))
        .replace("__DOCUMENTS_REMAINING__", documents_remaining)
    )
