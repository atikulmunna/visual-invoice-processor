from __future__ import annotations

import json
import os
import secrets
from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi import Cookie, Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel, ValidationError

from app.alpha_store import (
    AlphaAuthenticationError,
    AlphaNotFoundError,
    AlphaQuotaError,
    AlphaStore,
    AlphaUser,
)
from app.config import Settings, load_dotenv
from app.drive_service import is_supported_mime_type
from app.object_storage_service import ObjectStorageService
from app.review_queue import dismiss_review_item, list_review_items, resolve_review_item
from app.web_session import SESSION_COOKIE_NAME, clear_session_cookie
from app.vendor_api import build_vendor_router
from app.workspace_api import build_workspace_router, mount_frontend


class ReviewResolveRequest(BaseModel):
    action: str = "approve"
    note: str | None = None
    corrected_record: dict[str, Any] | None = None


class PresignUploadRequest(BaseModel):
    filename: str
    content_type: str
    size: int


SITE_ICON_PATH = Path(__file__).resolve().parents[1] / "assets" / "icon.png"
FRONTEND_DIST = Path(__file__).resolve().parents[1] / "frontend" / "dist"


def _validation_message(error: ValidationError) -> str:
    """One readable line per invalid field, such as "line item 1 quantity: Input should be greater than 0"."""
    parts = []
    for issue in error.errors():
        location = [
            f"line item {part + 1}" if isinstance(part, int) else str(part).replace("_", " ")
            for part in issue["loc"]
            if part != "line_items"
        ]
        parts.append(f"{' '.join(location)}: {issue['msg']}")
    return "; ".join(parts)


def _org_scope(principal: str | AlphaUser) -> str | None:
    """Organization that bounds every query for this request.

    Tester accounts are always scoped to their active organization. A plain string
    principal comes from basic or anonymous auth on a single-operator deployment,
    which has no tenants and sees everything.
    """
    if isinstance(principal, AlphaUser):
        if not principal.org_id:
            raise HTTPException(status_code=403, detail="No organization is available for this account")
        return principal.org_id
    return None


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
    frontend_dist: str | Path = FRONTEND_DIST,
) -> FastAPI:
    app = FastAPI(title="Invoice Processor Monitoring API", version="0.1.0")
    active_postgres_dsn = postgres_dsn or os.getenv("POSTGRES_DSN")
    require_dashboard_auth = _build_auth_dependency(active_postgres_dsn)

    @app.get("/assets/icon.png", include_in_schema=False)
    def site_icon() -> FileResponse:
        return FileResponse(
            SITE_ICON_PATH,
            media_type="image/png",
            headers={"Cache-Control": "public, max-age=86400"},
        )

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/", include_in_schema=False)
    def root() -> RedirectResponse:
        if (os.getenv("ALPHA_AUTH_ENABLED") or "").strip().lower() in {"1", "true", "yes", "on"}:
            return RedirectResponse(url="/login", status_code=307)
        return RedirectResponse(url="/app/overview", status_code=307)

    @app.get("/login", include_in_schema=False)
    def login_page(request: Request) -> RedirectResponse:
        query = request.url.query
        return RedirectResponse(url=f"/app/login?{query}" if query else "/app/login", status_code=307)

    @app.post("/logout", include_in_schema=False)
    def logout(
        session_token: str | None = Cookie(default=None, alias=SESSION_COOKIE_NAME),
    ) -> RedirectResponse:
        if session_token and active_postgres_dsn:
            AlphaStore(active_postgres_dsn).delete_session(session_token)
        response = RedirectResponse(url="/login", status_code=303)
        clear_session_cookie(response)
        return response

    # The local JSONL metrics and dead-letter logs are not tenant-aware, so only an
    # unscoped operator reads them.
    def _metric_events(org_id: str | None) -> list[dict[str, Any]]:
        return _read_jsonl(metrics_path) if org_id is None else []

    def _dead_letters(org_id: str | None, resolved_hashes: set[str]) -> list[dict[str, Any]]:
        return _active_dead_letters(dead_letter_path, resolved_hashes) if org_id is None else []

    @app.get("/stats")
    def stats(principal: str | AlphaUser = Depends(require_dashboard_auth)) -> dict[str, Any]:
        org_id = _org_scope(principal)
        resolved_hashes = _resolved_file_hashes(active_postgres_dsn, org_id=org_id)
        dead_letters = _dead_letters(org_id, resolved_hashes)
        review_items = _active_review_items(review_queue_dir, resolved_hashes, org_id=org_id)
        counters = _aggregate_metrics(_metric_events(org_id))
        counters["dead_letter_total"] = len(dead_letters)
        counters["review_queue_total"] = len(review_items)
        return counters

    @app.get("/failures")
    def failures(limit: int = 50, principal: str | AlphaUser = Depends(require_dashboard_auth)) -> dict[str, Any]:
        items = _read_jsonl(dead_letter_path) if _org_scope(principal) is None else []
        return {"count": len(items), "items": items[-limit:]}

    @app.get("/backlog")
    def backlog(principal: str | AlphaUser = Depends(require_dashboard_auth)) -> dict[str, Any]:
        org_id = _org_scope(principal)
        resolved_hashes = _resolved_file_hashes(active_postgres_dsn, org_id=org_id)
        queue_size = len(_active_review_items(review_queue_dir, resolved_hashes, org_id=org_id))
        dead_letters = len(_dead_letters(org_id, resolved_hashes))
        return {
            "review_queue_total": queue_size,
            "dead_letter_total": dead_letters,
            "attention_total": queue_size + dead_letters,
        }

    @app.get("/review-items")
    def review_items(principal: str | AlphaUser = Depends(require_dashboard_auth)) -> dict[str, Any]:
        org_id = _org_scope(principal)
        resolved_hashes = _resolved_file_hashes(active_postgres_dsn, org_id=org_id)
        items = _active_review_items(review_queue_dir, resolved_hashes, org_id=org_id)
        return {"count": len(items), "items": items}

    @app.get("/review-history")
    def review_history(
        limit: int = 20,
        principal: str | AlphaUser = Depends(require_dashboard_auth),
    ) -> dict[str, Any]:
        items = _review_history_items(review_queue_dir, limit=limit, org_id=_org_scope(principal))
        return {"count": len(items), "items": items}

    @app.post("/review-items/{document_id}/resolve")
    def review_resolve(
        document_id: str,
        payload: ReviewResolveRequest | None = None,
        principal: str | AlphaUser = Depends(require_dashboard_auth),
    ) -> dict[str, Any]:
        org_id = _org_scope(principal)
        try:
            action = (payload.action if payload else "approve").strip().lower()
            if action == "approve":
                result = resolve_review_item(
                    document_id=document_id,
                    queue_dir=review_queue_dir,
                    record_override=payload.corrected_record if payload else None,
                    note=payload.note if payload else None,
                    org_id=org_id,
                )
            elif action == "reject":
                result = dismiss_review_item(
                    document_id=document_id,
                    queue_dir=review_queue_dir,
                    resolution_status="REJECTED",
                    note=payload.note if payload else None,
                    org_id=org_id,
                )
            elif action == "duplicate":
                result = dismiss_review_item(
                    document_id=document_id,
                    queue_dir=review_queue_dir,
                    resolution_status="RESOLVED_DUPLICATE_MANUAL",
                    note=payload.note if payload else None,
                    org_id=org_id,
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
        except ValidationError as exc:
            raise HTTPException(status_code=400, detail=_validation_message(exc)) from exc
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=str(exc)) from exc

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
        _org_scope(principal)
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
        org_id = _org_scope(principal)
        try:
            return AlphaStore(active_postgres_dsn or "").get_job(job_id, org_id=org_id)
        except AlphaNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/uploads/{job_id}/retry")
    def retry_upload(
        job_id: str,
        principal: str | AlphaUser = Depends(require_dashboard_auth),
    ) -> dict[str, str]:
        if not isinstance(principal, AlphaUser):
            raise HTTPException(status_code=403, detail="Private-alpha authentication is required")
        org_id = _org_scope(principal)
        load_dotenv()
        settings = Settings.from_env()
        store = AlphaStore(active_postgres_dsn or "")
        try:
            object_key = store.retry_job(job_id, org_id=org_id)
        except AlphaQuotaError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        # Only a job already verified to belong to this organization can be marked failed below.
        try:
            ObjectStorageService.from_settings(settings).retrigger_object(object_key)
        except Exception as exc:  # noqa: BLE001
            store.complete_job(
                job_id,
                status="FAILED",
                error_code="retry_trigger_failed",
                error_message=str(exc),
            )
            raise HTTPException(status_code=400, detail="Retry could not be scheduled") from exc
        return {"job_id": job_id, "status": "AUTHORIZED"}

    @app.get("/dashboard", include_in_schema=False)
    def classic_dashboard() -> RedirectResponse:
        # The classic page was replaced by the workspace; old bookmarks land on its overview.
        return RedirectResponse(url="/app/overview", status_code=308)

    def pending_reviews(org_id: str) -> list[dict[str, Any]]:
        resolved_hashes = _resolved_file_hashes(active_postgres_dsn, org_id=org_id)
        return _active_review_items(review_queue_dir, resolved_hashes, org_id=org_id)

    app.include_router(
        build_workspace_router(
            require_dashboard_auth,
            active_postgres_dsn,
            review_queue_dir=review_queue_dir,
            pending_reviews=pending_reviews,
        )
    )
    app.include_router(build_vendor_router(require_dashboard_auth, active_postgres_dsn))
    mount_frontend(app, Path(frontend_dist))
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


def _active_review_items(
    path: str | Path,
    resolved_hashes: set[str],
    *,
    org_id: str | None = None,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for payload in list_review_items(queue_dir=path, org_id=org_id):
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


def _review_history_items(
    path: str | Path,
    *,
    limit: int = 20,
    org_id: str | None = None,
) -> list[dict[str, Any]]:
    history: list[dict[str, Any]] = []
    for payload in list_review_items(queue_dir=path, org_id=org_id):
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


def _org_where(org_id: str | None, *, prefix: str = "where") -> tuple[str, tuple[Any, ...]]:
    if org_id is None:
        return "", ()
    return f"{prefix} org_id = %s", (org_id,)


def _resolved_file_hashes(postgres_dsn: str | None, *, org_id: str | None = None) -> set[str]:
    if not postgres_dsn:
        return set()
    try:
        import psycopg
    except ImportError:
        return set()

    try:
        with psycopg.connect(postgres_dsn, prepare_threshold=None) as conn:
            with conn.cursor() as cur:
                org_sql, org_params = _org_where(org_id, prefix="and")
                cur.execute(
                    f"""
                    select distinct file_hash
                    from public.ledger_records
                    where status in ('STORED', 'ARCHIVED') {org_sql}
                    """,
                    org_params,
                )
                return {str(row[0]) for row in cur.fetchall() if row and row[0]}
    except Exception:  # noqa: BLE001
        return set()
