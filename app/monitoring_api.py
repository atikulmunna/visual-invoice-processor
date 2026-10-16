from __future__ import annotations

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
from app.object_storage_service import ObjectStorageService
from app.review_queue import dismiss_review_item, pending_review_items, resolve_review_item
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


# Review actions that close an item without storing a record, and the status each one leaves.
DISMISSALS = {"reject": "REJECTED", "duplicate": "RESOLVED_DUPLICATE_MANUAL"}
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
    postgres_dsn: str | None = None,
    frontend_dist: str | Path = FRONTEND_DIST,
) -> FastAPI:
    app = FastAPI(title="Invoice Processor Monitoring API", version="0.1.0")
    dsn = postgres_dsn or os.getenv("POSTGRES_DSN") or ""
    require_dashboard_auth = _build_auth_dependency(dsn)

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
        if session_token and dsn:
            AlphaStore(dsn).delete_session(session_token)
        response = RedirectResponse(url="/login", status_code=303)
        clear_session_cookie(response)
        return response

    @app.get("/backlog")
    def backlog(principal: str | AlphaUser = Depends(require_dashboard_auth)) -> dict[str, Any]:
        return {"review_queue_total": len(pending_review_items(dsn, org_id=_org_scope(principal)))}

    @app.post("/review-items/{document_id}/resolve")
    def review_resolve(
        document_id: str,
        payload: ReviewResolveRequest | None = None,
        principal: str | AlphaUser = Depends(require_dashboard_auth),
    ) -> dict[str, Any]:
        org_id = _org_scope(principal)
        request = payload or ReviewResolveRequest()
        action = request.action.strip().lower()
        try:
            if action in DISMISSALS:
                result = dismiss_review_item(
                    dsn, document_id, resolution_status=DISMISSALS[action], note=request.note, org_id=org_id
                )
            elif action == "approve":
                result = resolve_review_item(
                    dsn, document_id, record_override=request.corrected_record, note=request.note, org_id=org_id
                )
            else:
                raise ValueError(f"Unsupported review action: {action}")
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValidationError as exc:
            raise HTTPException(status_code=400, detail=_validation_message(exc)) from exc
        except ValueError as exc:
            # Fields left empty, an item already handled, or an unknown action: the reviewer can act on these.
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {
            "status": "ok",
            "document_id": document_id,
            "action": action,
            "storage_result": result["storage_result"],
            "review_status": result["review_item"].get("status"),
        }

    @app.post("/uploads/presign")
    def presign_upload(
        payload: PresignUploadRequest,
        principal: str | AlphaUser = Depends(require_dashboard_auth),
    ) -> dict[str, Any]:
        load_dotenv()
        settings = Settings.from_env()
        if not isinstance(principal, AlphaUser):
            raise HTTPException(status_code=403, detail="Private-alpha authentication is required")
        _org_scope(principal)
        if payload.size < 1 or payload.size > settings.max_upload_bytes:
            raise HTTPException(status_code=400, detail=f"File must be between 1 and {settings.max_upload_bytes} bytes")
        if payload.content_type not in settings.allowed_mime_types:
            raise HTTPException(status_code=400, detail=f"Unsupported file type: {payload.content_type}")

        original_name = Path(payload.filename).name
        if not original_name:
            raise HTTPException(status_code=400, detail="A filename is required")
        job_id = str(uuid4())
        object_key = (
            f"{settings.s3_inbox_prefix.rstrip('/')}/{principal.id}/{job_id}/{original_name}"
        )
        try:
            store = AlphaStore(dsn)
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
        except AlphaAuthenticationError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
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
            return AlphaStore(dsn).get_job(job_id, org_id=org_id)
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
        store = AlphaStore(dsn)
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

    app.include_router(build_workspace_router(require_dashboard_auth, dsn))
    app.include_router(build_vendor_router(require_dashboard_auth, dsn))
    mount_frontend(app, Path(frontend_dist))
    return app
