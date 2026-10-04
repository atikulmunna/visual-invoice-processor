"""JSON endpoints and static hosting for the new single-page workspace served at /app."""

from __future__ import annotations

import logging
import mimetypes
from pathlib import Path
from typing import Any, Callable

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Response
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from app.alpha_store import (
    MAX_JOB_ATTEMPTS,
    AlphaAuthenticationError,
    AlphaNotFoundError,
    AlphaStore,
    AlphaUser,
)
from app.config import Settings, load_dotenv
from app.object_storage_service import ObjectStorageService
from app.review_queue import load_review_item
from app.web_session import set_session_cookie

logger = logging.getLogger(__name__)


class OrganizationUpdate(BaseModel):
    base_currency: str


class SignInRequest(BaseModel):
    # Bounded so an oversized password cannot make password hashing expensive.
    username: str = Field(max_length=64)
    password: str = Field(max_length=256)


def _organization_payload(store: AlphaStore, user: AlphaUser) -> dict[str, Any]:
    organization = store.get_organization(user.org_id or "")
    return {
        "id": organization.id,
        "name": organization.name,
        "role": user.role,
        "base_currency": organization.base_currency,
    }


def _isoformat(value: Any) -> str | None:
    return value.isoformat() if value is not None else None


def job_view(job: dict[str, Any]) -> dict[str, Any]:
    """What the workspace shows for a processing job.

    Failed jobs expose only an error code: their messages can contain upstream provider
    responses. Rejections describe the uploaded file itself, so their message is kept.
    """
    result = job.get("result") if isinstance(job.get("result"), dict) else {}
    record = result.get("record") if isinstance(result.get("record"), dict) else None
    summary = None
    if record:
        summary = {
            "vendor_name": record.get("vendor_name"),
            "invoice_date": record.get("invoice_date"),
            "total_amount": record.get("total_amount"),
            "currency": record.get("currency"),
            "currency_assumed": bool(record.get("currency_assumed")),
        }
    status = job["status"]
    return {
        "id": str(job["id"]),
        "name": job["original_name"],
        "content_type": job["content_type"],
        "size": int(job["declared_size"]),
        "status": status,
        "page_count": job.get("page_count"),
        "reason_codes": list(result.get("reason_codes") or []),
        "error_code": job.get("error_code"),
        "error_message": job.get("error_message") if status == "REJECTED" else None,
        "retryable": status == "FAILED" and int(job.get("attempts") or 0) < MAX_JOB_ATTEMPTS,
        "authorized_at": _isoformat(job.get("authorized_at_utc")),
        "completed_at": _isoformat(job.get("completed_at_utc")),
        "summary": summary,
    }


def review_summary(item: dict[str, Any]) -> dict[str, Any]:
    """One row of the review queue."""
    return {
        "document_id": item.get("document_id"),
        "created_at": item.get("created_at_utc"),
        "reason_codes": list(item.get("reason_codes") or []),
        "vendor_name": item.get("vendor_name"),
        "invoice_number": item.get("invoice_number"),
        "invoice_date": item.get("invoice_date"),
        "currency": item.get("currency"),
        "total_amount": item.get("total_amount"),
    }


def _organization_member(principal: str | AlphaUser) -> AlphaUser:
    if not isinstance(principal, AlphaUser) or not principal.org_id:
        raise HTTPException(status_code=403, detail="No organization is available for this account")
    return principal


def build_workspace_router(
    require_auth: Callable[..., Any],
    postgres_dsn: str | None,
    *,
    review_queue_dir: str | Path,
    pending_reviews: Callable[[str], list[dict[str, Any]]],
) -> APIRouter:
    """pending_reviews(org_id) lists the review items still waiting, as the top bar counts them."""
    router = APIRouter(prefix="/api")

    def review_document(source_key: str | None, org_id: str) -> dict[str, Any]:
        """The flagged document's name, type, and a five-minute inline link, when it still exists."""
        if not source_key:
            return {"name": None, "content_type": None, "url": None}
        job = AlphaStore(postgres_dsn or "").find_job_by_object_key(source_key, org_id=org_id)
        name = job["original_name"] if job else source_key.rsplit("/", 1)[-1]
        content_type = job["content_type"] if job else (mimetypes.guess_type(name)[0] or "application/octet-stream")
        try:
            load_dotenv()
            storage = ObjectStorageService.from_settings(Settings.from_env())
            archived = storage.archive_key_for(source_key)
            url = storage.create_presigned_download(archived, content_type=content_type) if storage.object_exists(archived) else None
        except Exception:  # noqa: BLE001
            logger.exception("Could not prepare a link for review document %s", source_key)
            url = None
        return {"name": name, "content_type": content_type, "url": url}

    @router.post("/session")
    def sign_in(credentials: SignInRequest, response: Response) -> dict[str, str]:
        if not postgres_dsn:
            raise HTTPException(status_code=503, detail="Sign-in is not configured")
        store = AlphaStore(postgres_dsn)
        try:
            user = store.authenticate(credentials.username, credentials.password)
        except AlphaAuthenticationError as exc:
            raise HTTPException(status_code=401, detail="The username or password is incorrect.") from exc
        set_session_cookie(response, store.create_session(user))
        return {"username": user.username}

    @router.get("/me")
    def me(principal: str | AlphaUser = Depends(require_auth)) -> dict[str, Any]:
        if not isinstance(principal, AlphaUser) or not principal.org_id:
            # Single-operator deployments have no organization or upload quota.
            username = principal.username if isinstance(principal, AlphaUser) else str(principal)
            return {"username": username, "organization": None, "documents_remaining": None, "document_limit": None}
        return {
            "username": principal.username,
            "organization": _organization_payload(AlphaStore(postgres_dsn or ""), principal),
            "documents_remaining": principal.documents_remaining,
            "document_limit": principal.document_limit,
        }

    @router.get("/upload-limits")
    def upload_limits(_: str | AlphaUser = Depends(require_auth)) -> dict[str, Any]:
        load_dotenv()
        settings = Settings.from_env()
        return {
            "max_upload_bytes": settings.max_upload_bytes,
            "max_pdf_pages": settings.max_pdf_pages,
            "allowed_types": list(settings.allowed_mime_types),
        }

    @router.get("/jobs")
    def recent_jobs(
        limit: int = Query(default=20, ge=1, le=50),
        principal: str | AlphaUser = Depends(require_auth),
    ) -> dict[str, Any]:
        member = _organization_member(principal)
        jobs = AlphaStore(postgres_dsn or "").list_jobs(org_id=member.org_id or "", limit=limit)
        return {"jobs": [job_view(job) for job in jobs]}

    @router.get("/jobs/{job_id}")
    def job_status(job_id: str, principal: str | AlphaUser = Depends(require_auth)) -> dict[str, Any]:
        member = _organization_member(principal)
        try:
            return job_view(AlphaStore(postgres_dsn or "").get_job(job_id, org_id=member.org_id or ""))
        except AlphaNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @router.get("/review")
    def review_queue(principal: str | AlphaUser = Depends(require_auth)) -> dict[str, Any]:
        member = _organization_member(principal)
        return {"items": [review_summary(item) for item in pending_reviews(member.org_id or "")]}

    @router.get("/review/{document_id}")
    def review_detail(document_id: str, principal: str | AlphaUser = Depends(require_auth)) -> dict[str, Any]:
        member = _organization_member(principal)
        try:
            item = load_review_item(document_id, queue_dir=review_queue_dir, org_id=member.org_id)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail="Review item not found") from exc
        metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
        violations = metadata.get("violations") if isinstance(metadata.get("violations"), list) else []
        return {
            "document_id": item["document_id"],
            "status": item.get("status"),
            "created_at": item.get("created_at_utc"),
            "reason_codes": list(item.get("reason_codes") or []),
            "violation_codes": [v.get("code") for v in violations if isinstance(v, dict) and v.get("code")],
            "record": metadata.get("normalized_record") or {},
            "document": review_document(
                metadata.get("source_file_id") or metadata.get("drive_file_id"), member.org_id or ""
            ),
        }

    @router.put("/organization")
    def update_organization(
        changes: OrganizationUpdate,
        principal: str | AlphaUser = Depends(require_auth),
    ) -> dict[str, Any]:
        if not isinstance(principal, AlphaUser) or not principal.org_id:
            raise HTTPException(status_code=403, detail="No organization is available for this account")
        if principal.role != "owner":
            raise HTTPException(status_code=403, detail="Only organization owners can change settings")
        store = AlphaStore(postgres_dsn or "")
        try:
            store.set_base_currency(principal.org_id, changes.base_currency)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except AlphaNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return _organization_payload(store, principal)

    return router


def mount_frontend(app: FastAPI, dist_dir: Path) -> None:
    """Serve the built app under /app, falling back to index.html for client-side routes."""
    index = dist_dir / "index.html"
    if not index.is_file():
        return  # Not built, as in API-only test runs.
    app.mount("/app/assets", StaticFiles(directory=dist_dir / "assets"), name="app-assets")

    # index.html is never cached so a deploy takes effect on the next load; the hashed
    # assets it references can be cached by the browser as usual.
    @app.get("/app", include_in_schema=False)
    @app.get("/app/{client_path:path}", include_in_schema=False)
    def single_page_app(client_path: str = "") -> FileResponse:
        return FileResponse(index, media_type="text/html", headers={"Cache-Control": "no-cache"})
