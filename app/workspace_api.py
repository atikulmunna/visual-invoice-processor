"""JSON endpoints and static hosting for the new single-page workspace served at /app."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Response
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from app.alpha_store import AlphaAuthenticationError, AlphaNotFoundError, AlphaStore, AlphaUser
from app.web_session import set_session_cookie


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


def build_workspace_router(require_auth: Callable[..., Any], postgres_dsn: str | None) -> APIRouter:
    router = APIRouter(prefix="/api")

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
