"""Workspace endpoints for the vendor master: the vendor list, one vendor's history, edits, and merges."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Callable

from fastapi import APIRouter, Depends, HTTPException
from fastapi import Path as PathParam
from pydantic import BaseModel, Field

from app import vendor_store
from app.alpha_store import AlphaUser
from app.workspace_api import organization_member

VENDOR_ID = PathParam(ge=1, le=2**63 - 1)


class VendorUpdate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    tax_id: str | None = Field(default=None, max_length=40)
    default_currency: str | None = Field(default=None, max_length=3)


class VendorMerge(BaseModel):
    vendor_ids: list[int] = Field(min_length=1, max_length=50)


def _today() -> date:
    return datetime.now(timezone.utc).date()


def build_vendor_router(require_auth: Callable[..., Any], postgres_dsn: str | None) -> APIRouter:
    router = APIRouter(prefix="/api/vendors")
    dsn = postgres_dsn or ""

    def detail(org_id: str, vendor_id: int) -> dict[str, Any]:
        try:
            return vendor_store.vendor_detail(dsn, org_id=org_id, vendor_id=vendor_id, today=_today())
        except vendor_store.VendorNotFound as exc:
            raise HTTPException(status_code=404, detail="Vendor not found") from exc

    @router.get("")
    def vendors(principal: str | AlphaUser = Depends(require_auth)) -> dict[str, Any]:
        member = organization_member(principal)
        rows = vendor_store.list_vendors(dsn, org_id=member.org_id or "")
        return {"vendors": rows, "suggestions": vendor_store.suggest_merges(rows)}

    @router.get("/{vendor_id}")
    def vendor(vendor_id: int = VENDOR_ID, principal: str | AlphaUser = Depends(require_auth)) -> dict[str, Any]:
        return detail(organization_member(principal).org_id or "", vendor_id)

    @router.put("/{vendor_id}")
    def update_vendor(
        changes: VendorUpdate,
        vendor_id: int = VENDOR_ID,
        principal: str | AlphaUser = Depends(require_auth),
    ) -> dict[str, Any]:
        org_id = organization_member(principal).org_id or ""
        try:
            vendor_store.update_vendor(
                dsn,
                org_id=org_id,
                vendor_id=vendor_id,
                name=changes.name,
                tax_id=changes.tax_id,
                default_currency=changes.default_currency,
            )
        except vendor_store.VendorNotFound as exc:
            raise HTTPException(status_code=404, detail="Vendor not found") from exc
        except vendor_store.VendorConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return detail(org_id, vendor_id)

    @router.post("/{vendor_id}/merge")
    def merge_into(
        payload: VendorMerge,
        vendor_id: int = VENDOR_ID,
        principal: str | AlphaUser = Depends(require_auth),
    ) -> dict[str, Any]:
        """Merges the listed vendors into this one, which keeps its name."""
        org_id = organization_member(principal).org_id or ""
        try:
            vendor_store.merge_vendors(dsn, org_id=org_id, keep_id=vendor_id, merge_ids=payload.vendor_ids)
        except vendor_store.VendorNotFound as exc:
            raise HTTPException(status_code=404, detail="Vendor not found") from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return detail(org_id, vendor_id)

    return router
