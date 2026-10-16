"""What reviewers have taught us about each vendor: corrections, a track record, and hints for extraction.

Nothing extra is stored. An approved review keeps the record as extracted and the record as approved,
and the stored record is linked to its vendor, so each vendor's corrections are read from that history
(and survive a vendor merge). Corrections become hints in the extraction prompt for PDFs whose text
names the vendor, and a vendor whose recent reviews needed no changes may skip review when the only
doubt is the model's own low score.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import psycopg

from app.config import Settings
from app.duplicates import normalize_invoice_number
from app.extraction_service import native_pdf_text
from app.vendor_store import find_vendor, normalize_tax_id, normalize_vendor_name

# The vendor's most recent reviews must all have been approved unchanged before it is trusted.
TRUSTED_AFTER = 5
RECENT_REVIEWS = 20
MAX_HINTS = 5
MAX_SHOWN = 10
MAX_HINT_VALUE = 80
MIN_NAME_LENGTH = 3

AMOUNT_FIELDS = ("subtotal", "tax_amount", "shipping_amount", "discount_amount", "total_amount")
CORRECTION_FIELDS = (
    "document_type", "vendor_name", "vendor_tax_id", "invoice_number", "invoice_date", "due_date", "currency",
    "payment_method", *AMOUNT_FIELDS,
)
# What the review form shows for a value the model left empty, so accepting it is not a correction.
FORM_DEFAULTS = {"document_type": "invoice", "payment_method": "unknown"}

logger = logging.getLogger(__name__)

# Approved reviews with the record each one stored, which carries the vendor link.
_APPROVED_REVIEWS = """
    from public.review_queue_items r
    join public.ledger_records lr on lr.id = (r.storage_result ->> 'row_id')::bigint and lr.org_id = r.org_id
    where r.status = 'RESOLVED_STORED'
"""

# One vendor's approved reviews, newest first.
_REVIEWS = f"""
    select r.resolved_at_utc, r.metadata_json -> 'normalized_record', r.resolved_record
    {_APPROVED_REVIEWS} and r.org_id = %s and lr.vendor_id = %s
    order by r.resolved_at_utc desc
    limit %s
"""

# Names of the organization's vendors that reviewers have approved documents for.
_REVIEWED_VENDOR_NAMES = f"""
    select a.vendor_id, a.normalized
    from public.vendor_aliases a
    where a.org_id = %s and exists (select 1 {_APPROVED_REVIEWS} and r.org_id = a.org_id and lr.vendor_id = a.vendor_id)
"""


def _comparable(field: str, value: Any) -> Any:
    """The form a value is compared in, so a change of spelling or format is not counted as a correction."""
    if field in AMOUNT_FIELDS:
        try:
            return round(float(value or 0), 2)
        except (TypeError, ValueError):
            return value
    if field == "vendor_name":
        return normalize_vendor_name(value)
    if field == "vendor_tax_id":
        return normalize_tax_id(value)
    if field == "invoice_number":
        return normalize_invoice_number(value)
    return str(value or FORM_DEFAULTS.get(field, "")).strip().casefold() or None


def corrections_between(extracted: dict[str, Any], approved: dict[str, Any]) -> list[dict[str, Any]]:
    """The fields a reviewer changed, with the value as read and as approved. Line items are not compared."""
    return [
        {"field": field, "before": extracted.get(field), "after": approved.get(field)}
        for field in CORRECTION_FIELDS
        if _comparable(field, extracted.get(field)) != _comparable(field, approved.get(field))
    ]


def _reviews(conn: psycopg.Connection, *, org_id: str, vendor_id: int, limit: int) -> list[dict[str, Any]]:
    rows = conn.execute(_REVIEWS, (org_id, vendor_id, limit)).fetchall()
    return [
        {"reviewed_at": reviewed_at.isoformat(), "corrections": corrections_between(before or {}, after or {})}
        for reviewed_at, before, after in rows
    ]


def is_trusted(reviews: list[dict[str, Any]]) -> bool:
    """True when the latest TRUSTED_AFTER reviews (newest first) all needed no correction."""
    latest = reviews[:TRUSTED_AFTER]
    return len(latest) == TRUSTED_AFTER and not any(review["corrections"] for review in latest)


def vendor_memory(dsn: str, *, org_id: str, vendor_id: int) -> dict[str, Any]:
    """A vendor's recent track record and corrections, for its page."""
    with psycopg.connect(dsn, prepare_threshold=None) as conn:
        reviews = _reviews(conn, org_id=org_id, vendor_id=vendor_id, limit=RECENT_REVIEWS)
    corrections = [
        {**correction, "reviewed_at": review["reviewed_at"]} for review in reviews for correction in review["corrections"]
    ]
    return {
        "reviews": len(reviews),
        "unchanged": sum(1 for review in reviews if not review["corrections"]),
        "trusted": is_trusted(reviews),
        "trusted_after": TRUSTED_AFTER,
        "corrections": corrections[:MAX_SHOWN],
    }


def vendor_in_text(names: list[tuple[int, str]], text: str) -> int | None:
    """The vendor whose name appears in the text as whole words; the longest name wins."""
    words = f" {normalize_vendor_name(text) or ''} "
    found = [(len(name), vendor_id) for vendor_id, name in names if len(name) >= MIN_NAME_LENGTH and f" {name} " in words]
    return max(found)[1] if found else None


def _quoted(value: Any) -> str:
    if value in (None, ""):
        return "empty"
    return json.dumps(str(value)[:MAX_HINT_VALUE], ensure_ascii=False)


def hint_text(reviews: list[dict[str, Any]]) -> str | None:
    """Recent corrections phrased for the extraction prompt, one per field, newest first."""
    latest: dict[str, dict[str, Any]] = {}
    for review in reviews:
        for correction in review["corrections"]:
            latest.setdefault(correction["field"], correction)
    if not latest:
        return None
    lines = [
        f"{field} was read as {_quoted(correction['before'])} but the reviewer corrected it to {_quoted(correction['after'])}"
        for field, correction in list(latest.items())[:MAX_HINTS]
    ]
    return (
        "A reviewer corrected earlier documents from this vendor: " + "; ".join(lines) + ". Those values belong to "
        "the earlier documents, so never copy them; read this document the same way where the same mistake could happen."
    )


def extraction_hints(dsn: str, *, org_id: str, file_path: Path) -> str | None:
    """Hints for a PDF whose text names a vendor with past corrections; None for anything else."""
    with psycopg.connect(dsn, prepare_threshold=None) as conn:
        names = conn.execute(_REVIEWED_VENDOR_NAMES, (org_id,)).fetchall()
        if not names:
            return None
        text = native_pdf_text(file_path)
        vendor_id = vendor_in_text(names, text) if text else None
        if vendor_id is None:
            return None
        return hint_text(_reviews(conn, org_id=org_id, vendor_id=vendor_id, limit=RECENT_REVIEWS))


def vendor_is_trusted(dsn: str, *, org_id: str, record: dict[str, Any]) -> bool:
    with psycopg.connect(dsn, prepare_threshold=None) as conn:
        with conn.cursor() as cur:
            vendor_id = find_vendor(cur, org_id=org_id, name=record.get("vendor_name"), tax_id=record.get("vendor_tax_id"))
        if vendor_id is None:
            return False
        return is_trusted(_reviews(conn, org_id=org_id, vendor_id=vendor_id, limit=TRUSTED_AFTER))


def _memory_available(settings: Settings, org_id: str | None) -> bool:
    return settings.ledger_backend == "postgres" and bool(settings.postgres_dsn) and bool(org_id)


def hints_for_worker(settings: Settings, file_path: Path, org_id: str | None) -> str | None:
    """The worker's lookup. Hints only help, so a failure is logged and extraction goes ahead without them."""
    if not _memory_available(settings, org_id):
        return None
    try:
        return extraction_hints(settings.postgres_dsn or "", org_id=str(org_id), file_path=file_path)
    except Exception:  # noqa: BLE001
        logger.exception("Vendor hints failed; extracting without them")
        return None


def trusted_for_worker(settings: Settings, record: dict[str, Any], org_id: str | None) -> bool:
    """The worker's check. When it fails the document goes to review as it would have anyway."""
    if not _memory_available(settings, org_id):
        return False
    try:
        return vendor_is_trusted(settings.postgres_dsn or "", org_id=str(org_id), record=record)
    except Exception:  # noqa: BLE001
        logger.exception("Vendor track record check failed; sending the document to review")
        return False
