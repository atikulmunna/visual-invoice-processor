"""Documents waiting for a person: deciding when to send one, and the review items in Postgres.

The table comes from the migrations. org_id None means an unscoped operator, who sees every
organization's items; every workspace request passes its organization.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import psycopg

from app.storage_service import append_record
from app.validation import REQUIRED_FIELDS, missing_required_fields, validate_and_score


@dataclass(frozen=True)
class ReviewDecision:
    status: str
    reason_codes: tuple[str, ...]


_SELECT_COLUMNS = """
    r.document_id, r.status, r.reason_codes, r.metadata_json, r.created_at_utc, r.resolved_at_utc,
    r.resolved_record, r.storage_result, r.resolution_note, r.org_id
"""


def _row_to_item(row: Any) -> dict[str, Any]:
    return {
        "document_id": row[0],
        "status": row[1],
        "reason_codes": row[2] or [],
        "metadata": row[3] or {},
        "created_at_utc": row[4].isoformat() if row[4] else None,
        "resolved_at_utc": row[5].isoformat() if row[5] else None,
        "resolved_record": row[6],
        "storage_result": row[7],
        "resolution_note": row[8],
        "org_id": str(row[9]) if row[9] else None,
    }


def _org_clause(org_id: str | None) -> tuple[str, tuple[Any, ...]]:
    return ("", ()) if org_id is None else (" and r.org_id = %s", (org_id,))


def _connect(dsn: str) -> psycopg.Connection:
    return psycopg.connect(dsn, prepare_threshold=None)


def decide_review_status(
    is_valid: bool,
    model_confidence: float,
    *,
    confidence_threshold: float = 0.85,
    checked_against_text: bool = False,
) -> ReviewDecision:
    """checked_against_text means each value was verified against the document's own text, which
    replaces the model's opinion of itself; only documents without text fall back to it."""
    reasons: list[str] = []
    if not is_valid:
        reasons.append("validation_failed")
    if model_confidence < confidence_threshold and not checked_against_text:
        reasons.append("low_confidence")
    if reasons:
        return ReviewDecision(status="REVIEW_REQUIRED", reason_codes=tuple(reasons))
    return ReviewDecision(status="VALIDATED", reason_codes=tuple())


def route_to_review_queue(
    dsn: str,
    *,
    document_id: str,
    reason_codes: list[str],
    metadata: dict[str, Any],
    org_id: str | None,
) -> dict[str, Any]:
    created_at = datetime.now(timezone.utc).isoformat()
    with _connect(dsn) as conn:
        conn.execute(
            """
            insert into public.review_queue_items
                (document_id, status, reason_codes, metadata_json, created_at_utc, org_id)
            values (%s, 'REVIEW_REQUIRED', %s::jsonb, %s::jsonb, %s, %s)
            on conflict (document_id) do update set
                status = excluded.status,
                reason_codes = excluded.reason_codes,
                metadata_json = excluded.metadata_json,
                created_at_utc = excluded.created_at_utc,
                org_id = excluded.org_id
            """,
            (document_id, json.dumps(reason_codes), json.dumps(metadata, ensure_ascii=True), created_at, org_id),
        )
    return {
        "document_id": document_id,
        "status": "REVIEW_REQUIRED",
        "reason_codes": reason_codes,
        "created_at_utc": created_at,
        "metadata": metadata,
        "org_id": org_id,
    }


def pending_review_items(dsn: str, *, org_id: str | None) -> list[dict[str, Any]]:
    """Items still waiting, oldest first. One whose file was stored some other way is no longer waiting."""
    org_sql, org_params = _org_clause(org_id)
    with _connect(dsn) as conn:
        rows = conn.execute(
            f"""
            select {_SELECT_COLUMNS}
            from public.review_queue_items r
            where r.status = 'REVIEW_REQUIRED'{org_sql}
              and not exists (
                  select 1 from public.ledger_records lr
                  where lr.file_hash = r.metadata_json ->> 'file_hash'
                    and lr.status in ('STORED', 'ARCHIVED')
                    and lr.org_id is not distinct from r.org_id
              )
            order by r.created_at_utc
            """,
            org_params,
        ).fetchall()
    return [_row_to_item(row) for row in rows]


def load_review_item(dsn: str, document_id: str, *, org_id: str | None) -> dict[str, Any]:
    org_sql, org_params = _org_clause(org_id)
    with _connect(dsn) as conn:
        row = conn.execute(
            f"select {_SELECT_COLUMNS} from public.review_queue_items r where r.document_id = %s{org_sql}",
            (document_id, *org_params),
        ).fetchone()
    if row is None:
        raise FileNotFoundError(f"Review item not found: {document_id}")
    return _row_to_item(row)


def _mark_resolved(
    dsn: str,
    document_id: str,
    *,
    resolution_status: str,
    resolved_record: dict[str, Any] | None,
    storage_result: dict[str, Any],
    note: str | None,
    org_id: str | None,
) -> dict[str, Any]:
    org_sql, org_params = _org_clause(org_id)
    with _connect(dsn) as conn:
        updated = conn.execute(
            f"""
            update public.review_queue_items r
            set status = %s, resolved_at_utc = %s, resolved_record = %s::jsonb, storage_result = %s::jsonb,
                resolution_note = %s
            where r.document_id = %s{org_sql}
            """,
            (
                resolution_status,
                datetime.now(timezone.utc).isoformat(),
                json.dumps(resolved_record, ensure_ascii=True),
                json.dumps(storage_result, ensure_ascii=True),
                note,
                document_id,
                *org_params,
            ),
        )
        if updated.rowcount == 0:
            raise FileNotFoundError(f"Review item not found: {document_id}")
    return load_review_item(dsn, document_id, org_id=org_id)


def dismiss_review_item(
    dsn: str,
    document_id: str,
    *,
    resolution_status: str,
    note: str | None = None,
    org_id: str | None,
) -> dict[str, Any]:
    if resolution_status not in {"REJECTED", "RESOLVED_DUPLICATE_MANUAL"}:
        raise ValueError(f"Unsupported dismissal status: {resolution_status}")
    storage_result = {"status": "dismissed", "action": resolution_status}
    updated = _mark_resolved(
        dsn,
        document_id,
        resolution_status=resolution_status,
        resolved_record=None,
        storage_result=storage_result,
        note=note,
        org_id=org_id,
    )
    return {"review_item": updated, "storage_result": storage_result, "resolved_record": None}


def resolve_review_item(
    dsn: str,
    document_id: str,
    *,
    record_override: dict[str, Any] | None = None,
    note: str | None = None,
    org_id: str | None,
) -> dict[str, Any]:
    """Approves a review item: the reviewer's record (or the one as read) is validated and stored."""
    review_item = load_review_item(dsn, document_id, org_id=org_id)
    if review_item["status"] != "REVIEW_REQUIRED":
        raise ValueError(f"Review item {document_id} is not active; current status={review_item['status']}")
    metadata = review_item["metadata"]
    record = record_override if record_override is not None else metadata.get("normalized_record")
    if not isinstance(record, dict):
        raise ValueError("Review item does not contain a normalized_record. Supply a corrected record instead.")
    missing = missing_required_fields(record)
    if missing:
        labels = ", ".join(REQUIRED_FIELDS[field][1] for field in missing)
        raise ValueError(f"Fill in the {labels} before approving this document")
    source_id = metadata.get("source_file_id") or metadata.get("drive_file_id")
    file_hash = metadata.get("file_hash")
    if not source_id or not file_hash:
        raise ValueError("Review item metadata is missing source_file_id/drive_file_id or file_hash")

    validation = validate_and_score(record)
    resolved_record = validation["record"].model_dump(mode="json")
    resolved_record["validation_score"] = validation["validation_score"]
    resolved_record["needs_review"] = False
    append_result = append_record(
        dsn,
        record=resolved_record,
        metadata={
            "document_id": document_id,
            "drive_file_id": source_id,
            "file_hash": file_hash,
            "status": "STORED",
            "processed_at_utc": datetime.now(timezone.utc).isoformat(),
            "needs_review": False,
            "used_provider": metadata.get("used_provider", "manual_review"),
            "resolution_source": "manual_review",
            "org_id": review_item["org_id"],
        },
    )
    updated = _mark_resolved(
        dsn,
        document_id,
        resolution_status="RESOLVED_STORED" if append_result["status"] == "appended" else "RESOLVED_DUPLICATE",
        resolved_record=resolved_record,
        storage_result=append_result,
        note=note,
        org_id=org_id,
    )
    return {"review_item": updated, "storage_result": append_result, "resolved_record": resolved_record}
