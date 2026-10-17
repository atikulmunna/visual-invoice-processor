"""Stored records in Postgres. The table comes from the migrations."""

from __future__ import annotations

import json
from typing import Any

import psycopg

from app.payables import initial_payment
from app.vendor_store import link_vendor


class StorageError(RuntimeError):
    pass


def _link_vendor(conn: psycopg.Connection, record_id: int, org_id: str, record: dict[str, Any]) -> None:
    """Ties a new record to its vendor in the same transaction, so a stored record is never left unlinked."""
    with conn.cursor() as cur:
        vendor_id = link_vendor(
            cur,
            org_id=org_id,
            name=record.get("vendor_name"),
            tax_id=record.get("vendor_tax_id"),
            currency=record.get("currency"),
        )
        if vendor_id is not None:
            cur.execute("update public.ledger_records set vendor_id = %s where id = %s", (vendor_id, record_id))


def append_record(dsn: str, *, record: dict[str, Any], metadata: dict[str, Any]) -> dict[str, Any]:
    """Stores a record once per source file and hash, linked to its vendor in the same transaction."""
    source_id = metadata.get("drive_file_id")
    file_hash = metadata.get("file_hash")
    if not source_id or not file_hash:
        raise StorageError("Storing a record requires metadata.drive_file_id and metadata.file_hash")

    org_id = metadata.get("org_id")
    payment_status, payment_date = initial_payment(record)
    with psycopg.connect(dsn, prepare_threshold=None) as conn:
        row = conn.execute(
            """
            insert into public.ledger_records
                (drive_file_id, file_hash, status, record_json, metadata_json, processed_at_utc, org_id,
                 payment_status, payment_date)
            values (%s, %s, %s, %s::jsonb, %s::jsonb, now(), %s, %s, %s)
            on conflict (drive_file_id, file_hash) do nothing
            returning id
            """,
            (
                source_id,
                file_hash,
                metadata.get("status", "STORED"),
                json.dumps(record, ensure_ascii=True),
                json.dumps(metadata, ensure_ascii=True),
                org_id,
                payment_status,
                payment_date,
            ),
        ).fetchone()
        if row is not None and org_id:
            _link_vendor(conn, int(row[0]), str(org_id), record)

    result = {"drive_file_id": source_id, "file_hash": file_hash}
    if row is None:
        return {"status": "skipped_duplicate", **result}
    return {"status": "appended", "row_id": int(row[0]), **result}
