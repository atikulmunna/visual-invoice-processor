from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ReviewDecision:
    status: str
    reason_codes: tuple[str, ...]


def _settings_or_none() -> Any | None:
    try:
        from app.config import Settings, load_dotenv

        load_dotenv()
        return Settings.from_env()
    except Exception:  # noqa: BLE001
        return None


def _queue_backend(queue_dir: str | Path) -> str:
    queue_path = str(queue_dir)
    if queue_path != "review_queue":
        return "filesystem"

    settings = _settings_or_none()
    if settings is None:
        return "filesystem"

    import os

    configured = os.getenv("REVIEW_QUEUE_BACKEND", "auto").strip().lower()
    if configured in {"filesystem", "postgres"}:
        return configured
    if settings.ledger_backend == "postgres" and settings.postgres_dsn:
        return "postgres"
    return "filesystem"


_SELECT_COLUMNS = """
    document_id,
    status,
    reason_codes,
    metadata_json,
    source_file_moved_to,
    created_at_utc,
    resolved_at_utc,
    resolved_record,
    storage_result,
    resolution_note,
    org_id
"""


def _row_to_item(row: Any) -> dict[str, Any]:
    return {
        "document_id": row[0],
        "status": row[1],
        "reason_codes": row[2] or [],
        "metadata": row[3] or {},
        "source_file_moved_to": row[4],
        "created_at_utc": row[5].isoformat() if row[5] else None,
        "resolved_at_utc": row[6].isoformat() if row[6] else None,
        "resolved_record": row[7],
        "storage_result": row[8],
        "resolution_note": row[9],
        "org_id": str(row[10]) if row[10] else None,
    }


def _org_clause(org_id: str | None, *, prefix: str) -> tuple[str, tuple[Any, ...]]:
    """SQL filter for an organization; None means the caller is an unscoped operator."""
    if org_id is None:
        return "", ()
    return f" {prefix} org_id = %s", (org_id,)


class PostgresReviewQueueStore:
    def __init__(self, dsn: str, table_name: str = "review_queue_items") -> None:
        self._dsn = dsn
        self._table = table_name
        self._ensure_schema()

    def _connect(self) -> Any:
        try:
            import psycopg
        except ImportError as exc:
            raise RuntimeError("psycopg package is required for PostgreSQL review queue backend") from exc
        return psycopg.connect(self._dsn, prepare_threshold=None)

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self._table} (
                        document_id TEXT PRIMARY KEY,
                        status TEXT NOT NULL,
                        reason_codes JSONB NOT NULL DEFAULT '[]'::jsonb,
                        metadata_json JSONB,
                        source_file_moved_to TEXT,
                        created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        resolved_at_utc TIMESTAMPTZ,
                        resolved_record JSONB,
                        storage_result JSONB,
                        resolution_note TEXT,
                        org_id UUID
                    )
                    """
                )
            conn.commit()

    def create_item(
        self,
        *,
        document_id: str,
        reason_codes: list[str],
        moved_file: str | None,
        metadata: dict[str, Any] | None,
        org_id: str | None = None,
    ) -> dict[str, Any]:
        created_at = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self._table}
                        (document_id, status, reason_codes, metadata_json, source_file_moved_to,
                         created_at_utc, org_id)
                    VALUES
                        (%s, %s, %s::jsonb, %s::jsonb, %s, %s, %s)
                    ON CONFLICT (document_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        reason_codes = EXCLUDED.reason_codes,
                        metadata_json = EXCLUDED.metadata_json,
                        source_file_moved_to = EXCLUDED.source_file_moved_to,
                        created_at_utc = EXCLUDED.created_at_utc,
                        org_id = EXCLUDED.org_id
                    """,
                    (
                        document_id,
                        "REVIEW_REQUIRED",
                        json.dumps(reason_codes, ensure_ascii=True),
                        json.dumps(metadata or {}, ensure_ascii=True),
                        moved_file,
                        created_at,
                        org_id,
                    ),
                )
            conn.commit()
        return {
            "document_id": document_id,
            "status": "REVIEW_REQUIRED",
            "reason_codes": reason_codes,
            "created_at_utc": created_at,
            "source_file_moved_to": moved_file,
            "metadata": metadata or {},
            "org_id": org_id,
        }

    def list_items(self, org_id: str | None = None) -> list[dict[str, Any]]:
        org_sql, org_params = _org_clause(org_id, prefix="WHERE")
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT {_SELECT_COLUMNS}
                    FROM {self._table}{org_sql}
                    ORDER BY created_at_utc ASC
                    """,
                    org_params,
                )
                rows = cur.fetchall()
        return [_row_to_item(row) for row in rows]

    def load_item(self, document_id: str, org_id: str | None = None) -> dict[str, Any]:
        org_sql, org_params = _org_clause(org_id, prefix="AND")
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT {_SELECT_COLUMNS}
                    FROM {self._table}
                    WHERE document_id = %s{org_sql}
                    """,
                    (document_id, *org_params),
                )
                row = cur.fetchone()
        if row is None:
            raise FileNotFoundError(f"Review item not found: {document_id}")
        return _row_to_item(row)

    def mark_resolved(
        self,
        *,
        document_id: str,
        resolution_status: str,
        resolved_record: dict[str, Any] | None,
        storage_result: dict[str, Any] | None,
        note: str | None,
        org_id: str | None = None,
    ) -> dict[str, Any]:
        resolved_at = datetime.now(timezone.utc).isoformat()
        org_sql, org_params = _org_clause(org_id, prefix="AND")
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {self._table}
                    SET
                        status = %s,
                        resolved_at_utc = %s,
                        resolved_record = %s::jsonb,
                        storage_result = %s::jsonb,
                        resolution_note = %s
                    WHERE document_id = %s{org_sql}
                    """,
                    (
                        resolution_status,
                        resolved_at,
                        json.dumps(resolved_record, ensure_ascii=True) if resolved_record is not None else "null",
                        json.dumps(storage_result, ensure_ascii=True) if storage_result is not None else "null",
                        note,
                        document_id,
                        *org_params,
                    ),
                )
                if cur.rowcount == 0:
                    raise FileNotFoundError(f"Review item not found: {document_id}")
            conn.commit()
        return self.load_item(document_id, org_id=org_id)


def _postgres_store() -> PostgresReviewQueueStore:
    settings = _settings_or_none()
    if settings is None or not settings.postgres_dsn:
        raise ValueError("POSTGRES_DSN is required for postgres review queue backend")

    import os

    table_name = os.getenv("REVIEW_QUEUE_TABLE", "review_queue_items")
    return PostgresReviewQueueStore(dsn=settings.postgres_dsn, table_name=table_name)


def decide_review_status(
    is_valid: bool,
    model_confidence: float,
    *,
    confidence_threshold: float = 0.85,
) -> ReviewDecision:
    reasons: list[str] = []
    if not is_valid:
        reasons.append("validation_failed")
    if model_confidence < confidence_threshold:
        reasons.append("low_confidence")
    if reasons:
        return ReviewDecision(status="REVIEW_REQUIRED", reason_codes=tuple(reasons))
    return ReviewDecision(status="VALIDATED", reason_codes=tuple())


def route_to_review_queue(
    document_id: str,
    reason_codes: list[str],
    *,
    queue_dir: str | Path = "review_queue",
    source_file: str | Path | None = None,
    metadata: dict[str, Any] | None = None,
    org_id: str | None = None,
) -> dict[str, Any]:
    moved_file = None
    backend = _queue_backend(queue_dir)
    if backend == "filesystem":
        target_dir = Path(queue_dir)
        target_dir.mkdir(parents=True, exist_ok=True)

        if source_file is not None:
            source_path = Path(source_file)
            destination = target_dir / source_path.name
            if source_path.exists():
                shutil.move(str(source_path), str(destination))
                moved_file = str(destination)
    elif source_file is not None:
        moved_file = str(source_file)

    if backend == "postgres":
        return _postgres_store().create_item(
            document_id=document_id,
            reason_codes=reason_codes,
            moved_file=moved_file,
            metadata=metadata,
            org_id=org_id,
        )

    record = {
        "document_id": document_id,
        "status": "REVIEW_REQUIRED",
        "reason_codes": reason_codes,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_file_moved_to": moved_file,
        "org_id": org_id,
    }
    if metadata:
        record["metadata"] = metadata

    record_file = Path(queue_dir) / f"{document_id}.json"
    record_file.write_text(json.dumps(record, ensure_ascii=True, indent=2), encoding="utf-8")
    return record


def _in_org(payload: dict[str, Any], org_id: str | None) -> bool:
    return org_id is None or payload.get("org_id") == org_id


def list_review_items(
    queue_dir: str | Path = "review_queue",
    *,
    org_id: str | None = None,
) -> list[dict[str, Any]]:
    if _queue_backend(queue_dir) == "postgres":
        return _postgres_store().list_items(org_id=org_id)

    target_dir = Path(queue_dir)
    if not target_dir.exists():
        return []

    items: list[dict[str, Any]] = []
    for record_file in sorted(target_dir.glob("*.json")):
        if not record_file.is_file():
            continue
        try:
            payload = json.loads(record_file.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        if not _in_org(payload, org_id):
            continue
        payload["_record_path"] = str(record_file)
        items.append(payload)
    return items


def _review_record_file(queue_dir: str | Path, document_id: str) -> Path:
    if not document_id or Path(document_id).name != document_id:
        raise FileNotFoundError(f"Review item not found: {document_id}")
    return Path(queue_dir) / f"{document_id}.json"


def load_review_item(
    document_id: str,
    queue_dir: str | Path = "review_queue",
    *,
    org_id: str | None = None,
) -> dict[str, Any]:
    if _queue_backend(queue_dir) == "postgres":
        return _postgres_store().load_item(document_id, org_id=org_id)

    record_file = _review_record_file(queue_dir, document_id)
    if not record_file.exists():
        raise FileNotFoundError(f"Review item not found: {document_id}")
    payload = json.loads(record_file.read_text(encoding="utf-8"))
    if not _in_org(payload, org_id):
        raise FileNotFoundError(f"Review item not found: {document_id}")
    payload["_record_path"] = str(record_file)
    return payload


def mark_review_resolved(
    document_id: str,
    *,
    queue_dir: str | Path = "review_queue",
    resolution_status: str,
    resolved_record: dict[str, Any] | None,
    storage_result: dict[str, Any] | None,
    note: str | None = None,
    org_id: str | None = None,
) -> dict[str, Any]:
    if _queue_backend(queue_dir) == "postgres":
        return _postgres_store().mark_resolved(
            document_id=document_id,
            resolution_status=resolution_status,
            resolved_record=resolved_record,
            storage_result=storage_result,
            note=note,
            org_id=org_id,
        )

    record_file = _review_record_file(queue_dir, document_id)
    if not record_file.exists():
        raise FileNotFoundError(f"Review item not found: {document_id}")

    payload = json.loads(record_file.read_text(encoding="utf-8"))
    if not _in_org(payload, org_id):
        raise FileNotFoundError(f"Review item not found: {document_id}")
    payload["status"] = resolution_status
    payload["resolved_at_utc"] = datetime.now(timezone.utc).isoformat()
    payload["resolved_record"] = resolved_record
    payload["storage_result"] = storage_result
    if note:
        payload["resolution_note"] = note

    record_file.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    return payload


def dismiss_review_item(
    document_id: str,
    *,
    queue_dir: str | Path = "review_queue",
    resolution_status: str,
    note: str | None = None,
    org_id: str | None = None,
) -> dict[str, Any]:
    if resolution_status not in {"REJECTED", "RESOLVED_DUPLICATE_MANUAL"}:
        raise ValueError(f"Unsupported dismissal status: {resolution_status}")

    updated = mark_review_resolved(
        document_id=document_id,
        queue_dir=queue_dir,
        resolution_status=resolution_status,
        resolved_record=None,
        storage_result={"status": "dismissed", "action": resolution_status},
        note=note,
        org_id=org_id,
    )
    return {
        "review_item": updated,
        "storage_result": updated.get("storage_result"),
        "resolved_record": None,
    }


def _load_resolution_record(
    review_item: dict[str, Any],
    record_path: str | None,
    record_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if record_override is not None:
        return record_override
    if record_path:
        payload = json.loads(Path(record_path).read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("Resolved review record JSON must be an object")
        return payload

    metadata = review_item.get("metadata", {}) if isinstance(review_item.get("metadata"), dict) else {}
    payload = metadata.get("normalized_record")
    if not isinstance(payload, dict):
        raise ValueError(
            "Review item does not contain a normalized_record. Supply a corrected JSON record instead."
        )
    return payload


def resolve_review_item(
    document_id: str,
    *,
    queue_dir: str | Path = "review_queue",
    record_path: str | None = None,
    record_override: dict[str, Any] | None = None,
    note: str | None = None,
    org_id: str | None = None,
) -> dict[str, Any]:
    from datetime import datetime, timezone

    from app.storage_service import append_record
    from app.validation import REQUIRED_FIELDS, missing_required_fields, validate_and_score

    review_item = load_review_item(document_id=document_id, queue_dir=queue_dir, org_id=org_id)
    if review_item.get("status") != "REVIEW_REQUIRED":
        raise ValueError(f"Review item {document_id} is not active; current status={review_item.get('status')}")

    record = _load_resolution_record(review_item, record_path, record_override)
    missing = missing_required_fields(record)
    if missing:
        labels = ", ".join(REQUIRED_FIELDS[field][1] for field in missing)
        raise ValueError(f"Fill in the {labels} before approving this document")
    validation = validate_and_score(record)
    resolved_record = validation["record"].model_dump(mode="json")
    resolved_record["validation_score"] = validation["validation_score"]
    resolved_record["needs_review"] = False

    metadata = review_item.get("metadata", {}) if isinstance(review_item.get("metadata"), dict) else {}
    drive_file_id = metadata.get("source_file_id") or metadata.get("drive_file_id")
    file_hash = metadata.get("file_hash")
    if not drive_file_id or not file_hash:
        raise ValueError("Review item metadata is missing source_file_id/drive_file_id or file_hash")

    append_metadata = {
        "document_id": document_id,
        "drive_file_id": drive_file_id,
        "file_hash": file_hash,
        "status": "STORED",
        "processed_at_utc": datetime.now(timezone.utc).isoformat(),
        "needs_review": False,
        "used_provider": metadata.get("used_provider", "manual_review"),
        "resolution_source": "manual_review",
        "org_id": review_item.get("org_id"),
    }

    append_result = append_record(record=resolved_record, metadata=append_metadata)
    resolution_status = "RESOLVED_STORED" if append_result.get("status") == "appended" else "RESOLVED_DUPLICATE"
    updated = mark_review_resolved(
        document_id=document_id,
        queue_dir=queue_dir,
        resolution_status=resolution_status,
        resolved_record=resolved_record,
        storage_result=append_result,
        note=note,
        org_id=org_id,
    )
    return {
        "review_item": updated,
        "storage_result": append_result,
        "resolved_record": resolved_record,
    }
