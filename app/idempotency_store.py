from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class ClaimResult:
    status: str
    drive_file_id: str
    file_hash: str
    owner_id: str | None = None


class DocumentClaimStore:
    def __init__(self, db_path: str | Path = "data/metadata.db") -> None:
        self._db_path = str(db_path)
        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, timeout=10, isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS document_claims (
                    drive_file_id TEXT NOT NULL,
                    file_hash TEXT NOT NULL,
                    status TEXT NOT NULL,
                    owner_id TEXT,
                    claimed_at_utc TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL,
                    PRIMARY KEY (drive_file_id, file_hash)
                )
                """
            )

    def claim_document(self, drive_file_id: str, file_hash: str, owner_id: str) -> ClaimResult:
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO document_claims
                (drive_file_id, file_hash, status, owner_id, claimed_at_utc, updated_at_utc)
                VALUES (?, ?, 'CLAIMED', ?, ?, ?)
                """,
                (drive_file_id, file_hash, owner_id, now, now),
            )
            if cursor.rowcount == 1:
                conn.execute("COMMIT")
                return ClaimResult(
                    status="claimed",
                    drive_file_id=drive_file_id,
                    file_hash=file_hash,
                    owner_id=owner_id,
                )

            row = conn.execute(
                """
                SELECT status, owner_id FROM document_claims
                WHERE drive_file_id = ? AND file_hash = ?
                """,
                (drive_file_id, file_hash),
            ).fetchone()

            if row and row[0] in {"FAILED", "REVIEW_REQUIRED"}:
                conn.execute(
                    """
                    UPDATE document_claims
                    SET status = 'CLAIMED', owner_id = ?, updated_at_utc = ?
                    WHERE drive_file_id = ? AND file_hash = ?
                    """,
                    (owner_id, now, drive_file_id, file_hash),
                )
                conn.execute("COMMIT")
                return ClaimResult(
                    status="claimed",
                    drive_file_id=drive_file_id,
                    file_hash=file_hash,
                    owner_id=owner_id,
                )
            conn.execute("COMMIT")

        if not row:
            return ClaimResult(
                status="already_claimed",
                drive_file_id=drive_file_id,
                file_hash=file_hash,
            )

        current_status, existing_owner = row
        if current_status in {"STORED", "ARCHIVED"}:
            return ClaimResult(
                status="already_processed",
                drive_file_id=drive_file_id,
                file_hash=file_hash,
                owner_id=existing_owner,
            )
        return ClaimResult(
            status="already_claimed",
            drive_file_id=drive_file_id,
            file_hash=file_hash,
            owner_id=existing_owner,
        )

    def mark_status(self, drive_file_id: str, file_hash: str, status: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE document_claims
                SET status = ?, updated_at_utc = ?
                WHERE drive_file_id = ? AND file_hash = ?
                """,
                (status, now, drive_file_id, file_hash),
            )
