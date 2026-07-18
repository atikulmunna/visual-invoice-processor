from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

from app.idempotency_store import ClaimResult


class AlphaStoreError(RuntimeError):
    pass


class AlphaAuthenticationError(AlphaStoreError):
    pass


class AlphaQuotaError(AlphaStoreError):
    pass


class AlphaNotFoundError(AlphaStoreError):
    pass


@dataclass(frozen=True)
class AlphaUser:
    id: str
    username: str
    document_limit: int
    documents_used: int
    is_active: bool = True

    @property
    def documents_remaining(self) -> int:
        return max(self.document_limit - self.documents_used, 0)


def normalize_username(username: str) -> str:
    normalized = username.strip().lower()
    if not normalized or len(normalized) > 64:
        raise ValueError("Username must contain between 1 and 64 characters")
    if not all(ch.isalnum() or ch in {"-", "_", "."} for ch in normalized):
        raise ValueError("Username contains unsupported characters")
    return normalized


def hash_password(password: str, *, salt: bytes | None = None) -> str:
    if len(password) < 12:
        raise ValueError("Password must contain at least 12 characters")
    active_salt = salt or secrets.token_bytes(16)
    n, r, p = 2**14, 8, 1
    digest = hashlib.scrypt(password.encode("utf-8"), salt=active_salt, n=n, r=r, p=p, dklen=32)
    return "$".join(
        (
            "scrypt",
            str(n),
            str(r),
            str(p),
            base64.urlsafe_b64encode(active_salt).decode("ascii"),
            base64.urlsafe_b64encode(digest).decode("ascii"),
        )
    )


def verify_password(password: str, encoded: str) -> bool:
    try:
        algorithm, n_text, r_text, p_text, salt_text, digest_text = encoded.split("$", 5)
        if algorithm != "scrypt":
            return False
        salt = base64.urlsafe_b64decode(salt_text.encode("ascii"))
        expected = base64.urlsafe_b64decode(digest_text.encode("ascii"))
        actual = hashlib.scrypt(
            password.encode("utf-8"),
            salt=salt,
            n=int(n_text),
            r=int(r_text),
            p=int(p_text),
            dklen=len(expected),
        )
        return hmac.compare_digest(actual, expected)
    except (ValueError, TypeError):
        return False


def generate_password(length: int = 20) -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789!@#$%"
    return "".join(secrets.choice(alphabet) for _ in range(length))


class AlphaStore:
    def __init__(self, dsn: str) -> None:
        if not dsn:
            raise ValueError("POSTGRES_DSN is required for private-alpha state")
        self._dsn = dsn

    @classmethod
    def from_env(cls) -> "AlphaStore":
        return cls(os.environ.get("POSTGRES_DSN", ""))

    def _connect(self) -> Any:
        try:
            import psycopg
        except ImportError as exc:
            raise RuntimeError("psycopg is required for private-alpha state") from exc
        return psycopg.connect(self._dsn, prepare_threshold=None)

    def create_user(
        self,
        username: str,
        password: str,
        *,
        document_limit: int = 20,
        max_users: int = 10,
    ) -> AlphaUser:
        normalized = normalize_username(username)
        password_digest = hash_password(password)
        user_id = uuid4()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_xact_lock(%s)", (812735,))
                cur.execute("SELECT count(*) FROM public.alpha_users WHERE is_active = true")
                if int(cur.fetchone()[0]) >= max_users:
                    raise AlphaQuotaError(f"The active tester limit of {max_users} has been reached")
                cur.execute(
                    """
                    INSERT INTO public.alpha_users
                      (id, username, password_hash, document_limit)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id, username, document_limit, documents_used, is_active
                    """,
                    (user_id, normalized, password_digest, document_limit),
                )
                row = cur.fetchone()
            conn.commit()
        return AlphaUser(str(row[0]), row[1], int(row[2]), int(row[3]), bool(row[4]))

    def set_user_active(self, username: str, active: bool) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE public.alpha_users SET is_active = %s, updated_at_utc = NOW() WHERE username = %s",
                    (active, normalize_username(username)),
                )
                if cur.rowcount != 1:
                    raise AlphaNotFoundError("Tester account not found")
            conn.commit()

    def reset_user_usage(self, username: str) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE public.alpha_users SET documents_used = 0, updated_at_utc = NOW() WHERE username = %s",
                    (normalize_username(username),),
                )
                if cur.rowcount != 1:
                    raise AlphaNotFoundError("Tester account not found")
            conn.commit()

    def list_users(self) -> list[AlphaUser]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, username, document_limit, documents_used, is_active
                    FROM public.alpha_users ORDER BY username
                    """
                )
                rows = cur.fetchall()
        return [AlphaUser(str(row[0]), row[1], int(row[2]), int(row[3]), bool(row[4])) for row in rows]

    def authenticate(self, username: str, password: str) -> AlphaUser:
        try:
            normalized = normalize_username(username)
        except ValueError as exc:
            raise AlphaAuthenticationError("Invalid credentials") from exc
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, username, password_hash, document_limit, documents_used, is_active
                    FROM public.alpha_users WHERE username = %s
                    """,
                    (normalized,),
                )
                row = cur.fetchone()
        if row is None or not bool(row[5]) or not verify_password(password, row[2]):
            raise AlphaAuthenticationError("Invalid credentials")
        return AlphaUser(str(row[0]), row[1], int(row[3]), int(row[4]), bool(row[5]))

    @staticmethod
    def _session_token_hash(token: str) -> str:
        if not token or len(token) > 256:
            raise AlphaAuthenticationError("Invalid session")
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

    def create_session(self, user: AlphaUser, *, lifetime: timedelta = timedelta(days=7)) -> str:
        token = secrets.token_urlsafe(32)
        token_hash = self._session_token_hash(token)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM public.alpha_sessions WHERE expires_at_utc <= NOW()")
                cur.execute(
                    """
                    INSERT INTO public.alpha_sessions(token_hash, user_id, expires_at_utc)
                    VALUES (%s, %s, NOW() + %s)
                    """,
                    (token_hash, user.id, lifetime),
                )
            conn.commit()
        return token

    def authenticate_session(self, token: str) -> AlphaUser:
        token_hash = self._session_token_hash(token)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT u.id, u.username, u.document_limit, u.documents_used, u.is_active
                    FROM public.alpha_sessions AS s
                    JOIN public.alpha_users AS u ON u.id = s.user_id
                    WHERE s.token_hash = %s
                      AND s.expires_at_utc > NOW()
                      AND u.is_active = true
                    """,
                    (token_hash,),
                )
                row = cur.fetchone()
        if row is None:
            raise AlphaAuthenticationError("Invalid session")
        return AlphaUser(str(row[0]), row[1], int(row[2]), int(row[3]), bool(row[4]))

    def delete_session(self, token: str) -> None:
        try:
            token_hash = self._session_token_hash(token)
        except AlphaAuthenticationError:
            return
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM public.alpha_sessions WHERE token_hash = %s", (token_hash,))
            conn.commit()

    def authorize_upload(
        self,
        user: AlphaUser,
        *,
        object_key: str,
        original_name: str,
        content_type: str,
        declared_size: int,
    ) -> str:
        job_id = str(UUID(object_key.split("/")[-2]))
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT is_active, document_limit, documents_used
                    FROM public.alpha_users WHERE id = %s FOR UPDATE
                    """,
                    (user.id,),
                )
                row = cur.fetchone()
                if row is None or not bool(row[0]):
                    raise AlphaAuthenticationError("Tester account is disabled")
                if int(row[2]) >= int(row[1]):
                    raise AlphaQuotaError("Tester document allowance is exhausted")
                cur.execute(
                    """
                    INSERT INTO public.processing_jobs
                      (id, user_id, object_key, original_name, content_type, declared_size)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (job_id, user.id, object_key, original_name, content_type, declared_size),
                )
                cur.execute(
                    """
                    UPDATE public.alpha_users
                    SET documents_used = documents_used + 1, updated_at_utc = NOW()
                    WHERE id = %s
                    """,
                    (user.id,),
                )
            conn.commit()
        return job_id

    def get_job(self, job_id: str, *, user_id: str | None = None) -> dict[str, Any]:
        params: list[Any] = [job_id]
        owner_clause = ""
        if user_id is not None:
            owner_clause = " AND user_id = %s"
            params.append(user_id)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT id, user_id, object_key, original_name, content_type, declared_size,
                           status, attempts, page_count, document_id, result_json,
                           error_code, error_message, authorized_at_utc, started_at_utc,
                           completed_at_utc
                    FROM public.processing_jobs WHERE id = %s{owner_clause}
                    """,
                    tuple(params),
                )
                row = cur.fetchone()
        if row is None:
            raise AlphaNotFoundError("Processing job not found")
        keys = (
            "id", "user_id", "object_key", "original_name", "content_type", "declared_size",
            "status", "attempts", "page_count", "document_id", "result", "error_code",
            "error_message", "authorized_at_utc", "started_at_utc", "completed_at_utc",
        )
        return {key: value for key, value in zip(keys, row)}

    def claim_job(self, object_key: str, *, max_attempts: int = 3) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE public.processing_jobs
                    SET status = 'PROCESSING', attempts = attempts + 1,
                        started_at_utc = NOW(), updated_at_utc = NOW(),
                        error_code = NULL, error_message = NULL
                    WHERE object_key = %s
                      AND status IN ('AUTHORIZED', 'FAILED')
                      AND attempts < %s
                    RETURNING id, user_id, original_name, content_type, declared_size, attempts
                    """,
                    (object_key, max_attempts),
                )
                row = cur.fetchone()
            conn.commit()
        if row is None:
            return None
        return {
            "id": str(row[0]),
            "user_id": str(row[1]),
            "original_name": row[2],
            "content_type": row[3],
            "declared_size": int(row[4]),
            "attempts": int(row[5]),
        }

    def retry_job(self, job_id: str, *, user_id: str, max_attempts: int = 3) -> str:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE public.processing_jobs
                    SET status = 'AUTHORIZED', updated_at_utc = NOW(),
                        error_code = NULL, error_message = NULL
                    WHERE id = %s AND user_id = %s AND status = 'FAILED' AND attempts < %s
                    RETURNING object_key
                    """,
                    (job_id, user_id, max_attempts),
                )
                row = cur.fetchone()
            conn.commit()
        if row is None:
            raise AlphaQuotaError("This job cannot be retried")
        return str(row[0])

    def reserve_pages(self, job_id: str, page_count: int, *, global_limit: int) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT page_attempts FROM public.alpha_budget WHERE id = 1 FOR UPDATE")
                current = int(cur.fetchone()[0])
                if current + page_count > global_limit:
                    raise AlphaQuotaError("The global page-processing allowance is exhausted")
                cur.execute(
                    "UPDATE public.alpha_budget SET page_attempts = page_attempts + %s, updated_at_utc = NOW() WHERE id = 1",
                    (page_count,),
                )
                cur.execute(
                    "UPDATE public.processing_jobs SET page_count = %s, updated_at_utc = NOW() WHERE id = %s",
                    (page_count, job_id),
                )
            conn.commit()

    def complete_job(
        self,
        job_id: str,
        *,
        status: str,
        result: dict[str, Any] | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE public.processing_jobs
                    SET status = %s, result_json = %s::jsonb,
                        document_id = %s, error_code = %s, error_message = %s,
                        completed_at_utc = NOW(), updated_at_utc = NOW()
                    WHERE id = %s
                    """,
                    (
                        status,
                        json.dumps(result or {}, ensure_ascii=True),
                        (result or {}).get("document_id"),
                        error_code,
                        (error_message or "")[:1000] or None,
                        job_id,
                    ),
                )
            conn.commit()

    def claim_document(self, source_id: str, file_hash: str, owner_id: str) -> ClaimResult:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public.document_claims(file_hash, source_id, status, owner_id)
                    VALUES (%s, %s, 'CLAIMED', %s)
                    ON CONFLICT (file_hash) DO NOTHING
                    RETURNING status
                    """,
                    (file_hash, source_id, owner_id),
                )
                inserted = cur.fetchone()
                if inserted is None:
                    cur.execute(
                        """
                        UPDATE public.document_claims
                        SET source_id = %s, status = 'CLAIMED', owner_id = %s, updated_at_utc = NOW()
                        WHERE file_hash = %s AND status IN ('FAILED', 'REJECTED')
                        RETURNING source_id, status, owner_id
                        """,
                        (source_id, owner_id, file_hash),
                    )
                    reclaimed = cur.fetchone()
                else:
                    reclaimed = None
                if inserted is None and reclaimed is None:
                    cur.execute(
                        "SELECT source_id, status, owner_id FROM public.document_claims WHERE file_hash = %s",
                        (file_hash,),
                    )
                    existing = cur.fetchone()
            conn.commit()
        if inserted is not None:
            return ClaimResult("claimed", source_id, file_hash, owner_id)
        if reclaimed is not None:
            return ClaimResult("claimed", source_id, file_hash, owner_id)
        status = "already_processed" if existing[1] in {"STORED", "ARCHIVED"} else "already_claimed"
        return ClaimResult(status, existing[0], file_hash, existing[2])

    def mark_status(self, source_id: str, file_hash: str, status: str) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE public.document_claims
                    SET status = %s, updated_at_utc = NOW()
                    WHERE file_hash = %s
                    """,
                    (status, file_hash),
                )
            conn.commit()

    def write_failure(self, payload: dict[str, Any]) -> None:
        job_id = payload.get("job_id")
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public.processing_events(job_id, event_type, payload_json)
                    VALUES (%s, 'FAILURE', %s::jsonb)
                    """,
                    (job_id, json.dumps(payload, ensure_ascii=True)),
                )
            conn.commit()
