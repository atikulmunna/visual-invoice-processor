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


ORG_ROLES = ("owner", "member")


@dataclass(frozen=True)
class AlphaUser:
    id: str
    username: str
    document_limit: int
    documents_used: int
    is_active: bool = True
    org_id: str | None = None
    org_name: str | None = None
    role: str | None = None

    @property
    def documents_remaining(self) -> int:
        return max(self.document_limit - self.documents_used, 0)


@dataclass(frozen=True)
class Organization:
    id: str
    name: str
    members: tuple[tuple[str, str], ...] = ()
    base_currency: str = "BDT"


# Picks the session's organization, or the user's first membership when the session has none.
_ACTIVE_MEMBERSHIP_SQL = """
    JOIN LATERAL (
        SELECT m.org_id, m.role
        FROM public.memberships AS m
        WHERE m.user_id = u.id AND ({session_org} IS NULL OR m.org_id = {session_org})
        ORDER BY m.created_at_utc, m.org_id
        LIMIT 1
    ) AS m ON true
    JOIN public.organizations AS o ON o.id = m.org_id
"""


def _user_from_row(row: Any) -> AlphaUser:
    return AlphaUser(
        str(row[0]),
        row[1],
        int(row[2]),
        int(row[3]),
        bool(row[4]),
        org_id=str(row[5]),
        org_name=row[6],
        role=row[7],
    )


def normalize_org_name(name: str) -> str:
    normalized = " ".join(name.split())
    if not normalized or len(normalized) > 120:
        raise ValueError("Organization name must contain between 1 and 120 characters")
    return normalized


def _require_uuid(value: str, error: AlphaStoreError) -> None:
    try:
        UUID(str(value))
    except ValueError as exc:
        raise error from exc


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
                org_id = uuid4()
                cur.execute(
                    "INSERT INTO public.organizations (id, name) VALUES (%s, %s)",
                    (org_id, normalized),
                )
                cur.execute(
                    "INSERT INTO public.memberships (org_id, user_id, role) VALUES (%s, %s, 'owner')",
                    (org_id, user_id),
                )
            conn.commit()
        return _user_from_row((*row, org_id, normalized, "owner"))

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

    def set_password(self, username: str, password: str) -> None:
        """Replace a tester's password and sign them out everywhere."""
        password_digest = hash_password(password)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE public.alpha_users SET password_hash = %s, updated_at_utc = NOW()
                    WHERE username = %s
                    RETURNING id
                    """,
                    (password_digest, normalize_username(username)),
                )
                row = cur.fetchone()
                if row is None:
                    raise AlphaNotFoundError("Tester account not found")
                cur.execute("DELETE FROM public.alpha_sessions WHERE user_id = %s", (row[0],))
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

    @staticmethod
    def _user_id(cur: Any, username: str) -> Any:
        cur.execute("SELECT id FROM public.alpha_users WHERE username = %s", (normalize_username(username),))
        row = cur.fetchone()
        if row is None:
            raise AlphaNotFoundError("Tester account not found")
        return row[0]

    @staticmethod
    def _require_org(cur: Any, org_id: str) -> None:
        _require_uuid(org_id, AlphaNotFoundError("Organization not found"))
        cur.execute("SELECT 1 FROM public.organizations WHERE id = %s", (org_id,))
        if cur.fetchone() is None:
            raise AlphaNotFoundError("Organization not found")

    def create_organization(self, name: str, *, owner_username: str) -> Organization:
        org_name = normalize_org_name(name)
        org_id = uuid4()
        with self._connect() as conn:
            with conn.cursor() as cur:
                user_id = self._user_id(cur, owner_username)
                cur.execute(
                    "INSERT INTO public.organizations (id, name) VALUES (%s, %s)",
                    (org_id, org_name),
                )
                cur.execute(
                    "INSERT INTO public.memberships (org_id, user_id, role) VALUES (%s, %s, 'owner')",
                    (org_id, user_id),
                )
            conn.commit()
        return Organization(str(org_id), org_name, ((normalize_username(owner_username), "owner"),))

    def add_member(self, org_id: str, username: str, *, role: str = "member") -> None:
        if role not in ORG_ROLES:
            raise ValueError(f"Role must be one of: {', '.join(ORG_ROLES)}")
        with self._connect() as conn:
            with conn.cursor() as cur:
                self._require_org(cur, org_id)
                user_id = self._user_id(cur, username)
                cur.execute(
                    """
                    INSERT INTO public.memberships (org_id, user_id, role)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (org_id, user_id) DO UPDATE SET role = EXCLUDED.role
                    """,
                    (org_id, user_id, role),
                )
            conn.commit()

    def remove_member(self, org_id: str, username: str) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                self._require_org(cur, org_id)
                user_id = self._user_id(cur, username)
                cur.execute(
                    "DELETE FROM public.memberships WHERE org_id = %s AND user_id = %s",
                    (org_id, user_id),
                )
                if cur.rowcount != 1:
                    raise AlphaNotFoundError("Membership not found")
            conn.commit()

    def list_organizations(self) -> list[Organization]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT o.id, o.name, o.base_currency, u.username, m.role
                    FROM public.organizations AS o
                    LEFT JOIN public.memberships AS m ON m.org_id = o.id
                    LEFT JOIN public.alpha_users AS u ON u.id = m.user_id
                    ORDER BY o.name, o.id, u.username
                    """
                )
                rows = cur.fetchall()
        grouped: dict[str, tuple[str, str, list[tuple[str, str]]]] = {}
        for org_id, name, base_currency, username, role in rows:
            entry = grouped.setdefault(str(org_id), (name, base_currency, []))
            if username is not None:
                entry[2].append((username, role))
        return [
            Organization(org_id, name, tuple(members), base_currency)
            for org_id, (name, base_currency, members) in grouped.items()
        ]

    def get_organization(self, org_id: str) -> Organization:
        with self._connect() as conn:
            with conn.cursor() as cur:
                self._require_org(cur, org_id)
                cur.execute("SELECT id, name, base_currency FROM public.organizations WHERE id = %s", (org_id,))
                row = cur.fetchone()
        return Organization(str(row[0]), row[1], base_currency=row[2])

    def set_base_currency(self, org_id: str, currency: str) -> str:
        code = currency.strip().upper()
        if len(code) != 3 or not code.isalpha() or not code.isascii():
            raise ValueError("Base currency must be a three-letter ISO code such as BDT or USD")
        with self._connect() as conn:
            with conn.cursor() as cur:
                self._require_org(cur, org_id)
                cur.execute("UPDATE public.organizations SET base_currency = %s WHERE id = %s", (code, org_id))
            conn.commit()
        return code

    def adopt_unowned_records(self, org_id: str) -> dict[str, int]:
        """Assign ledger records and review items that have no organization to org_id."""
        with self._connect() as conn:
            with conn.cursor() as cur:
                self._require_org(cur, org_id)
                cur.execute("UPDATE public.ledger_records SET org_id = %s WHERE org_id IS NULL", (org_id,))
                records = cur.rowcount
                cur.execute("UPDATE public.review_queue_items SET org_id = %s WHERE org_id IS NULL", (org_id,))
                review_items = cur.rowcount
            conn.commit()
        return {"ledger_records": records, "review_items": review_items}

    def authenticate(self, username: str, password: str) -> AlphaUser:
        try:
            normalized = normalize_username(username)
        except ValueError as exc:
            raise AlphaAuthenticationError("Invalid credentials") from exc
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT u.id, u.username, u.document_limit, u.documents_used, u.is_active,
                           o.id, o.name, m.role, u.password_hash
                    FROM public.alpha_users AS u
                    {_ACTIVE_MEMBERSHIP_SQL.format(session_org="NULL::uuid")}
                    WHERE u.username = %s
                    """,
                    (normalized,),
                )
                row = cur.fetchone()
        if row is None or not bool(row[4]) or not verify_password(password, row[8]):
            raise AlphaAuthenticationError("Invalid credentials")
        return _user_from_row(row)

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
                    INSERT INTO public.alpha_sessions(token_hash, user_id, org_id, expires_at_utc)
                    VALUES (%s, %s, %s, NOW() + %s)
                    """,
                    (token_hash, user.id, user.org_id, lifetime),
                )
            conn.commit()
        return token

    def authenticate_session(self, token: str) -> AlphaUser:
        token_hash = self._session_token_hash(token)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT u.id, u.username, u.document_limit, u.documents_used, u.is_active,
                           o.id, o.name, m.role
                    FROM public.alpha_sessions AS s
                    JOIN public.alpha_users AS u ON u.id = s.user_id
                    {_ACTIVE_MEMBERSHIP_SQL.format(session_org="s.org_id")}
                    WHERE s.token_hash = %s
                      AND s.expires_at_utc > NOW()
                      AND u.is_active = true
                    """,
                    (token_hash,),
                )
                row = cur.fetchone()
        if row is None:
            raise AlphaAuthenticationError("Invalid session")
        return _user_from_row(row)

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
        if not user.org_id:
            raise AlphaAuthenticationError("An organization is required to upload documents")
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
                      (id, user_id, org_id, object_key, original_name, content_type, declared_size)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (job_id, user.id, user.org_id, object_key, original_name, content_type, declared_size),
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

    def get_job(self, job_id: str, *, org_id: str) -> dict[str, Any]:
        _require_uuid(job_id, AlphaNotFoundError("Processing job not found"))
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, user_id, org_id, object_key, original_name, content_type, declared_size,
                           status, attempts, page_count, document_id, result_json,
                           error_code, error_message, authorized_at_utc, started_at_utc,
                           completed_at_utc
                    FROM public.processing_jobs WHERE id = %s AND org_id = %s
                    """,
                    (job_id, org_id),
                )
                row = cur.fetchone()
        if row is None:
            raise AlphaNotFoundError("Processing job not found")
        keys = (
            "id", "user_id", "org_id", "object_key", "original_name", "content_type", "declared_size",
            "status", "attempts", "page_count", "document_id", "result", "error_code",
            "error_message", "authorized_at_utc", "started_at_utc", "completed_at_utc",
        )
        return {key: value for key, value in zip(keys, row)}

    def claim_job(self, object_key: str, *, max_attempts: int = 3) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH claimed AS (
                        UPDATE public.processing_jobs
                        SET status = 'PROCESSING', attempts = attempts + 1,
                            started_at_utc = NOW(), updated_at_utc = NOW(),
                            error_code = NULL, error_message = NULL
                        WHERE object_key = %s
                          AND status IN ('AUTHORIZED', 'FAILED')
                          AND attempts < %s
                        RETURNING id, user_id, org_id, original_name, content_type, declared_size, attempts
                    )
                    SELECT c.id, c.user_id, c.org_id, c.original_name, c.content_type,
                           c.declared_size, c.attempts, o.base_currency
                    FROM claimed AS c
                    JOIN public.organizations AS o ON o.id = c.org_id
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
            "org_id": str(row[2]),
            "original_name": row[3],
            "content_type": row[4],
            "declared_size": int(row[5]),
            "attempts": int(row[6]),
            "base_currency": row[7],
        }

    def retry_job(self, job_id: str, *, org_id: str, max_attempts: int = 3) -> str:
        _require_uuid(job_id, AlphaQuotaError("This job cannot be retried"))
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE public.processing_jobs
                    SET status = 'AUTHORIZED', updated_at_utc = NOW(),
                        error_code = NULL, error_message = NULL
                    WHERE id = %s AND org_id = %s AND status = 'FAILED' AND attempts < %s
                    RETURNING object_key
                    """,
                    (job_id, org_id, max_attempts),
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
        """Claim a file hash within the organization that owns the source job."""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT org_id FROM public.processing_jobs WHERE object_key = %s",
                    (source_id,),
                )
                job = cur.fetchone()
                if job is None:
                    raise AlphaNotFoundError("Processing job not found for document claim")
                org_id = job[0]
                cur.execute(
                    """
                    INSERT INTO public.document_claims(org_id, file_hash, source_id, status, owner_id)
                    VALUES (%s, %s, %s, 'CLAIMED', %s)
                    ON CONFLICT (org_id, file_hash) DO NOTHING
                    RETURNING status
                    """,
                    (org_id, file_hash, source_id, owner_id),
                )
                inserted = cur.fetchone()
                if inserted is None:
                    cur.execute(
                        """
                        UPDATE public.document_claims
                        SET source_id = %s, status = 'CLAIMED', owner_id = %s, updated_at_utc = NOW()
                        WHERE org_id = %s AND file_hash = %s AND status IN ('FAILED', 'REJECTED')
                        RETURNING source_id, status, owner_id
                        """,
                        (source_id, owner_id, org_id, file_hash),
                    )
                    reclaimed = cur.fetchone()
                else:
                    reclaimed = None
                if inserted is None and reclaimed is None:
                    cur.execute(
                        """
                        SELECT source_id, status, owner_id FROM public.document_claims
                        WHERE org_id = %s AND file_hash = %s
                        """,
                        (org_id, file_hash),
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
                    WHERE file_hash = %s AND source_id = %s
                    """,
                    (status, file_hash, source_id),
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
