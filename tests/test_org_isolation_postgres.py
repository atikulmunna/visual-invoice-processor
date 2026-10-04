"""Tenant isolation against a real PostgreSQL server.

Set TEST_POSTGRES_DSN to a server where the connecting role may create databases.
Each run creates and drops its own database.
"""

from __future__ import annotations

import json
import os
import shutil
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from app.alpha_store import AlphaAuthenticationError, AlphaNotFoundError, AlphaQuotaError, AlphaStore, AlphaUser
from app.db_migrate import DEFAULT_MIGRATIONS_DIR, apply_migrations
from app.object_storage_service import ObjectStorageService

psycopg = pytest.importorskip("psycopg")

BASE_DSN = os.getenv("TEST_POSTGRES_DSN", "").strip()
pytestmark = pytest.mark.skipif(not BASE_DSN, reason="TEST_POSTGRES_DSN is not set")

PASSWORD = "A-strong-alpha-password"


def _create_database() -> str:
    name = f"ledgerly_test_{uuid4().hex[:12]}"
    with psycopg.connect(BASE_DSN, autocommit=True) as conn:
        conn.execute(f'CREATE DATABASE "{name}"')
        # Supabase roles referenced by the lockdown migrations.
        for role in ("anon", "authenticated"):
            conn.execute(
                f"DO $$ BEGIN CREATE ROLE {role} NOLOGIN; "
                "EXCEPTION WHEN duplicate_object THEN NULL; END $$"
            )
    return psycopg.conninfo.make_conninfo(BASE_DSN, dbname=name)


def _drop_database(dsn: str) -> None:
    name = psycopg.conninfo.conninfo_to_dict(dsn)["dbname"]
    with psycopg.connect(BASE_DSN, autocommit=True) as conn:
        conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


@pytest.fixture(scope="module")
def dsn() -> Iterator[str]:
    database = _create_database()
    try:
        apply_migrations(database)
        yield database
    finally:
        _drop_database(database)


@pytest.fixture
def pg_env(dsn: str, monkeypatch: pytest.MonkeyPatch) -> str:
    monkeypatch.setenv("INGESTION_BACKEND", "s3")
    monkeypatch.setenv("S3_BUCKET_NAME", "alpha-invoices")
    monkeypatch.setenv("LEDGER_BACKEND", "postgres")
    monkeypatch.setenv("POSTGRES_DSN", dsn)
    monkeypatch.setenv("POSTGRES_TABLE", "ledger_records")
    monkeypatch.setenv("REVIEW_QUEUE_BACKEND", "postgres")
    monkeypatch.setenv("REVIEW_QUEUE_TABLE", "review_queue_items")
    return dsn


def _user(store: AlphaStore, prefix: str) -> AlphaUser:
    return store.create_user(f"{prefix}-{uuid4().hex[:8]}", PASSWORD, max_users=1000)


def _upload(store: AlphaStore, user: AlphaUser, name: str = "invoice.png", size: int = 64) -> tuple[str, str]:
    job_id = str(uuid4())
    object_key = f"inbox/{user.id}/{job_id}/{name}"
    store.authorize_upload(
        user,
        object_key=object_key,
        original_name=name,
        content_type="image/png",
        declared_size=size,
    )
    return job_id, object_key


def test_migration_backfills_existing_rows_into_personal_organizations(tmp_path: Path) -> None:
    database = _create_database()
    try:
        legacy_dir = tmp_path / "legacy"
        legacy_dir.mkdir()
        for path in DEFAULT_MIGRATIONS_DIR.glob("00[0-4]_*.sql"):
            shutil.copy(path, legacy_dir / path.name)
        apply_migrations(database, legacy_dir)

        user_a, user_b = uuid4(), uuid4()
        key_a = f"inbox/{user_a}/{uuid4()}/a.pdf"
        key_b = f"inbox/{user_b}/{uuid4()}/b.pdf"
        with psycopg.connect(database) as conn:
            for user_id, name in ((user_a, "tester.a"), (user_b, "tester.b")):
                conn.execute(
                    "INSERT INTO alpha_users(id, username, password_hash) VALUES (%s, %s, 'x')",
                    (user_id, name),
                )
            for user_id, key in ((user_a, key_a), (user_b, key_b)):
                conn.execute(
                    """
                    INSERT INTO processing_jobs(id, user_id, object_key, original_name, content_type, declared_size)
                    VALUES (%s, %s, %s, 'f.pdf', 'application/pdf', 10)
                    """,
                    (uuid4(), user_id, key),
                )
            for key in (key_a, key_b, "drive-legacy"):
                conn.execute(
                    """
                    INSERT INTO ledger_records(drive_file_id, file_hash, status, record_json, metadata_json)
                    VALUES (%s, 'same-hash', 'STORED', '{}', '{}')
                    """,
                    (key,),
                )
            conn.execute(
                "INSERT INTO review_queue_items(document_id, status, metadata_json) VALUES ('rv-a', 'REVIEW_REQUIRED', %s)",
                (json.dumps({"source_file_id": key_a}),),
            )
            conn.execute(
                "INSERT INTO document_claims(file_hash, source_id, status) VALUES ('same-hash', %s, 'STORED')",
                (key_a,),
            )
            conn.execute(
                "INSERT INTO document_claims(file_hash, source_id, status) VALUES ('legacy-hash', 'drive-legacy', 'STORED')"
            )

        assert apply_migrations(database) == ["005_organizations.sql", "006_org_base_currency.sql"]

        with psycopg.connect(database) as conn:
            orgs = dict(conn.execute("SELECT id, name FROM organizations").fetchall())
            assert orgs == {user_a: "tester.a", user_b: "tester.b"}
            currencies = {row[0] for row in conn.execute("SELECT base_currency FROM organizations").fetchall()}
            assert currencies == {"BDT"}
            owners = conn.execute("SELECT org_id, user_id, role FROM memberships ORDER BY role").fetchall()
            assert sorted(owners) == sorted([(user_a, user_a, "owner"), (user_b, user_b, "owner")])
            records = dict(conn.execute("SELECT drive_file_id, org_id FROM ledger_records").fetchall())
            assert records == {key_a: user_a, key_b: user_b, "drive-legacy": None}
            assert conn.execute("SELECT org_id FROM review_queue_items").fetchone()[0] == user_a
            claims = conn.execute("SELECT org_id, file_hash FROM document_claims").fetchall()
            assert claims == [(user_a, "same-hash")]
            assert conn.execute("SELECT count(*) FROM processing_jobs WHERE org_id IS NULL").fetchone()[0] == 0
    finally:
        _drop_database(database)


def test_users_sessions_and_memberships_carry_the_active_organization(dsn: str) -> None:
    store = AlphaStore(dsn)
    user = _user(store, "owner")
    assert user.org_id and user.role == "owner"

    authenticated = store.authenticate(user.username, PASSWORD)
    assert authenticated.org_id == user.org_id

    token = store.create_session(authenticated)
    assert store.authenticate_session(token).org_id == user.org_id

    shared = store.create_organization("Shared books", owner_username=user.username)
    member = _user(store, "member")
    store.add_member(shared.id, member.username)
    listed = {org.id: org for org in store.list_organizations()}
    assert set(listed[shared.id].members) == {(user.username, "owner"), (member.username, "member")}

    # A session pinned to an organization dies when the membership is removed.
    with psycopg.connect(dsn) as conn:
        conn.execute("UPDATE alpha_sessions SET org_id = %s WHERE user_id = %s", (shared.id, user.id))
    assert store.authenticate_session(token).org_id == shared.id
    store.remove_member(shared.id, user.username)
    with pytest.raises(AlphaAuthenticationError):
        store.authenticate_session(token)

    with pytest.raises(AlphaNotFoundError):
        store.add_member(str(uuid4()), member.username)


def test_jobs_are_visible_and_retryable_only_inside_their_organization(dsn: str) -> None:
    store = AlphaStore(dsn)
    one, two = _user(store, "one"), _user(store, "two")
    job_id, object_key = _upload(store, one)

    assert str(store.get_job(job_id, org_id=one.org_id)["org_id"]) == one.org_id
    with pytest.raises(AlphaNotFoundError):
        store.get_job(job_id, org_id=two.org_id)
    with pytest.raises(AlphaNotFoundError):
        store.get_job("not-a-uuid", org_id=one.org_id)

    store.complete_job(job_id, status="FAILED", error_code="x")
    with pytest.raises(AlphaQuotaError):
        store.retry_job(job_id, org_id=two.org_id)
    assert store.retry_job(job_id, org_id=one.org_id) == object_key

    claimed = store.claim_job(object_key)
    assert claimed is not None and claimed["org_id"] == one.org_id


def test_deduplication_is_scoped_per_organization(dsn: str) -> None:
    store = AlphaStore(dsn)
    one, two = _user(store, "one"), _user(store, "two")
    _, key_one = _upload(store, one)
    _, key_two = _upload(store, two)
    _, key_one_again = _upload(store, one)
    file_hash = uuid4().hex

    assert store.claim_document(key_one, file_hash, owner_id="w1").status == "claimed"
    store.mark_status(key_one, file_hash, "STORED")
    # The same bytes uploaded by another organization are processed normally.
    assert store.claim_document(key_two, file_hash, owner_id="w2").status == "claimed"
    # A second upload inside the same organization is a duplicate.
    duplicate = store.claim_document(key_one_again, file_hash, owner_id="w3")
    assert duplicate.status == "already_processed"
    assert duplicate.drive_file_id == key_one

    # A non-owner cannot change another job's claim.
    store.mark_status(key_one_again, file_hash, "FAILED")
    with psycopg.connect(dsn) as conn:
        statuses = dict(
            conn.execute(
                "SELECT source_id, status FROM document_claims WHERE file_hash = %s",
                (file_hash,),
            ).fetchall()
        )
    assert statuses == {key_one: "STORED", key_two: "CLAIMED"}


def test_review_queue_and_dashboard_queries_are_scoped(pg_env: str) -> None:
    from app.monitoring_api import _query_dashboard_data, _resolved_file_hashes
    from app.review_queue import (
        dismiss_review_item,
        list_review_items,
        load_review_item,
        resolve_review_item,
        route_to_review_queue,
    )
    from app.storage_service import append_record

    store = AlphaStore(pg_env)
    one, two = _user(store, "one"), _user(store, "two")
    record = {
        "document_type": "invoice",
        "vendor_name": "Acme",
        "invoice_number": "INV-1",
        "invoice_date": "2026-01-02",
        "currency": "USD",
        "subtotal": 100.0,
        "tax_amount": 0.0,
        "total_amount": 100.0,
        "model_confidence": 0.9,
        "validation_score": 0.9,
        "line_items": [],
    }
    for user in (one, two):
        _, key = _upload(store, user)
        append_record(
            record={**record, "vendor_name": f"Vendor {user.username}"},
            metadata={"drive_file_id": key, "file_hash": f"stored-{user.id}", "org_id": user.org_id},
        )
        route_to_review_queue(
            document_id=f"review-{user.id}",
            reason_codes=["low_confidence"],
            metadata={"source_file_id": key, "file_hash": f"review-{user.id}", "normalized_record": record},
            org_id=user.org_id,
        )

    def _ids(org_id: str | None) -> set[str]:
        return {item["document_id"] for item in list_review_items(org_id=org_id)}

    assert _ids(one.org_id) == {f"review-{one.id}"}
    assert {f"review-{one.id}", f"review-{two.id}"} <= _ids(None)

    with pytest.raises(FileNotFoundError):
        load_review_item(f"review-{two.id}", org_id=one.org_id)
    with pytest.raises(FileNotFoundError):
        resolve_review_item(f"review-{two.id}", org_id=one.org_id)
    with pytest.raises(FileNotFoundError):
        dismiss_review_item(f"review-{two.id}", resolution_status="REJECTED", org_id=one.org_id)
    assert load_review_item(f"review-{two.id}", org_id=two.org_id)["status"] == "REVIEW_REQUIRED"

    resolved = resolve_review_item(f"review-{one.id}", org_id=one.org_id)
    assert resolved["storage_result"]["status"] == "appended"

    data = _query_dashboard_data(pg_env, limit=20, org_id=one.org_id)
    assert data["error"] is None
    assert data["kpis"]["records_total"] == 2
    assert {row["vendor_name"] for row in data["vendor_spend"]} == {f"Vendor {one.username}", "Acme"}
    assert sum(day["records_total"] for day in data["daily_summary"]) == 2
    assert _resolved_file_hashes(pg_env, org_id=one.org_id) == {f"stored-{one.id}", f"review-{one.id}"}
    assert _query_dashboard_data(pg_env, limit=20, org_id=two.org_id)["kpis"]["records_total"] == 1


class _FakeObjectStorage(ObjectStorageService):
    """Stands in for S3; every upload contains the same bytes."""

    content = b"\x89PNG\r\n\x1a\n" + b"\x00" * 56

    def __init__(self) -> None:
        self.archived: list[str] = []

    def download_file(self, object_key: str, out_path: str | Path) -> Path:
        Path(out_path).write_bytes(self.content)
        return Path(out_path)

    def move_to_archive(self, object_key: str, archive_prefix: str | None = None) -> str:
        self.archived.append(object_key)
        return object_key

    def delete_object(self, object_key: str) -> None:
        pass


def test_worker_processes_identical_files_independently_per_organization(
    pg_env: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.config import Settings
    from app.serverless_worker import process_s3_object

    extracted: dict[str, Any] = {
        "document_type": "invoice",
        "vendor_name": "Acme Supplies",
        "invoice_number": "INV-77",
        "invoice_date": "2026-03-04",
        "currency": "USD",
        "subtotal": 100,
        "tax_amount": 0,
        "total_amount": 100,
        "model_confidence": 0.95,
        "line_items": [],
        "_provider": "fake",
    }
    monkeypatch.setattr("app.main.extract_document", lambda **_: dict(extracted))
    storage = _FakeObjectStorage()

    store = AlphaStore(pg_env)
    one, two = _user(store, "one"), _user(store, "two")
    settings = Settings.from_env()
    _, key_one = _upload(store, one)
    _, key_two = _upload(store, two)
    _, key_one_again = _upload(store, one)

    results = [
        process_s3_object(key, store=store, storage=storage, settings=settings)
        for key in (key_one, key_two, key_one_again)
    ]

    assert [result["status"] for result in results] == ["STORED", "STORED", "SKIPPED_DUPLICATE"]
    with psycopg.connect(pg_env) as conn:
        owners = dict(
            conn.execute(
                "SELECT drive_file_id, org_id::text FROM ledger_records WHERE drive_file_id = ANY(%s)",
                ([key_one, key_two],),
            ).fetchall()
        )
    assert owners == {key_one: one.org_id, key_two: two.org_id}


def test_base_currency_is_validated_and_listed(dsn: str) -> None:
    store = AlphaStore(dsn)
    owner = _user(store, "currency")

    assert store.set_base_currency(owner.org_id, "usd") == "USD"
    assert {org.id: org.base_currency for org in store.list_organizations()}[owner.org_id] == "USD"
    assert store.get_organization(owner.org_id).base_currency == "USD"
    for bad in ("US", "US1", "dollar"):
        with pytest.raises(ValueError):
            store.set_base_currency(owner.org_id, bad)
    with pytest.raises(AlphaNotFoundError):
        store.set_base_currency(str(uuid4()), "EUR")


def test_worker_uses_each_organization_base_currency_and_never_invents_fields(
    pg_env: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.config import Settings
    from app.serverless_worker import process_s3_object

    no_currency: dict[str, Any] = {
        "document_type": "invoice",
        "vendor_name": "Acme Supplies",
        "invoice_number": "INV-90",
        "invoice_date": "2026-03-04",
        "subtotal": 100,
        "tax_amount": 0,
        "total_amount": 100,
        "model_confidence": 0.95,
        "line_items": [],
        "_provider": "fake",
    }
    outputs = [
        dict(no_currency),
        dict(no_currency),
        {**no_currency, "vendor_name": None, "invoice_date": None},
    ]
    monkeypatch.setattr("app.main.extract_document", lambda **_: outputs.pop(0))

    store = AlphaStore(pg_env)
    bdt_org, usd_org = _user(store, "bdt"), _user(store, "usd")
    store.set_base_currency(usd_org.org_id, "USD")
    settings = Settings.from_env()
    _, key_bdt = _upload(store, bdt_org)
    _, key_usd = _upload(store, usd_org)
    _, key_blank = _upload(store, usd_org, name="blank.png")

    # Distinct bytes per upload so in-org deduplication does not interfere.
    results = []
    for index, key in enumerate((key_bdt, key_usd, key_blank)):
        storage = _FakeObjectStorage()
        storage.content = _FakeObjectStorage.content + bytes([index])
        results.append(process_s3_object(key, store=store, storage=storage, settings=settings))

    assert [result["status"] for result in results] == ["STORED", "STORED", "REVIEW_REQUIRED"]
    assert results[2]["reason_codes"] == ["missing_vendor", "missing_invoice_date"]
    with psycopg.connect(pg_env) as conn:
        stored = dict(
            conn.execute(
                """
                SELECT drive_file_id, record_json ->> 'currency' || ' assumed=' || (record_json ->> 'currency_assumed')
                FROM ledger_records WHERE drive_file_id = ANY(%s)
                """,
                ([key_bdt, key_usd],),
            ).fetchall()
        )
        reviewed = conn.execute(
            "SELECT reason_codes, metadata_json -> 'normalized_record' ->> 'vendor_name' FROM review_queue_items "
            "WHERE metadata_json ->> 'source_file_id' = %s",
            (key_blank,),
        ).fetchone()
    assert stored == {key_bdt: "BDT assumed=true", key_usd: "USD assumed=true"}
    assert reviewed == (["missing_vendor", "missing_invoice_date"], None)


def test_set_password_replaces_the_password_and_ends_sessions(dsn: str) -> None:
    store = AlphaStore(dsn)
    user = _user(store, "reset")
    token = store.create_session(store.authenticate(user.username, PASSWORD))

    store.set_password(user.username, "A-brand-new-password")

    assert store.authenticate(user.username, "A-brand-new-password").id == user.id
    with pytest.raises(AlphaAuthenticationError):
        store.authenticate(user.username, PASSWORD)
    with pytest.raises(AlphaAuthenticationError):
        store.authenticate_session(token)
    with pytest.raises(AlphaNotFoundError):
        store.set_password("no-such-tester", "A-brand-new-password")
    with pytest.raises(ValueError, match="12"):
        store.set_password(user.username, "short")
