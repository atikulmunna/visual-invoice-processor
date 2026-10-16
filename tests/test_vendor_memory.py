from __future__ import annotations

from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from app.alpha_store import AlphaStore
from app.config import Settings
from app.vendor_memory import (
    corrections_between,
    extraction_hints,
    hint_text,
    hints_for_worker,
    is_trusted,
    trusted_for_worker,
    vendor_in_text,
    vendor_is_trusted,
    vendor_memory,
)

PASSWORD = "A-strong-alpha-password"
EXTRACTED = {
    "document_type": "invoice", "vendor_name": "Shwapno", "vendor_tax_id": None, "invoice_number": "SHW-2405",
    "invoice_date": "2026-09-15", "due_date": None, "currency": "BDT", "payment_method": None, "subtotal": 4208.46,
    "tax_amount": 0.0, "shipping_amount": None, "discount_amount": None, "total_amount": 4208.46,
    "model_confidence": 0.4, "line_items": [],
}
# What the review form fills in for values the model left empty.
FORM_FILLED = {"payment_method": "unknown", "shipping_amount": 0.0, "discount_amount": 0.0, "validation_score": 0.5}
CLEAN = {"corrections": []}
FIXED = {"corrections": [{"field": "currency", "before": "BDT", "after": "USD"}]}


def test_changes_of_spelling_format_or_form_default_are_not_corrections() -> None:
    approved = {
        **EXTRACTED, "vendor_name": "SHWAPNO LTD.", "invoice_number": "shw 2405", "currency": "bdt",
        "payment_method": "unknown", "due_date": "", "shipping_amount": 0, "discount_amount": 0.0,
        "subtotal": 4208.460, "line_items": [{"description": "Rice", "quantity": 1, "unit_price": 1, "line_total": 1}],
    }

    assert corrections_between(EXTRACTED, approved) == []


def test_real_changes_are_corrections_with_both_values() -> None:
    approved = {**EXTRACTED, "invoice_number": "SHW-2406", "currency": "USD", "total_amount": 4208.0,
                "vendor_tax_id": "001234567-0101", "payment_method": "card"}

    assert corrections_between(EXTRACTED, approved) == [
        {"field": "vendor_tax_id", "before": None, "after": "001234567-0101"},
        {"field": "invoice_number", "before": "SHW-2405", "after": "SHW-2406"},
        {"field": "currency", "before": "BDT", "after": "USD"},
        {"field": "payment_method", "before": None, "after": "card"},
        {"field": "total_amount", "before": 4208.46, "after": 4208.0},
    ]


@pytest.mark.parametrize(
    ("reviews", "trusted"),
    [
        ([CLEAN] * 4, False),
        ([CLEAN] * 5, True),
        ([CLEAN] * 4 + [FIXED], False),
        ([CLEAN] * 5 + [FIXED], True),  # only the latest five count, newest first
        ([FIXED] + [CLEAN] * 9, False),
    ],
)
def test_trust_needs_the_latest_five_reviews_unchanged(reviews: list[dict[str, Any]], trusted: bool) -> None:
    assert is_trusted(reviews) is trusted


def test_the_vendor_is_found_by_its_whole_name_in_the_text() -> None:
    names = [(1, "tech land"), (2, "tech land bd"), (3, "ab"), (4, "acme")]
    text = "TAX INVOICE\nTech Land BD Ltd.\nUttara, Dhaka\nAB 12"

    assert vendor_in_text(names, text) == 2  # the longest name wins
    assert vendor_in_text(names, "Acmeland Traders") is None  # whole words only
    assert vendor_in_text(names, "Paid at AB") is None  # names this short are too common
    assert vendor_in_text([], text) is None


def test_hints_quote_the_newest_correction_of_each_field() -> None:
    reviews = [
        {"corrections": [{"field": "currency", "before": "BDT", "after": "USD"}]},
        {"corrections": [{"field": "currency", "before": "BDT", "after": "EUR"},
                         {"field": "invoice_number", "before": "1001872388", "after": "1001866868"},
                         {"field": "due_date", "before": None, "after": "2026-10-01"},
                         {"field": "vendor_name", "before": "x" * 200, "after": "Daraz"}]},
    ]

    hint = hint_text(reviews)

    assert hint is not None
    assert 'currency was read as "BDT" but the reviewer corrected it to "USD"' in hint
    assert "EUR" not in hint
    assert 'invoice_number was read as "1001872388" but the reviewer corrected it to "1001866868"' in hint
    assert 'due_date was read as empty but the reviewer corrected it to "2026-10-01"' in hint
    assert "x" * 81 not in hint and "x" * 80 in hint
    assert "never copy them" in hint
    assert hint_text([CLEAN, CLEAN]) is None


def test_the_worker_lookups_only_run_on_postgres_and_never_block(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def broken(*_: Any, **__: Any) -> None:
        raise RuntimeError("database unreachable")

    monkeypatch.setattr("app.vendor_memory.extraction_hints", broken)
    monkeypatch.setattr("app.vendor_memory.vendor_is_trusted", broken)
    sheets = Settings(ledger_backend="sheets")
    postgres = Settings(ledger_backend="postgres", postgres_dsn="postgresql://example")
    pdf = tmp_path / "invoice.pdf"

    assert hints_for_worker(sheets, pdf, "org") is None
    assert hints_for_worker(postgres, pdf, None) is None
    assert hints_for_worker(postgres, pdf, "org") is None
    assert trusted_for_worker(sheets, EXTRACTED, "org") is False
    assert trusted_for_worker(postgres, EXTRACTED, None) is False
    assert trusted_for_worker(postgres, EXTRACTED, "org") is False


@pytest.fixture
def reviews_env(dsn: str, monkeypatch: pytest.MonkeyPatch) -> str:
    monkeypatch.setenv("INGESTION_BACKEND", "s3")
    monkeypatch.setenv("S3_BUCKET_NAME", "alpha-invoices")
    monkeypatch.setenv("LEDGER_BACKEND", "postgres")
    monkeypatch.setenv("POSTGRES_DSN", dsn)
    monkeypatch.setenv("POSTGRES_TABLE", "ledger_records")
    monkeypatch.setenv("REVIEW_QUEUE_BACKEND", "postgres")
    monkeypatch.setenv("REVIEW_QUEUE_TABLE", "review_queue_items")
    return dsn


def _org(dsn: str) -> str:
    return AlphaStore(dsn).create_user(f"memory-{uuid4().hex[:8]}", PASSWORD, max_users=1000).org_id


def _review(org_id: str, **changes: Any) -> None:
    """One document sent to review and approved, with the reviewer's changes."""
    from app.review_queue import resolve_review_item, route_to_review_queue

    document_id = uuid4().hex
    route_to_review_queue(
        document_id=document_id,
        reason_codes=["low_confidence"],
        metadata={"source_file_id": f"inbox/{org_id}/{document_id}.pdf", "file_hash": document_id,
                  "normalized_record": EXTRACTED},
        org_id=org_id,
    )
    resolve_review_item(document_id, record_override={**EXTRACTED, **FORM_FILLED, **changes}, org_id=org_id)


def _vendor_id(dsn: str, org_id: str) -> int:
    import psycopg

    with psycopg.connect(dsn) as conn:
        return conn.execute("select id from vendors where org_id = %s", (org_id,)).fetchone()[0]


def test_reviews_build_a_vendor_track_record(reviews_env: str) -> None:
    org, other = _org(reviews_env), _org(reviews_env)
    _review(org, currency="USD", invoice_number="SHW-2406")
    for _ in range(4):
        _review(org, vendor_name="SHWAPNO LTD")  # a spelling, not a correction
    vendor_id = _vendor_id(reviews_env, org)

    memory = vendor_memory(reviews_env, org_id=org, vendor_id=vendor_id)

    assert (memory["reviews"], memory["unchanged"], memory["trusted"], memory["trusted_after"]) == (5, 4, False, 5)
    assert [(c["field"], c["before"], c["after"]) for c in memory["corrections"]] == [
        ("invoice_number", "SHW-2405", "SHW-2406"),
        ("currency", "BDT", "USD"),
    ]
    assert not vendor_is_trusted(reviews_env, org_id=org, record={"vendor_name": "Shwapno"})

    _review(org)

    assert vendor_is_trusted(reviews_env, org_id=org, record={"vendor_name": "Shwapno Ltd"})
    assert not vendor_is_trusted(reviews_env, org_id=org, record={"vendor_name": "Agora"})
    assert not vendor_is_trusted(reviews_env, org_id=other, record={"vendor_name": "Shwapno"})
    assert vendor_memory(reviews_env, org_id=other, vendor_id=vendor_id)["reviews"] == 0


def test_hints_come_from_the_vendor_the_pdf_text_names(reviews_env: str, monkeypatch: pytest.MonkeyPatch) -> None:
    org, other = _org(reviews_env), _org(reviews_env)
    _review(org, currency="USD")
    texts = {"shwapno.pdf": "SHWAPNO\nCash memo\nTotal 4,208.46", "agora.pdf": "Agora Superstore\nTotal 99"}
    monkeypatch.setattr("app.vendor_memory.native_pdf_text", lambda path: texts.get(Path(path).name))

    hint = extraction_hints(reviews_env, org_id=org, file_path=Path("shwapno.pdf"))

    assert hint is not None and 'currency was read as "BDT" but the reviewer corrected it to "USD"' in hint
    assert extraction_hints(reviews_env, org_id=org, file_path=Path("agora.pdf")) is None
    assert extraction_hints(reviews_env, org_id=org, file_path=Path("photo.jpg")) is None
    assert extraction_hints(reviews_env, org_id=other, file_path=Path("shwapno.pdf")) is None
