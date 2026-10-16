from __future__ import annotations

import pytest

from app.config import Settings


@pytest.fixture
def production_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("S3_BUCKET_NAME", "alpha-invoices")
    monkeypatch.setenv("S3_REGION", "ap-southeast-1")
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost:5432/db")
    monkeypatch.delenv("ALLOWED_MIME_TYPES", raising=False)


def test_settings_come_from_the_environment(production_env: None, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MAX_PDF_PAGES", "3")

    settings = Settings.from_env()

    assert settings.s3_bucket_name == "alpha-invoices"
    assert settings.s3_region == "ap-southeast-1"
    assert settings.postgres_dsn == "postgresql://user:pass@localhost:5432/db"
    assert settings.allowed_mime_types == ("image/jpeg", "image/png", "application/pdf")
    assert settings.max_pdf_pages == 3


@pytest.mark.parametrize("missing", ["S3_BUCKET_NAME", "POSTGRES_DSN"])
def test_the_bucket_and_the_database_are_required(
    production_env: None, monkeypatch: pytest.MonkeyPatch, missing: str
) -> None:
    monkeypatch.setenv(missing, " ")

    with pytest.raises(ValueError, match=missing):
        Settings.from_env()


def test_at_least_one_file_type_must_be_allowed(production_env: None, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALLOWED_MIME_TYPES", " , ")

    with pytest.raises(ValueError, match="ALLOWED_MIME_TYPES"):
        Settings.from_env()
