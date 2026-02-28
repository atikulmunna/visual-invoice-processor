from __future__ import annotations

from pathlib import Path

import pytest

from app.config import Settings


@pytest.fixture
def service_account_file(tmp_path: Path) -> Path:
    p = tmp_path / "svc.json"
    p.write_text("{}", encoding="utf-8")
    return p


def test_settings_load_service_account(monkeypatch: pytest.MonkeyPatch, service_account_file: Path) -> None:
    monkeypatch.setenv("DRIVE_INBOX_FOLDER_ID", "folder-123")
    monkeypatch.setenv("GOOGLE_AUTH_MODE", "service_account")
    monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_FILE", str(service_account_file))

    settings = Settings.from_env()
    assert settings.drive_inbox_folder_id == "folder-123"
    assert settings.google_auth_mode == "service_account"
    assert "application/pdf" in settings.allowed_mime_types


def test_settings_missing_required_env(
    monkeypatch: pytest.MonkeyPatch, service_account_file: Path
) -> None:
    monkeypatch.delenv("DRIVE_INBOX_FOLDER_ID", raising=False)
    monkeypatch.setenv("GOOGLE_AUTH_MODE", "service_account")
    monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_FILE", str(service_account_file))
    with pytest.raises(ValueError, match="DRIVE_INBOX_FOLDER_ID"):
        Settings.from_env()


def test_settings_requires_existing_service_account_file(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DRIVE_INBOX_FOLDER_ID", "folder-123")
    monkeypatch.setenv("GOOGLE_AUTH_MODE", "service_account")
    monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_FILE", "missing-file.json")
    with pytest.raises(ValueError, match="not found"):
        Settings.from_env()


def test_settings_rejects_invalid_ledger_backend(
    monkeypatch: pytest.MonkeyPatch, service_account_file: Path
) -> None:
    monkeypatch.setenv("DRIVE_INBOX_FOLDER_ID", "folder-123")
    monkeypatch.setenv("GOOGLE_AUTH_MODE", "service_account")
    monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_FILE", str(service_account_file))
    monkeypatch.setenv("LEDGER_BACKEND", "mongo")
    with pytest.raises(ValueError, match="LEDGER_BACKEND"):
        Settings.from_env()


def test_settings_r2_postgres_does_not_require_google(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("INGESTION_BACKEND", "r2")
    monkeypatch.setenv("LEDGER_BACKEND", "postgres")
    monkeypatch.setenv("R2_ENDPOINT_URL", "https://example.r2.cloudflarestorage.com")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "abc")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "xyz")
    monkeypatch.setenv("R2_BUCKET_NAME", "invoices")
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost:5432/db")
    monkeypatch.delenv("GOOGLE_SERVICE_ACCOUNT_FILE", raising=False)
    monkeypatch.delenv("GOOGLE_OAUTH_CLIENT_SECRET_FILE", raising=False)
    monkeypatch.delenv("DRIVE_INBOX_FOLDER_ID", raising=False)

    settings = Settings.from_env()
    assert settings.ingestion_backend == "r2"
    assert settings.ledger_backend == "postgres"
    assert settings.r2_bucket_name == "invoices"


def test_settings_r2_requires_core_r2_envs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("INGESTION_BACKEND", "r2")
    monkeypatch.setenv("LEDGER_BACKEND", "postgres")
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost:5432/db")
    monkeypatch.delenv("R2_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("R2_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("R2_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.delenv("R2_BUCKET_NAME", raising=False)

    with pytest.raises(ValueError, match="R2_ENDPOINT_URL"):
        Settings.from_env()
