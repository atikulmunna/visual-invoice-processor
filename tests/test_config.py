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
