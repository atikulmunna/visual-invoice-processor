from __future__ import annotations

from pathlib import Path
from typing import Any

from app.config import Settings
from app.drive_service import DriveService, is_supported_mime_type


class _FakeFilesAPI:
    def __init__(self, response: dict[str, Any]) -> None:
        self._response = response

    def list(self, **_: Any) -> "_FakeFilesAPI":
        return self

    def execute(self) -> dict[str, Any]:
        return self._response


class _FakeDriveClient:
    def __init__(self, response: dict[str, Any]) -> None:
        self._files = _FakeFilesAPI(response)

    def files(self) -> _FakeFilesAPI:
        return self._files


def _settings(tmp_path: Path) -> Settings:
    sa = tmp_path / "sa.json"
    sa.write_text("{}", encoding="utf-8")
    return Settings(
        drive_inbox_folder_id="folder-123",
        google_auth_mode="service_account",
        google_service_account_file=str(sa),
        google_oauth_client_secret_file=None,
        google_oauth_token_file=".tokens/token.json",
        allowed_mime_types=("image/jpeg", "image/png", "application/pdf"),
        log_level="INFO",
    )


def test_is_supported_mime_type() -> None:
    allowed = ("image/jpeg", "image/png", "application/pdf")
    assert is_supported_mime_type("image/jpeg", allowed)
    assert not is_supported_mime_type("text/plain", allowed)


def test_list_inbox_files_filters_unsupported(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    response = {
        "files": [
            {"id": "1", "name": "a.jpg", "mimeType": "image/jpeg"},
            {"id": "2", "name": "b.txt", "mimeType": "text/plain"},
        ]
    }
    drive = DriveService(_FakeDriveClient(response), settings=settings)
    files = drive.list_inbox_files()
    assert len(files) == 1
    assert files[0]["id"] == "1"

