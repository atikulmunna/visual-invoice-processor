from __future__ import annotations

from pathlib import Path
from typing import Any

from app.config import Settings
from app.r2_service import R2Service


class _FakeR2Client:
    def __init__(self, list_pages: list[dict[str, Any]]) -> None:
        self._pages = list_pages
        self._list_calls = 0
        self.download_calls: list[tuple[str, str, str]] = []
        self.copy_calls: list[dict[str, Any]] = []
        self.delete_calls: list[dict[str, Any]] = []

    def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]:
        idx = self._list_calls
        self._list_calls += 1
        if idx < len(self._pages):
            return self._pages[idx]
        return {"Contents": [], "IsTruncated": False}

    def download_file(self, bucket: str, key: str, output_path: str) -> None:
        self.download_calls.append((bucket, key, output_path))
        Path(output_path).write_bytes(b"data")

    def copy_object(self, **kwargs: Any) -> None:
        self.copy_calls.append(kwargs)

    def delete_object(self, **kwargs: Any) -> None:
        self.delete_calls.append(kwargs)


def _settings() -> Settings:
    return Settings(
        ingestion_backend="r2",
        drive_inbox_folder_id=None,
        google_auth_mode="service_account",
        google_service_account_file=None,
        google_oauth_client_secret_file=None,
        google_oauth_token_file=".tokens/google_token.json",
        r2_endpoint_url="https://example.r2.cloudflarestorage.com",
        r2_access_key_id="abc",
        r2_secret_access_key="xyz",
        r2_bucket_name="invoices",
        r2_inbox_prefix="inbox/",
        r2_archive_prefix="archive/",
        allowed_mime_types=("image/jpeg", "image/png", "application/pdf"),
        log_level="INFO",
        ledger_backend="postgres",
        ledger_spreadsheet_id=None,
        ledger_range="Ledger!A:Z",
        postgres_dsn="postgresql://user:pass@localhost:5432/db",
        postgres_table="ledger_records",
    )


def test_list_inbox_files_filters_unsupported_keys() -> None:
    fake = _FakeR2Client(
        [
            {
                "IsTruncated": False,
                "Contents": [
                    {"Key": "inbox/a.jpg", "Size": 10},
                    {"Key": "inbox/b.txt", "Size": 11},
                    {"Key": "inbox/sub/", "Size": 0},
                ],
            }
        ]
    )
    service = R2Service(fake, _settings())
    files = service.list_inbox_files()
    assert len(files) == 1
    assert files[0]["id"] == "inbox/a.jpg"


def test_download_and_archive_move() -> None:
    fake = _FakeR2Client([{"IsTruncated": False, "Contents": []}])
    service = R2Service(fake, _settings())
    out = service.download_file("inbox/a.jpg", "tmp/out.jpg")
    archived = service.move_to_archive("inbox/a.jpg")
    assert out.exists()
    assert archived == "archive/a.jpg"
    assert len(fake.copy_calls) == 1
    assert len(fake.delete_calls) == 1

