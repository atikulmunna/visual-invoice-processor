from __future__ import annotations

from pathlib import Path
from typing import Any

from app.config import Settings

SUPPORTED_MIME_TYPES = (
    "image/jpeg",
    "image/png",
    "application/pdf",
)


def is_supported_mime_type(mime_type: str, allowed_mime_types: tuple[str, ...]) -> bool:
    return mime_type in allowed_mime_types


class DriveService:
    def __init__(self, drive_client: Any, settings: Settings) -> None:
        self._drive = drive_client
        self._settings = settings

    @classmethod
    def from_credentials(cls, credentials: Any, settings: Settings) -> "DriveService":
        try:
            from googleapiclient.discovery import build
        except ImportError as exc:
            raise RuntimeError(
                "google-api-python-client is required for Drive API access"
            ) from exc
        drive_client = build("drive", "v3", credentials=credentials, cache_discovery=False)
        return cls(drive_client=drive_client, settings=settings)

    def list_inbox_files(self, folder_id: str | None = None) -> list[dict[str, str]]:
        target_folder = folder_id or self._settings.drive_inbox_folder_id
        query = (
            f"'{target_folder}' in parents and trashed = false "
            "and (mimeType='image/jpeg' or mimeType='image/png' or mimeType='application/pdf')"
        )
        response = (
            self._drive.files()
            .list(
                q=query,
                fields="files(id,name,mimeType,size,createdTime,modifiedTime)",
                pageSize=1000,
            )
            .execute()
        )
        files = response.get("files", [])
        return [
            f
            for f in files
            if is_supported_mime_type(f.get("mimeType", ""), self._settings.allowed_mime_types)
        ]

    def download_file(self, file_id: str, out_path: str | Path) -> Path:
        try:
            from googleapiclient.http import MediaIoBaseDownload
        except ImportError as exc:
            raise RuntimeError(
                "google-api-python-client is required for Drive API downloads"
            ) from exc

        output_path = Path(out_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        request = self._drive.files().get_media(fileId=file_id)
        with output_path.open("wb") as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
        return output_path

