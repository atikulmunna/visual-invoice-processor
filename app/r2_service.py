from __future__ import annotations

import mimetypes
from pathlib import Path
from typing import Any

from app.config import Settings
from app.drive_service import is_supported_mime_type


class R2Service:
    def __init__(self, s3_client: Any, settings: Settings) -> None:
        self._s3 = s3_client
        self._settings = settings
        if not settings.r2_bucket_name:
            raise ValueError("R2_BUCKET_NAME must be configured for R2Service.")
        self._bucket = settings.r2_bucket_name

    @classmethod
    def from_settings(cls, settings: Settings) -> "R2Service":
        try:
            import boto3
        except ImportError as exc:
            raise RuntimeError("boto3 is required for Cloudflare R2 ingestion") from exc
        client = boto3.client(
            "s3",
            endpoint_url=settings.r2_endpoint_url,
            aws_access_key_id=settings.r2_access_key_id,
            aws_secret_access_key=settings.r2_secret_access_key,
            region_name="auto",
        )
        return cls(s3_client=client, settings=settings)

    def list_inbox_files(self, prefix: str | None = None) -> list[dict[str, Any]]:
        active_prefix = prefix if prefix is not None else self._settings.r2_inbox_prefix
        continuation: str | None = None
        files: list[dict[str, Any]] = []

        while True:
            kwargs: dict[str, Any] = {"Bucket": self._bucket, "Prefix": active_prefix}
            if continuation:
                kwargs["ContinuationToken"] = continuation
            response = self._s3.list_objects_v2(**kwargs)
            for item in response.get("Contents", []):
                key = item.get("Key", "")
                if not key or key.endswith("/"):
                    continue
                mime, _ = mimetypes.guess_type(key)
                mime_type = mime or "application/octet-stream"
                if not is_supported_mime_type(mime_type, self._settings.allowed_mime_types):
                    continue
                files.append(
                    {
                        "id": key,
                        "name": Path(key).name,
                        "mimeType": mime_type,
                        "size": str(item.get("Size", "")),
                        "lastModified": str(item.get("LastModified", "")),
                    }
                )

            if not response.get("IsTruncated"):
                break
            continuation = response.get("NextContinuationToken")
        return files

    def download_file(self, object_key: str, out_path: str | Path) -> Path:
        output_path = Path(out_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        self._s3.download_file(self._bucket, object_key, str(output_path))
        return output_path

    def move_to_archive(self, object_key: str, archive_prefix: str | None = None) -> str:
        active_archive_prefix = archive_prefix if archive_prefix is not None else self._settings.r2_archive_prefix
        destination_key = f"{active_archive_prefix.rstrip('/')}/{Path(object_key).name}"
        self._s3.copy_object(
            Bucket=self._bucket,
            CopySource={"Bucket": self._bucket, "Key": object_key},
            Key=destination_key,
        )
        self._s3.delete_object(Bucket=self._bucket, Key=object_key)
        return destination_key

