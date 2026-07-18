from __future__ import annotations

from typing import Any

from app.config import Settings
from app.object_storage_service import ObjectStorageService


class R2Service(ObjectStorageService):
    def __init__(self, s3_client: Any, settings: Settings) -> None:
        self._settings = settings
        if not settings.r2_bucket_name:
            raise ValueError("R2_BUCKET_NAME must be configured for R2Service.")
        super().__init__(
            s3_client,
            bucket=settings.r2_bucket_name,
            inbox_prefix=settings.r2_inbox_prefix,
            archive_prefix=settings.r2_archive_prefix,
            allowed_mime_types=settings.allowed_mime_types,
        )

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
