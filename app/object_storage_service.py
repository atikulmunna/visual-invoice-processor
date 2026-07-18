from __future__ import annotations

import mimetypes
from pathlib import Path
from typing import Any

from app.config import Settings
from app.drive_service import is_supported_mime_type


class ObjectStorageService:
    """S3-compatible document storage for native S3 and Cloudflare R2."""

    def __init__(
        self,
        s3_client: Any,
        *,
        bucket: str,
        inbox_prefix: str,
        archive_prefix: str,
        allowed_mime_types: tuple[str, ...],
    ) -> None:
        if not bucket:
            raise ValueError("An object-storage bucket must be configured")
        self._s3 = s3_client
        self._bucket = bucket
        self._inbox_prefix = inbox_prefix
        self._archive_prefix = archive_prefix
        self._allowed_mime_types = allowed_mime_types

    @property
    def bucket_name(self) -> str:
        return self._bucket

    @property
    def inbox_prefix(self) -> str:
        return self._inbox_prefix

    @classmethod
    def from_settings(cls, settings: Settings) -> "ObjectStorageService":
        try:
            import boto3
        except ImportError as exc:
            raise RuntimeError("boto3 is required for object storage") from exc

        if settings.ingestion_backend == "s3":
            if not settings.s3_bucket_name:
                raise ValueError("S3_BUCKET_NAME must be configured")
            client = boto3.client("s3", region_name=settings.s3_region)
            return cls(
                client,
                bucket=settings.s3_bucket_name,
                inbox_prefix=settings.s3_inbox_prefix,
                archive_prefix=settings.s3_archive_prefix,
                allowed_mime_types=settings.allowed_mime_types,
            )

        if settings.ingestion_backend != "r2":
            raise ValueError("Object storage requires INGESTION_BACKEND=s3 or r2")
        if not settings.r2_bucket_name:
            raise ValueError("R2_BUCKET_NAME must be configured")
        client = boto3.client(
            "s3",
            endpoint_url=settings.r2_endpoint_url,
            aws_access_key_id=settings.r2_access_key_id,
            aws_secret_access_key=settings.r2_secret_access_key,
            region_name="auto",
        )
        return cls(
            client,
            bucket=settings.r2_bucket_name,
            inbox_prefix=settings.r2_inbox_prefix,
            archive_prefix=settings.r2_archive_prefix,
            allowed_mime_types=settings.allowed_mime_types,
        )

    def list_inbox_files(self, prefix: str | None = None) -> list[dict[str, Any]]:
        active_prefix = prefix if prefix is not None else self._inbox_prefix
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
                mime_type = mimetypes.guess_type(key)[0] or "application/octet-stream"
                if not is_supported_mime_type(mime_type, self._allowed_mime_types):
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

    def upload_bytes(self, object_key: str, content: bytes, *, content_type: str | None = None) -> str:
        extra_args: dict[str, Any] = {}
        if content_type:
            extra_args["ContentType"] = content_type
        self._s3.put_object(Bucket=self._bucket, Key=object_key, Body=content, **extra_args)
        return object_key

    def create_presigned_upload(
        self,
        object_key: str,
        *,
        content_type: str,
        max_bytes: int,
        expires_seconds: int = 300,
    ) -> dict[str, Any]:
        return self._s3.generate_presigned_post(
            Bucket=self._bucket,
            Key=object_key,
            Fields={"Content-Type": content_type},
            Conditions=[
                {"Content-Type": content_type},
                ["content-length-range", 1, max_bytes],
            ],
            ExpiresIn=expires_seconds,
        )

    def move_to_archive(self, object_key: str, archive_prefix: str | None = None) -> str:
        active_archive_prefix = archive_prefix if archive_prefix is not None else self._archive_prefix
        relative_key = object_key
        normalized_inbox = self._inbox_prefix.rstrip("/") + "/"
        if object_key.startswith(normalized_inbox):
            relative_key = object_key[len(normalized_inbox) :]
        destination_key = f"{active_archive_prefix.rstrip('/')}/{relative_key.lstrip('/')}"
        self._s3.copy_object(
            Bucket=self._bucket,
            CopySource={"Bucket": self._bucket, "Key": object_key},
            Key=destination_key,
        )
        self._s3.delete_object(Bucket=self._bucket, Key=object_key)
        return destination_key

    def delete_object(self, object_key: str) -> None:
        self._s3.delete_object(Bucket=self._bucket, Key=object_key)

    def retrigger_object(self, object_key: str) -> None:
        self._s3.copy_object(
            Bucket=self._bucket,
            CopySource={"Bucket": self._bucket, "Key": object_key},
            Key=object_key,
            MetadataDirective="REPLACE",
        )
