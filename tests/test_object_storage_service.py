from __future__ import annotations

from typing import Any

from app.object_storage_service import ObjectStorageService


class _FakeS3:
    def __init__(self) -> None:
        self.presign_calls: list[dict[str, Any]] = []

    def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]:
        return {
            "IsTruncated": False,
            "Contents": [
                {"Key": "inbox/a.pdf", "Size": 12},
                {"Key": "inbox/ignore.txt", "Size": 3},
            ],
        }

    def generate_presigned_post(self, **kwargs: Any) -> dict[str, Any]:
        self.presign_calls.append(kwargs)
        return {"url": "https://bucket.example", "fields": {"key": kwargs["Key"]}}


def test_native_storage_filters_and_presigns_with_size_condition() -> None:
    fake = _FakeS3()
    service = ObjectStorageService(
        fake,
        bucket="alpha-invoices",
        inbox_prefix="inbox/",
        archive_prefix="archive/",
        allowed_mime_types=("application/pdf",),
    )

    assert [item["id"] for item in service.list_inbox_files()] == ["inbox/a.pdf"]
    result = service.create_presigned_upload(
        "inbox/user/job/a.pdf",
        content_type="application/pdf",
        max_bytes=5_242_880,
    )

    assert result["fields"]["key"] == "inbox/user/job/a.pdf"
    assert ["content-length-range", 1, 5_242_880] in fake.presign_calls[0]["Conditions"]
