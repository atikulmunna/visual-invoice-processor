from __future__ import annotations

from types import SimpleNamespace
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


def test_native_s3_client_uses_regional_virtual_host_endpoint(monkeypatch: Any) -> None:
    import boto3

    captured: dict[str, Any] = {}
    fake = _FakeS3()

    def _client(service_name: str, **kwargs: Any) -> _FakeS3:
        captured["service_name"] = service_name
        captured.update(kwargs)
        return fake

    monkeypatch.setattr(boto3, "client", _client)
    settings = SimpleNamespace(
        ingestion_backend="s3",
        s3_bucket_name="alpha-invoices",
        s3_region="ap-southeast-1",
        s3_inbox_prefix="inbox/",
        s3_archive_prefix="archive/",
        allowed_mime_types=("application/pdf",),
    )

    service = ObjectStorageService.from_settings(settings)

    assert service.bucket_name == "alpha-invoices"
    assert captured["service_name"] == "s3"
    assert captured["region_name"] == "ap-southeast-1"
    assert captured["endpoint_url"] == "https://s3.ap-southeast-1.amazonaws.com"
    assert captured["config"].s3["addressing_style"] == "virtual"


class _ClientError(Exception):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class _ReviewS3:
    def __init__(self, existing: set[str], failure: str | None = None) -> None:
        self.existing = existing
        self.failure = failure
        self.url_calls: list[tuple[str, dict[str, Any], int]] = []

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        if self.failure:
            raise _ClientError(self.failure)
        if Key not in self.existing:
            raise _ClientError("404")
        return {}

    def generate_presigned_url(self, operation: str, *, Params: dict[str, Any], ExpiresIn: int) -> str:
        self.url_calls.append((operation, Params, ExpiresIn))
        return f"https://bucket.example/{Params['Key']}?signed"


def _service(client: Any) -> ObjectStorageService:
    return ObjectStorageService(
        client, bucket="alpha-invoices", inbox_prefix="inbox/", archive_prefix="archive/",
        allowed_mime_types=("application/pdf",),
    )


def test_archive_key_mirrors_the_inbox_path() -> None:
    service = _service(_ReviewS3(set()))

    assert service.archive_key_for("inbox/user/job/receipt.pdf") == "archive/user/job/receipt.pdf"
    assert service.archive_key_for("other/receipt.pdf") == "archive/other/receipt.pdf"


def test_object_exists_distinguishes_missing_from_real_errors() -> None:
    assert _service(_ReviewS3({"archive/a.pdf"})).object_exists("archive/a.pdf") is True
    assert _service(_ReviewS3(set())).object_exists("archive/a.pdf") is False
    try:
        _service(_ReviewS3(set(), failure="AccessDenied")).object_exists("archive/a.pdf")
    except _ClientError:
        pass
    else:
        raise AssertionError("A permissions error must not look like a missing file")


def test_download_links_are_short_lived_and_shown_inline() -> None:
    client = _ReviewS3(set())

    url = _service(client).create_presigned_download("archive/a.pdf", content_type="application/pdf")

    operation, params, expires = client.url_calls[0]
    assert url.startswith("https://bucket.example/archive/a.pdf")
    assert operation == "get_object"
    assert params == {
        "Bucket": "alpha-invoices",
        "Key": "archive/a.pdf",
        "ResponseContentType": "application/pdf",
        "ResponseContentDisposition": "inline",
    }
    assert expires == 300
