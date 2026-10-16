from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Any

import pytest
from pypdf import PdfWriter

from app.alpha_store import AlphaQuotaError
from app.config import Settings
from app.pipeline import DocumentRejectedError
from app.serverless_worker import _check_upload, _finish_job, inspect_document


def test_inspect_png_and_jpeg(tmp_path: Path) -> None:
    png = tmp_path / "invoice.png"
    png.write_bytes(b"\x89PNG\r\n\x1a\nrest")
    jpeg = tmp_path / "invoice.jpg"
    jpeg.write_bytes(b"\xff\xd8\xffrest")

    assert inspect_document(png, max_bytes=100, max_pdf_pages=5) == ("image/png", 1)
    assert inspect_document(jpeg, max_bytes=100, max_pdf_pages=5) == ("image/jpeg", 1)


def test_inspect_rejects_size_and_signature(tmp_path: Path) -> None:
    empty = tmp_path / "empty.pdf"
    empty.write_bytes(b"")
    bad = tmp_path / "bad.pdf"
    bad.write_bytes(b"not-a-document")

    with pytest.raises(DocumentRejectedError, match="between"):
        inspect_document(empty, max_bytes=100, max_pdf_pages=5)
    with pytest.raises(DocumentRejectedError, match="signature"):
        inspect_document(bad, max_bytes=100, max_pdf_pages=5)


def test_inspect_accepts_pdf_with_leading_ascii_whitespace(tmp_path: Path) -> None:
    buffer = BytesIO()
    writer = PdfWriter()
    writer.add_blank_page(width=100, height=100)
    writer.write(buffer)
    pdf = tmp_path / "leading-newline.pdf"
    pdf.write_bytes(b"\n" + buffer.getvalue())

    assert inspect_document(pdf, max_bytes=10_000, max_pdf_pages=5) == ("application/pdf", 1)


def test_inspect_rejects_pdf_with_non_whitespace_prefix(tmp_path: Path) -> None:
    buffer = BytesIO()
    writer = PdfWriter()
    writer.add_blank_page(width=100, height=100)
    writer.write(buffer)
    pdf = tmp_path / "prefixed.pdf"
    pdf.write_bytes(b"x" + buffer.getvalue())

    with pytest.raises(DocumentRejectedError, match="signature"):
        inspect_document(pdf, max_bytes=10_000, max_pdf_pages=5)


class _Store:
    def __init__(self, quota_left: bool = True) -> None:
        self.quota_left = quota_left
        self.reserved: list[tuple[str, int]] = []
        self.completed: list[dict[str, Any]] = []

    def reserve_pages(self, job_id: str, page_count: int, *, global_limit: int) -> None:
        if not self.quota_left:
            raise AlphaQuotaError("The shared page allowance is used up")
        self.reserved.append((job_id, page_count))

    def complete_job(self, job_id: str, **outcome: Any) -> None:
        self.completed.append({"job_id": job_id, **outcome})


class _Storage:
    def __init__(self, broken: bool = False) -> None:
        self.broken = broken
        self.actions: list[tuple[str, str]] = []

    def move_to_archive(self, object_key: str) -> None:
        self.actions.append(("archive", object_key))

    def delete_object(self, object_key: str) -> None:
        if self.broken:
            raise RuntimeError("S3 is unreachable")
        self.actions.append(("delete", object_key))


def _png(tmp_path: Path) -> Path:
    path = tmp_path / "receipt.png"
    path.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 32)
    return path


def test_uploads_must_match_their_declared_type_and_fit_the_page_allowance(tmp_path: Path) -> None:
    settings = Settings(max_upload_bytes=1024, max_pdf_pages=5)
    store = _Store()

    assert _check_upload(_png(tmp_path), {"id": "job-1", "content_type": "image/png"}, settings, store) == {"page_count": 1}
    assert store.reserved == [("job-1", 1)]
    with pytest.raises(DocumentRejectedError) as mismatch:
        _check_upload(_png(tmp_path), {"id": "job-2", "content_type": "application/pdf"}, settings, store)
    with pytest.raises(DocumentRejectedError) as exhausted:
        _check_upload(_png(tmp_path), {"id": "job-3", "content_type": "image/png"}, settings, _Store(quota_left=False))
    assert mismatch.value.code == "content_type_mismatch"
    assert exhausted.value.code == "global_page_limit_exceeded"


@pytest.mark.parametrize(
    ("result_status", "job_status", "action"),
    [
        ("STORED", "STORED", None),
        ("REVIEW_REQUIRED", "REVIEW_REQUIRED", "archive"),
        ("SKIPPED_DUPLICATE", "DUPLICATE", "delete"),
        ("REJECTED", "REJECTED", "delete"),
        ("FAILED", "FAILED", None),
        ("SOMETHING_NEW", "FAILED", None),
    ],
)
def test_finished_jobs_keep_the_file_only_while_someone_may_need_it(
    result_status: str, job_status: str, action: str | None
) -> None:
    store, storage = _Store(), _Storage()

    _finish_job({"status": result_status, "error_code": "x"}, "job-1", "inbox/u/j/a.png", storage, store)

    assert storage.actions == ([(action, "inbox/u/j/a.png")] if action else [])
    assert store.completed[0]["status"] == job_status
    assert store.completed[0]["error_code"] == "x"


def test_a_failed_cleanup_still_records_the_outcome() -> None:
    store = _Store()

    _finish_job({"status": "REJECTED"}, "job-1", "inbox/u/j/a.png", _Storage(broken=True), store)

    assert store.completed[0]["status"] == "REJECTED"


def test_the_worker_leaves_log_formatting_to_the_lambda_runtime() -> None:
    # Lambda drops log lines its own formatter did not write, so the worker must never replace it.
    import logging

    from app.serverless_worker import handle_s3_event

    runtime_formatter = logging.Formatter("%(message)s\n")
    handler = logging.StreamHandler()
    handler.setFormatter(runtime_formatter)
    root = logging.getLogger()
    root.addHandler(handler)
    try:
        assert handle_s3_event({"Records": [{"eventSource": "aws:sqs"}]}) == []
        assert handler.formatter is runtime_formatter
    finally:
        root.removeHandler(handler)
