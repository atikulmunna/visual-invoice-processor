from __future__ import annotations

from pathlib import Path

import pytest

from app.main import DocumentRejectedError
from app.serverless_worker import inspect_document


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
