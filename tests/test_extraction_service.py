from __future__ import annotations

from pathlib import Path

import pytest

from app.extraction_service import ExtractionError, extract_document


class _FakeVisionClient:
    def __init__(self, outputs: list[str]) -> None:
        self._outputs = outputs
        self.calls: list[tuple[str, str]] = []

    def extract_json(self, file_path: Path, model_name: str, prompt: str) -> str:
        self.calls.append((model_name, prompt))
        if not self._outputs:
            raise RuntimeError("No outputs configured")
        return self._outputs.pop(0)


def test_extract_document_success_first_try(tmp_path: Path) -> None:
    file_path = tmp_path / "doc.jpg"
    file_path.write_bytes(b"img")
    client = _FakeVisionClient(outputs=['{"vendor_name":"Test","total_amount":12.5}'])

    payload = extract_document(file_path=file_path, model_name="gpt-4o-mini", client=client)
    assert payload["vendor_name"] == "Test"
    assert len(client.calls) == 1


def test_extract_document_retries_once_on_invalid_json(tmp_path: Path) -> None:
    file_path = tmp_path / "doc.png"
    file_path.write_bytes(b"img")
    client = _FakeVisionClient(
        outputs=[
            "not json",
            '{"vendor_name":"Recovered","total_amount":100.0}',
        ]
    )

    payload = extract_document(file_path=file_path, model_name="gpt-4o-mini", client=client)
    assert payload["vendor_name"] == "Recovered"
    assert len(client.calls) == 2


def test_extract_document_fails_after_corrective_retry(tmp_path: Path) -> None:
    file_path = tmp_path / "doc.pdf"
    file_path.write_bytes(b"%PDF")
    client = _FakeVisionClient(outputs=["nope", "still nope"])

    with pytest.raises(ExtractionError, match="invalid JSON"):
        extract_document(file_path=file_path, client=client)
    assert len(client.calls) == 2


def test_extract_document_missing_file_raises() -> None:
    with pytest.raises(ExtractionError, match="File not found"):
        extract_document("missing.jpg", client=_FakeVisionClient(outputs=['{}']))

