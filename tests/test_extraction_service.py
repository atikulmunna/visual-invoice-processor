from __future__ import annotations

from pathlib import Path

import pytest

from app.extraction_service import (
    ExtractionError,
    MultiProviderVisionClient,
    extract_document,
)


class _FakeVisionClient:
    def __init__(self, outputs: list[str]) -> None:
        self._outputs = outputs
        self.calls: list[tuple[str, str]] = []

    def extract_json(self, file_path: Path, model_name: str, prompt: str) -> str:
        self.calls.append((model_name, prompt))
        if not self._outputs:
            raise RuntimeError("No outputs configured")
        return self._outputs.pop(0)


class _FakeVisionClientWithOcr(_FakeVisionClient):
    def __init__(self, outputs: list[str], ocr_text: str) -> None:
        super().__init__(outputs=outputs)
        self.last_ocr_text = ocr_text


class _AlwaysFailClient:
    def extract_json(self, file_path: Path, model_name: str, prompt: str) -> str:
        _ = (file_path, model_name, prompt)
        raise RuntimeError("provider down")


def test_extract_document_success_first_try(tmp_path: Path) -> None:
    file_path = tmp_path / "doc.jpg"
    file_path.write_bytes(b"img")
    client = _FakeVisionClient(outputs=['{"vendor_name":"Test","total_amount":12.5}'])

    payload = extract_document(file_path=file_path, model_name="gpt-4o-mini", client=client)
    assert payload["vendor_name"] == "Test"
    assert payload["_provider"] == "auto"
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


def test_multi_provider_client_falls_back_to_next_provider(tmp_path: Path) -> None:
    file_path = tmp_path / "doc.jpg"
    file_path.write_bytes(b"img")
    client = MultiProviderVisionClient(
        providers=[
            ("mistral", _AlwaysFailClient(), "pixtral-large-latest"),
            ("openrouter", _FakeVisionClient(outputs=['{"vendor_name":"Fallback"}']), "mistralai/pixtral-12b"),
        ]
    )

    payload = extract_document(file_path=file_path, client=client, model_name="auto")
    assert payload["vendor_name"] == "Fallback"


def test_extract_document_auto_provider_requires_any_key(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    file_path = tmp_path / "doc.jpg"
    file_path.write_bytes(b"img")
    monkeypatch.delenv("MISTRAL_API_KEY", raising=False)
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)

    with pytest.raises(ExtractionError, match="No provider API key found"):
        extract_document(file_path=file_path, provider="auto")


def test_extract_document_carries_ocr_text_when_available(tmp_path: Path) -> None:
    file_path = tmp_path / "doc.jpg"
    file_path.write_bytes(b"img")
    client = _FakeVisionClientWithOcr(
        outputs=['{"vendor_name":"A","total_amount":1.0}'],
        ocr_text="Invoice date: 01/03/2026",
    )
    payload = extract_document(file_path=file_path, client=client)
    assert payload["_ocr_text"].startswith("Invoice date")


def test_extract_document_uses_client_provider_name_if_available(tmp_path: Path) -> None:
    file_path = tmp_path / "doc.jpg"
    file_path.write_bytes(b"img")
    client = _FakeVisionClient(outputs=['{"vendor_name":"A","total_amount":1.0}'])
    client.provider_name = "mistral"
    payload = extract_document(file_path=file_path, client=client)
    assert payload["_provider"] == "mistral"
