from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from app.extraction_service import (
    CORRECTIVE_PROMPT,
    DEFAULT_MODEL,
    USER_EXTRACTION_PROMPT,
    ExtractionError,
    OpenRouterClient,
    extract_document,
)


class _FakeVisionClient:
    provider_name = "openrouter"

    def __init__(self, outputs: list[str]) -> None:
        self.outputs = outputs
        self.calls: list[tuple[str, str]] = []

    def extract_json(self, file_path: Path, model_name: str, prompt: str) -> str:
        self.calls.append((model_name, prompt))
        return self.outputs.pop(0)


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict[str, Any]) -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = str(payload)

    def json(self) -> dict[str, Any]:
        return self._payload


def _image(tmp_path: Path) -> Path:
    file_path = tmp_path / "receipt.jpg"
    file_path.write_bytes(b"\xff\xd8\xff fake image")
    return file_path


def test_extract_document_success_first_try(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("OPENROUTER_MODEL", raising=False)
    client = _FakeVisionClient(outputs=['{"vendor_name":"Test","total_amount":12.5}'])

    payload = extract_document(_image(tmp_path), client=client)

    assert payload["vendor_name"] == "Test"
    assert payload["_provider"] == "openrouter"
    assert client.calls == [(DEFAULT_MODEL, USER_EXTRACTION_PROMPT)]


def test_markdown_fenced_json_is_repaired_without_a_second_call(tmp_path: Path) -> None:
    client = _FakeVisionClient(outputs=['Here you go:\n```json\n{"vendor_name":"Fenced"}\n```'])

    payload = extract_document(_image(tmp_path), client=client)

    assert payload["vendor_name"] == "Fenced"
    assert len(client.calls) == 1


def test_unrepairable_output_retries_once_with_full_instructions(tmp_path: Path) -> None:
    client = _FakeVisionClient(outputs=["not json at all", '{"vendor_name":"Retry"}'])

    payload = extract_document(_image(tmp_path), client=client)

    assert payload["vendor_name"] == "Retry"
    assert client.calls[1][1] == f"{USER_EXTRACTION_PROMPT} {CORRECTIVE_PROMPT}"


def test_extract_document_fails_after_corrective_retry(tmp_path: Path) -> None:
    client = _FakeVisionClient(outputs=["nope", "still nope"])

    with pytest.raises(ExtractionError, match="invalid JSON"):
        extract_document(_image(tmp_path), client=client)


def test_extract_document_missing_file_raises() -> None:
    with pytest.raises(ExtractionError, match="File not found"):
        extract_document("missing.jpg", client=_FakeVisionClient(outputs=["{}"]))


def test_extract_document_requires_openrouter_key(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)

    with pytest.raises(ExtractionError, match="OPENROUTER_API_KEY") as exc_info:
        extract_document(_image(tmp_path))

    assert exc_info.value.code == "missing_api_key"


def test_extract_document_rejects_other_providers(tmp_path: Path) -> None:
    with pytest.raises(ExtractionError) as exc_info:
        extract_document(_image(tmp_path), provider="mistral", client=_FakeVisionClient(outputs=["{}"]))

    assert exc_info.value.code == "unsupported_provider"


def test_model_comes_from_environment_when_auto(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("OPENROUTER_MODEL", "vendor/cheaper-model")
    client = _FakeVisionClient(outputs=["{}"])

    extract_document(_image(tmp_path), client=client)

    assert client.calls[0][0] == "vendor/cheaper-model"


def test_openrouter_client_sends_images_inline_with_a_token_cap(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    sent: dict[str, Any] = {}

    def _fake_post(url: str, **kwargs: Any) -> _FakeResponse:
        sent.update(kwargs, url=url)
        return _FakeResponse(200, {"choices": [{"message": {"content": '{"vendor_name":"Img"}'}}]})

    monkeypatch.setattr("app.extraction_service.requests.post", _fake_post)

    text = OpenRouterClient("sk-test", max_tokens=900).extract_json(_image(tmp_path), "m/model", "prompt")

    body = sent["json"]
    document = body["messages"][1]["content"][1]
    assert text == '{"vendor_name":"Img"}'
    assert sent["url"] == "https://openrouter.ai/api/v1/chat/completions"
    assert sent["headers"]["Authorization"] == "Bearer sk-test"
    assert body["max_tokens"] == 900
    assert body["response_format"] == {"type": "json_object"}
    assert document["type"] == "image_url"
    assert document["image_url"]["url"].startswith("data:image/jpeg;base64,")
    assert "plugins" not in body


def test_openrouter_client_reads_pdfs_natively_never_with_paid_ocr(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    sent: dict[str, Any] = {}

    def _fake_post(url: str, **kwargs: Any) -> _FakeResponse:
        sent.update(kwargs)
        return _FakeResponse(200, {"choices": [{"message": {"content": "{}"}}]})

    monkeypatch.setattr("app.extraction_service.requests.post", _fake_post)
    pdf = tmp_path / "invoice.pdf"
    pdf.write_bytes(b"%PDF-1.4 fake")

    OpenRouterClient("sk-test").extract_json(pdf, "m/model", "prompt")

    document = sent["json"]["messages"][1]["content"][1]
    assert document["type"] == "file"
    assert document["file"]["filename"] == "invoice.pdf"
    assert document["file"]["file_data"].startswith("data:application/pdf;base64,")
    assert sent["json"]["plugins"] == [{"id": "file-parser", "pdf": {"engine": "native"}}]


def test_openrouter_client_reports_http_errors(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "app.extraction_service.requests.post",
        lambda url, **kwargs: _FakeResponse(402, {"error": {"message": "Insufficient credits"}}),
    )

    with pytest.raises(ExtractionError, match="402") as exc_info:
        OpenRouterClient("sk-test").extract_json(_image(tmp_path), "m/model", "prompt")

    assert exc_info.value.code == "provider_request_failed"
