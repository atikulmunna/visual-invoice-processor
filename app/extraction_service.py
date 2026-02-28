from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any, Protocol


class VisionClient(Protocol):
    def extract_json(self, file_path: Path, model_name: str, prompt: str) -> str:
        """Return raw model text output intended to be valid JSON."""


class ExtractionError(RuntimeError):
    def __init__(self, message: str, code: str = "extraction_failed") -> None:
        super().__init__(message)
        self.code = code


SYSTEM_PROMPT = (
    "Extract invoice/receipt data as strict JSON only. "
    "Do not include markdown, prose, or code fences."
)

CORRECTIVE_PROMPT = (
    "Your previous output was invalid. Return only one valid JSON object "
    "with no extra text."
)


def _mime_for_path(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in {".jpg", ".jpeg"}:
        return "image/jpeg"
    if suffix == ".png":
        return "image/png"
    if suffix == ".pdf":
        return "application/pdf"
    raise ExtractionError(f"Unsupported file extension: {suffix}", code="unsupported_type")


def _parse_json_payload(raw_text: str) -> dict[str, Any]:
    try:
        payload = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise ExtractionError("Model returned invalid JSON", code="invalid_json") from exc
    if not isinstance(payload, dict):
        raise ExtractionError("Model output must be a JSON object", code="invalid_json_shape")
    return payload


class OpenAIVisionClient:
    def __init__(self, api_key: str) -> None:
        try:
            from openai import OpenAI
        except ImportError as exc:
            raise RuntimeError("openai package is required for OpenAI extraction") from exc
        self._client = OpenAI(api_key=api_key)

    def extract_json(self, file_path: Path, model_name: str, prompt: str) -> str:
        mime = _mime_for_path(file_path)
        encoded = base64.b64encode(file_path.read_bytes()).decode("ascii")
        data_uri = f"data:{mime};base64,{encoded}"
        response = self._client.chat.completions.create(
            model=model_name,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": data_uri}},
                    ],
                },
            ],
        )
        text = response.choices[0].message.content
        if not text:
            raise ExtractionError("OpenAI returned empty response", code="empty_response")
        return text


class GeminiVisionClient:
    def __init__(self, api_key: str) -> None:
        try:
            from google import genai
        except ImportError as exc:
            raise RuntimeError("google-genai package is required for Gemini extraction") from exc
        self._client = genai.Client(api_key=api_key)

    def extract_json(self, file_path: Path, model_name: str, prompt: str) -> str:
        mime = _mime_for_path(file_path)
        response = self._client.models.generate_content(
            model=model_name,
            contents=[
                prompt,
                {
                    "mime_type": mime,
                    "data": file_path.read_bytes(),
                },
            ],
        )
        text = getattr(response, "text", None)
        if not text:
            raise ExtractionError("Gemini returned empty response", code="empty_response")
        return text


def _build_default_client(provider: str) -> VisionClient:
    import os

    normalized = provider.strip().lower()
    if normalized == "openai":
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ExtractionError("OPENAI_API_KEY is required for OpenAI provider", code="missing_api_key")
        return OpenAIVisionClient(api_key=api_key)
    if normalized == "gemini":
        api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
        if not api_key:
            raise ExtractionError("GEMINI_API_KEY is required for Gemini provider", code="missing_api_key")
        return GeminiVisionClient(api_key=api_key)
    raise ExtractionError(
        "Unsupported provider. Expected one of: openai, gemini",
        code="unsupported_provider",
    )


def extract_document(
    file_path: str | Path,
    model_name: str = "gpt-4o-mini",
    provider: str = "openai",
    client: VisionClient | None = None,
) -> dict[str, Any]:
    path = Path(file_path)
    if not path.exists():
        raise ExtractionError(f"File not found: {path}", code="file_not_found")

    active_client = client or _build_default_client(provider)

    first_text = active_client.extract_json(path, model_name, SYSTEM_PROMPT)
    try:
        return _parse_json_payload(first_text)
    except ExtractionError as exc:
        if exc.code not in {"invalid_json", "invalid_json_shape"}:
            raise

    corrective_text = active_client.extract_json(path, model_name, CORRECTIVE_PROMPT)
    return _parse_json_payload(corrective_text)
