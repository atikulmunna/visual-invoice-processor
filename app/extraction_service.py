from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any, Protocol

import requests


class VisionClient(Protocol):
    def extract_json(self, file_path: Path, model_name: str, prompt: str) -> str:
        """Return raw model text output intended to be valid JSON."""


class ExtractionError(RuntimeError):
    def __init__(self, message: str, code: str = "extraction_failed") -> None:
        super().__init__(message)
        self.code = code


SYSTEM_PROMPT = "Return strict JSON only. No markdown or prose."

USER_EXTRACTION_PROMPT = (
    "Extract invoice/receipt fields into one JSON object. "
    "Use null for unknown values."
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


class OpenAICompatibleVisionClient:
    def __init__(
        self,
        *,
        api_key: str,
        base_url: str,
        provider_name: str,
        default_headers: dict[str, str] | None = None,
    ) -> None:
        try:
            from openai import OpenAI
        except ImportError as exc:
            raise RuntimeError("openai package is required for OpenAI-compatible providers") from exc
        self._provider_name = provider_name
        self._client = OpenAI(
            api_key=api_key,
            base_url=base_url,
            default_headers=default_headers or {},
        )

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
            raise ExtractionError(
                f"{self._provider_name} returned empty response",
                code="empty_response",
            )
        return text


class MistralVisionClient:
    def __init__(self, api_key: str) -> None:
        self._api_key = api_key
        self._base_url = "https://api.mistral.ai/v1"

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }

    def _ocr_text(self, file_path: Path) -> str:
        mime = _mime_for_path(file_path)
        encoded = base64.b64encode(file_path.read_bytes()).decode("ascii")
        data_uri = f"data:{mime};base64,{encoded}"
        doc_type = "document_url" if mime == "application/pdf" else "image_url"
        doc_key = "document_url" if doc_type == "document_url" else "image_url"

        response = requests.post(
            f"{self._base_url}/ocr",
            headers=self._headers(),
            json={
                "model": "mistral-ocr-latest",
                "document": {
                    "type": doc_type,
                    doc_key: data_uri,
                },
            },
            timeout=60,
        )
        if response.status_code >= 400:
            raise ExtractionError(
                f"Mistral OCR failed with status {response.status_code}: {response.text[:300]}",
                code="provider_request_failed",
            )

        payload = response.json()
        pages = payload.get("pages", [])
        text_chunks: list[str] = []
        for page in pages:
            markdown = page.get("markdown")
            if isinstance(markdown, str) and markdown.strip():
                text_chunks.append(markdown)
        if not text_chunks:
            raise ExtractionError("Mistral OCR returned no text", code="empty_response")
        return "\n\n".join(text_chunks)

    def extract_json(self, file_path: Path, model_name: str, prompt: str) -> str:
        ocr_text = self._ocr_text(file_path)
        response = requests.post(
            f"{self._base_url}/chat/completions",
            headers=self._headers(),
            json={
                "model": model_name,
                "response_format": {"type": "json_object"},
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {
                        "role": "user",
                        "content": (
                            f"{prompt}\n\n"
                            "Extract fields from this OCR text:\n"
                            f"{ocr_text}"
                        ),
                    },
                ],
            },
            timeout=60,
        )
        if response.status_code >= 400:
            raise ExtractionError(
                f"Mistral chat failed with status {response.status_code}: {response.text[:300]}",
                code="provider_request_failed",
            )
        payload = response.json()
        choices = payload.get("choices", [])
        if not choices:
            raise ExtractionError("Mistral chat returned no choices", code="empty_response")
        message = choices[0].get("message", {})
        content = message.get("content")
        if not isinstance(content, str) or not content.strip():
            raise ExtractionError("Mistral chat returned empty content", code="empty_response")
        return content


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


class MultiProviderVisionClient:
    def __init__(self, providers: list[tuple[str, VisionClient, str]]) -> None:
        self._providers = providers

    def extract_json(self, file_path: Path, model_name: str, prompt: str) -> str:
        errors: list[str] = []
        for provider_name, client, provider_model in self._providers:
            active_model = provider_model or model_name
            try:
                return client.extract_json(file_path, active_model, prompt)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{provider_name}: {exc}")
                continue
        raise ExtractionError(
            "All configured providers failed: " + "; ".join(errors),
            code="all_providers_failed",
        )


def _provider_model(provider: str, fallback_model_name: str) -> str:
    import os

    normalized = provider.strip().lower()
    if fallback_model_name and fallback_model_name != "auto":
        return fallback_model_name
    defaults = {
        "mistral": os.getenv("MISTRAL_MODEL", "pixtral-large-latest"),
        "openrouter": os.getenv("OPENROUTER_MODEL", "mistralai/pixtral-12b"),
        "groq": os.getenv("GROQ_MODEL", "meta-llama/llama-4-scout-17b-16e-instruct"),
        "openai": os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
        "gemini": os.getenv("GEMINI_MODEL", "gemini-1.5-pro"),
    }
    return defaults.get(normalized, "gpt-4o-mini")


def _client_for_provider(provider: str) -> VisionClient | None:
    import os

    normalized = provider.strip().lower()
    if normalized == "mistral":
        api_key = os.getenv("MISTRAL_API_KEY")
        if not api_key:
            return None
        return MistralVisionClient(api_key=api_key)
    if normalized == "openrouter":
        api_key = os.getenv("OPENROUTER_API_KEY")
        if not api_key:
            return None
        return OpenAICompatibleVisionClient(
            api_key=api_key,
            base_url="https://openrouter.ai/api/v1",
            provider_name="OpenRouter",
            default_headers={"HTTP-Referer": "https://github.com/atikulmunna/visual-invoice-processor"},
        )
    if normalized == "groq":
        api_key = os.getenv("GROQ_API_KEY")
        if not api_key:
            return None
        return OpenAICompatibleVisionClient(
            api_key=api_key,
            base_url="https://api.groq.com/openai/v1",
            provider_name="Groq",
        )
    if normalized == "openai":
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            return None
        return OpenAIVisionClient(api_key=api_key)
    if normalized == "gemini":
        api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
        if not api_key:
            return None
        return GeminiVisionClient(api_key=api_key)
    raise ExtractionError("Unsupported provider", code="unsupported_provider")


def _build_default_client(provider: str, model_name: str) -> tuple[VisionClient, str]:
    import os

    normalized = provider.strip().lower()
    if normalized in {"auto", "fallback", "multi"}:
        order = os.getenv("EXTRACTION_PROVIDER_ORDER", "mistral,openrouter,groq").split(",")
        providers: list[tuple[str, VisionClient, str]] = []
        for name in [x.strip().lower() for x in order if x.strip()]:
            client = _client_for_provider(name)
            if client is None:
                continue
            providers.append((name, client, _provider_model(name, "auto")))
        if not providers:
            raise ExtractionError(
                "No provider API key found for configured fallback chain",
                code="missing_api_key",
            )
        return MultiProviderVisionClient(providers), "auto"

    client = _client_for_provider(normalized)
    if client is None:
        raise ExtractionError(
            f"Missing API key for provider: {normalized}",
            code="missing_api_key",
        )
    return client, _provider_model(normalized, model_name)


def extract_document(
    file_path: str | Path,
    model_name: str = "auto",
    provider: str = "auto",
    client: VisionClient | None = None,
) -> dict[str, Any]:
    path = Path(file_path)
    if not path.exists():
        raise ExtractionError(f"File not found: {path}", code="file_not_found")

    if client is None:
        active_client, active_model = _build_default_client(provider, model_name)
    else:
        active_client, active_model = client, model_name

    first_text = active_client.extract_json(path, active_model, USER_EXTRACTION_PROMPT)
    try:
        return _parse_json_payload(first_text)
    except ExtractionError as exc:
        if exc.code not in {"invalid_json", "invalid_json_shape"}:
            raise

    corrective_text = active_client.extract_json(path, active_model, CORRECTIVE_PROMPT)
    return _parse_json_payload(corrective_text)
