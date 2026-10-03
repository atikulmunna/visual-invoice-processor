from __future__ import annotations

import base64
import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Protocol

import requests

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
# Cheap, reads images and scanned PDFs natively, and spends no hidden reasoning tokens.
DEFAULT_MODEL = "google/gemini-2.5-flash-lite"
# Caps every reply so a runaway response cannot run up the bill.
DEFAULT_MAX_TOKENS = 2000

logger = logging.getLogger(__name__)


class VisionClient(Protocol):
    def extract_json(self, file_path: Path, model_name: str, prompt: str) -> str:
        """Return raw model text output intended to be valid JSON."""


class ExtractionError(RuntimeError):
    def __init__(self, message: str, code: str = "extraction_failed") -> None:
        super().__init__(message)
        self.code = code


SYSTEM_PROMPT = "Return strict JSON only. No markdown or prose."

USER_EXTRACTION_PROMPT = (
    "Extract this invoice or receipt into exactly one JSON object with these keys: "
    "document_type, vendor_name, vendor_tax_id, invoice_number, invoice_date, due_date, "
    "currency, subtotal, tax_amount, shipping_amount, discount_amount, total_amount, "
    "payment_method, line_items, and "
    "model_confidence. Each line_items entry must contain description, quantity, "
    "unit_price, line_total, and category. Set document_type to receipt only for proof of a "
    "completed point-of-sale payment, such as a till slip, ride or delivery receipt, or payment "
    "confirmation; use invoice for everything else, including bills, tax invoices, and order "
    "summaries. Read labeled values exactly: prefer Grand Total "
    "for total_amount, do not confuse an order number with an invoice number, convert currency "
    "symbols or labels to a three-letter ISO code, and use null only when a value is genuinely "
    "absent. Amounts and confidence must be JSON numbers, not formatted strings."
)

CORRECTIVE_PROMPT = "Your previous reply was not valid JSON. Return only one valid JSON object with no extra text."


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
    # Models sometimes wrap JSON in a markdown fence or add a stray sentence. Recovering
    # the object locally is free; asking the model again costs a second call.
    text = re.sub(r"^\s*```(?:json)?\s*|\s*```\s*$", "", raw_text, flags=re.IGNORECASE)
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        start, end = text.find("{"), text.rfind("}")
        if not 0 <= start < end:
            raise ExtractionError("Model returned invalid JSON", code="invalid_json") from None
        try:
            payload = json.loads(text[start : end + 1])
        except json.JSONDecodeError as exc:
            raise ExtractionError("Model returned invalid JSON", code="invalid_json") from exc
    if not isinstance(payload, dict):
        raise ExtractionError("Model output must be a JSON object", code="invalid_json_shape")
    return payload


def _native_pdf_text(file_path: Path) -> str | None:
    if file_path.suffix.lower() != ".pdf":
        return None
    try:
        from pypdf import PdfReader

        chunks = [page.extract_text() or "" for page in PdfReader(str(file_path)).pages]
    except Exception:  # noqa: BLE001
        return None
    text = "\n".join(chunk.strip() for chunk in chunks if chunk.strip()).strip()
    return text or None


def _enrich_extraction_payload(payload: dict[str, Any], *, file_path: Path, provider_name: str) -> dict[str, Any]:
    # The PDF text layer is free to read locally and lets normalization recover values
    # the model left out.
    native_text = _native_pdf_text(file_path)
    if native_text:
        payload["_ocr_text"] = native_text
    payload["_provider"] = provider_name
    return payload


class OpenRouterClient:
    provider_name = "openrouter"

    def __init__(self, api_key: str, *, max_tokens: int = DEFAULT_MAX_TOKENS, timeout: float = 90) -> None:
        self._api_key = api_key.strip()
        self._max_tokens = max_tokens
        self._timeout = timeout

    def _request_body(self, file_path: Path, model_name: str, prompt: str) -> dict[str, Any]:
        mime = _mime_for_path(file_path)
        data_url = f"data:{mime};base64,{base64.b64encode(file_path.read_bytes()).decode('ascii')}"
        body: dict[str, Any] = {
            "model": model_name,
            "temperature": 0,
            "max_tokens": self._max_tokens,
            "response_format": {"type": "json_object"},
            "usage": {"include": True},
        }
        if mime == "application/pdf":
            document = {"type": "file", "file": {"filename": file_path.name, "file_data": data_url}}
            # Let the model read the PDF itself; never fall back to OpenRouter's paid OCR engine.
            body["plugins"] = [{"id": "file-parser", "pdf": {"engine": "native"}}]
        else:
            document = {"type": "image_url", "image_url": {"url": data_url}}
        body["messages"] = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": [{"type": "text", "text": prompt}, document]},
        ]
        return body

    def extract_json(self, file_path: Path, model_name: str, prompt: str) -> str:
        try:
            response = requests.post(
                OPENROUTER_URL,
                headers={
                    "Authorization": f"Bearer {self._api_key}",
                    "HTTP-Referer": "https://github.com/atikulmunna/visual-invoice-processor",
                    "X-Title": "Ledgerly",
                },
                json=self._request_body(file_path, model_name, prompt),
                timeout=self._timeout,
            )
        except requests.RequestException as exc:
            raise ExtractionError(f"OpenRouter request failed: {exc}", code="provider_request_failed") from exc
        if response.status_code >= 400:
            raise ExtractionError(
                f"OpenRouter failed with status {response.status_code}: {response.text[:300]}",
                code="provider_request_failed",
            )
        payload = response.json()
        usage = payload.get("usage") or {}
        logger.info(
            "OpenRouter usage model=%s prompt_tokens=%s completion_tokens=%s cost=%s",
            payload.get("model", model_name),
            usage.get("prompt_tokens"),
            usage.get("completion_tokens"),
            usage.get("cost"),
        )
        choices = payload.get("choices") or []
        content = choices[0].get("message", {}).get("content") if choices else None
        if not isinstance(content, str) or not content.strip():
            raise ExtractionError("OpenRouter returned empty content", code="empty_response")
        return content


def _resolve_model(model_name: str) -> str:
    if model_name and model_name != "auto":
        return model_name
    return (os.getenv("OPENROUTER_MODEL") or DEFAULT_MODEL).strip()


def build_client() -> OpenRouterClient:
    api_key = (os.getenv("OPENROUTER_API_KEY") or "").strip()
    if not api_key:
        raise ExtractionError("OPENROUTER_API_KEY is not configured", code="missing_api_key")
    max_tokens = int(os.getenv("OPENROUTER_MAX_TOKENS") or DEFAULT_MAX_TOKENS)
    return OpenRouterClient(api_key, max_tokens=max_tokens)


def extract_document(
    file_path: str | Path,
    model_name: str = "auto",
    provider: str = "openrouter",
    client: VisionClient | None = None,
) -> dict[str, Any]:
    path = Path(file_path)
    if not path.exists():
        raise ExtractionError(f"File not found: {path}", code="file_not_found")
    if provider.strip().lower() not in {"openrouter", "auto"}:
        raise ExtractionError(
            f"Unsupported provider: {provider}. OpenRouter is the only extraction provider.",
            code="unsupported_provider",
        )

    active_client = client or build_client()
    active_model = _resolve_model(model_name)
    provider_name = str(getattr(active_client, "provider_name", "openrouter"))

    text = active_client.extract_json(path, active_model, USER_EXTRACTION_PROMPT)
    try:
        payload = _parse_json_payload(text)
    except ExtractionError:
        # One retry, with the full instructions, only when local repair failed.
        text = active_client.extract_json(path, active_model, f"{USER_EXTRACTION_PROMPT} {CORRECTIVE_PROMPT}")
        payload = _parse_json_payload(text)
    return _enrich_extraction_payload(payload, file_path=path, provider_name=provider_name)
