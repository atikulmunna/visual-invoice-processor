from __future__ import annotations

from typing import Any

from schemas.invoice_schema import InvoiceRecord


def validate_invoice_payload(payload: dict[str, Any]) -> InvoiceRecord:
    return InvoiceRecord.model_validate(payload)

