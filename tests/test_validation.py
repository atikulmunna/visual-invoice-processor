from __future__ import annotations

from copy import deepcopy

import pytest
from pydantic import ValidationError

from app.validation import validate_invoice_payload


def _valid_payload() -> dict:
    return {
        "document_type": "invoice",
        "vendor_name": "Acme Supplies",
        "vendor_tax_id": "TAX-123",
        "invoice_number": "INV-001",
        "invoice_date": "2026-02-27",
        "due_date": "2026-03-10",
        "currency": "USD",
        "subtotal": 100.0,
        "tax_amount": 10.0,
        "total_amount": 110.0,
        "payment_method": "card",
        "line_items": [
            {
                "description": "Paper",
                "quantity": 2,
                "unit_price": 50.0,
                "line_total": 100.0,
                "category": "office",
            }
        ],
        "model_confidence": 0.92,
        "validation_score": 0.95,
    }


def test_validate_invoice_payload_accepts_valid_sample() -> None:
    payload = _valid_payload()
    record = validate_invoice_payload(payload)
    assert record.vendor_name == "Acme Supplies"
    assert record.currency == "USD"
    assert record.total_amount == 110.0


@pytest.mark.parametrize(
    ("field", "value", "error_fragment"),
    [
        ("invoice_date", "02/27/2026", "invoice_date"),
        ("currency", "usd", "currency"),
        ("total_amount", -1.0, "total_amount"),
    ],
)
def test_validate_invoice_payload_rejects_invalid_samples(
    field: str, value: object, error_fragment: str
) -> None:
    payload = _valid_payload()
    payload[field] = value

    with pytest.raises(ValidationError) as exc_info:
        validate_invoice_payload(payload)

    errors = exc_info.value.errors()
    assert any(error_fragment in ".".join(map(str, e["loc"])) for e in errors)

