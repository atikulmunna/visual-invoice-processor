from __future__ import annotations

from app.normalization_engine import NormalizationRuleEngine


def _rules() -> dict:
    return {
        "default_currency": "BDT",
        "default_document_type": "invoice",
        "default_confidence": 0.8,
        "field_aliases": {
            "vendor_name": ["vendor_name", "vendor"],
            "invoice_date": ["invoice_date", "date_paid"],
            "currency": ["currency"],
            "subtotal_amount": ["subtotal"],
            "tax_amount": ["tax_amount", "tax"],
            "total_amount": ["total_amount", "total", "amount_paid"],
            "payment_method": ["payment_method"],
            "line_items": ["line_items", "items"],
            "model_confidence": ["model_confidence", "confidence"],
            "invoice_number": ["invoice_number", "receipt_number"],
            "vendor_tax_id": ["vendor_tax_id", "tax_id"],
            "due_date": ["due_date"],
        },
        "line_item_aliases": {
            "description": ["description", "name"],
            "quantity": ["quantity", "qty"],
            "unit_price": ["unit_price", "price"],
            "line_total": ["line_total", "amount", "total"],
            "category": ["category"],
        },
        "payment_method_map": {
            "card": ["card", "mastercard"],
            "cash": ["cash", "cod"],
            "bank": ["bank", "transfer"],
        },
    }


def test_engine_normalizes_vendor_object_and_currency_amount_strings() -> None:
    engine = NormalizationRuleEngine(_rules())
    raw = {
        "vendor": {"name": "Roboflow, Inc"},
        "amount_paid": "$12.00",
        "subtotal": "$12.00",
        "currency": "usd",
        "payment_method": "Mastercard - 1234",
        "date_paid": "February 13, 2026",
    }
    payload = engine.coerce_payload(raw)
    assert payload["vendor_name"] == "Roboflow, Inc"
    assert payload["total_amount"] == 12.0
    assert payload["currency"] == "USD"
    assert payload["payment_method"] == "card"
    assert payload["invoice_date"] == "2026-02-13"


def test_engine_recovers_line_items_from_ocr_text_when_item_amounts_missing() -> None:
    engine = NormalizationRuleEngine(_rules())
    raw = {
        "total": "8300",
        "subtotal": "8300",
        "line_items": [{"description": "SSD", "quantity": 1, "unit_price": 0, "line_total": 0}],
        "_ocr_text": "OSCOO ON901 256GB M.2 SSD 1 4300 4300\nUGREEN CM578 Enclosure 1 4000 4000",
    }
    payload = engine.coerce_payload(raw)
    assert len(payload["line_items"]) >= 2
    assert any(item["line_total"] > 0 for item in payload["line_items"])

