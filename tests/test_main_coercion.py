from __future__ import annotations

from app.main import _coerce_extraction_payload


def test_coerce_uses_ocr_date_when_missing() -> None:
    raw = {
        "vendor": "RYANS",
        "total": 8300,
        "subtotal": 8300,
        "currency": "bdt",
        "_ocr_text": "Order Date 01/03/2026",
    }
    payload = _coerce_extraction_payload(raw)
    assert payload["invoice_date"] == "2026-03-01"


def test_coerce_recovers_line_items_from_ocr_when_model_items_zero() -> None:
    raw = {
        "vendor": "RYANS",
        "total": 8300,
        "subtotal": 8300,
        "currency": "BDT",
        "line_items": [
            {
                "description": "SSD",
                "quantity": 1,
                "unit_price": 0,
                "line_total": 0,
            }
        ],
        "_ocr_text": "OSCOO ON901 256GB M.2 SSD 1 4300 4300\nUGREEN CM578 Enclosure 1 4000 4000",
    }
    payload = _coerce_extraction_payload(raw)
    assert len(payload["line_items"]) >= 2
    assert any(item["line_total"] > 0 for item in payload["line_items"])

