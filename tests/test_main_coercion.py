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


def test_coerce_infers_usd_from_explicit_ocr_markers() -> None:
    raw = {
        "vendor": "Roboflow, Inc",
        "total": 12,
        "subtotal": 12,
        "currency": "",
        "_ocr_text": "Invoice Total USD 12.00\nAmount Due $12.00",
    }

    payload = _coerce_extraction_payload(raw)

    assert payload["currency"] == "USD"


def test_coerce_infers_bdt_from_local_currency_markers() -> None:
    raw = {
        "vendor": "Techland",
        "total": 68700,
        "subtotal": 68700,
        "currency": None,
        "_ocr_text": "Grand Total ৳ 68,700\nPaid by cash",
    }

    payload = _coerce_extraction_payload(raw)

    assert payload["currency"] == "BDT"


def test_coerce_preserves_explicit_three_letter_currency() -> None:
    raw = {
        "vendor": "SaaS Vendor",
        "total": 29,
        "subtotal": 29,
        "currency": "eur",
        "_ocr_text": "",
    }

    payload = _coerce_extraction_payload(raw)

    assert payload["currency"] == "EUR"
