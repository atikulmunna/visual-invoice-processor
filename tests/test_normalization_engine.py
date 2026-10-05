from __future__ import annotations

from app.normalization_engine import NormalizationRuleEngine


def _rules() -> dict:
    return {
        "base_currency": "BDT",
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


def test_engine_reconciles_overcounted_line_items_to_subtotal() -> None:
    rules = _rules()
    rules["line_item_ignore_keywords"] = []
    engine = NormalizationRuleEngine(rules)
    raw = {
        "subtotal": "$12.00",
        "total": "$12.00",
        "items": [
            {"description": "Usage Summary", "qty": 3, "amount": "$12.00"},
            {"description": "First 15", "qty": 0, "amount": "$0.00"},
            {"description": "16 and above", "qty": 3, "amount": "$12.00"}
        ]
    }
    payload = engine.coerce_payload(raw)
    total = sum(item["line_total"] for item in payload["line_items"])
    assert abs(total - 12.0) < 0.01


def test_engine_recovers_labeled_invoice_fields_from_pdf_text() -> None:
    engine = NormalizationRuleEngine(_rules())
    raw = {
        "invoice_number": None,
        "subtotal": 0,
        "tax_amount": 0,
        "total_amount": 0,
        "_ocr_text": """
Order # 1001872388
03-03-2025
Invoice # 1001866868
Black Printed Ramie-Cotton Fatua 0080000104060 Tk 1,246.51 1 Tk 1,246.51
White Printed Cotton Fatua 0080000105624 Tk 927.27 1 Tk 927.27
Subtotal: Tk 4,722.62
Shipping & Handling Tk 80.00
VAT Tk 472.26
Grand Total Tk 5,274.88
Payment Method
Cash on delivery
""",
    }

    payload = engine.coerce_payload(raw)

    assert payload["invoice_number"] == "1001866868"
    assert payload["invoice_date"] == "2025-03-03"
    assert payload["currency"] == "BDT"
    assert payload["subtotal"] == 4722.62
    assert payload["tax_amount"] == 472.26
    assert payload["shipping_amount"] == 80.0
    assert payload["discount_amount"] == 0.0
    assert payload["total_amount"] == 5274.88
    assert payload["payment_method"] == "cash"
    assert payload["model_confidence"] == 0.6
    assert len(payload["line_items"]) == 2
    assert payload["line_items"][0]["line_total"] == 1246.51


def _minimal(**overrides: object) -> dict:
    raw: dict = {"vendor": "Acme", "total": 100, "subtotal": 100, "invoice_number": "INV-1"}
    raw.update(overrides)
    return raw


def test_missing_vendor_and_date_stay_empty_instead_of_being_invented() -> None:
    engine = NormalizationRuleEngine(_rules())

    payload = engine.coerce_payload({"total": 100, "currency": "USD"})

    assert payload["vendor_name"] is None
    assert payload["invoice_date"] is None


def test_blank_or_nameless_vendor_values_are_empty() -> None:
    engine = NormalizationRuleEngine(_rules())

    assert engine.coerce_payload(_minimal(vendor="   "))["vendor_name"] is None
    assert engine.coerce_payload(_minimal(vendor={"address": "Dhaka"}))["vendor_name"] is None


def test_date_is_recovered_from_document_text_when_model_omits_it() -> None:
    engine = NormalizationRuleEngine(_rules())

    payload = engine.coerce_payload(_minimal(currency="BDT", _ocr_text="Order Date 01/03/2026"))

    assert payload["invoice_date"] == "2026-03-01"


def test_common_date_formats_are_understood() -> None:
    engine = NormalizationRuleEngine(_rules())
    cases = {
        "2026-03-04T10:22:00Z": "2026-03-04",
        "04.03.2026": "2026-03-04",
        "4 Mar 2026": "2026-03-04",
        "04-Mar-2026": "2026-03-04",
        "March 4 2026": "2026-03-04",
        "04/03/26": "2026-03-04",
        "02/27/2026": "2026-02-27",
    }

    for raw_date, expected in cases.items():
        assert engine.coerce_payload(_minimal(invoice_date=raw_date))["invoice_date"] == expected, raw_date


def test_month_first_dates_are_read_only_when_day_first_is_impossible() -> None:
    engine = NormalizationRuleEngine(_rules())
    cases = {
        "09-19-2026": "2026-09-19",  # HiFi Heaven prints order dates month first
        "09-19-26": "2026-09-19",
        "09/19/26": "2026-09-19",
        "04-05-2026": "2026-05-04",  # ambiguous, so day first as before
        "04/05/26": "2026-05-04",
        "13-13-2026": None,
    }

    for raw_date, expected in cases.items():
        assert engine.coerce_payload(_minimal(invoice_date=raw_date))["invoice_date"] == expected, raw_date


def test_explicit_currency_codes_and_symbols_are_normalized() -> None:
    engine = NormalizationRuleEngine(_rules())

    for raw_currency, expected in {"eur": "EUR", "Tk": "BDT", "৳": "BDT", "$": "USD", "Taka": "BDT"}.items():
        payload = engine.coerce_payload(_minimal(currency=raw_currency))
        assert payload["currency"] == expected, raw_currency
        assert payload["currency_assumed"] is False


def test_currency_is_inferred_from_the_dominant_marker_in_document_text() -> None:
    engine = NormalizationRuleEngine(_rules())

    usd = engine.coerce_payload(_minimal(currency="", _ocr_text="Invoice Total USD 12.00\nAmount Due $12.00"))
    bdt = engine.coerce_payload(_minimal(currency=None, _ocr_text="Grand Total ৳ 68,700\nVAT Tk 500"))

    assert (usd["currency"], usd["currency_assumed"]) == ("USD", False)
    assert (bdt["currency"], bdt["currency_assumed"]) == ("BDT", False)


def test_missing_currency_falls_back_to_the_organization_base_currency_and_is_flagged() -> None:
    engine = NormalizationRuleEngine(_rules())

    org_default = engine.coerce_payload(_minimal(), base_currency="usd")
    rules_default = engine.coerce_payload(_minimal())

    assert (org_default["currency"], org_default["currency_assumed"]) == ("USD", True)
    assert (rules_default["currency"], rules_default["currency_assumed"]) == ("BDT", True)


def test_tied_or_ambiguous_markers_fall_back_to_base_currency() -> None:
    engine = NormalizationRuleEngine(_rules())

    tied = engine.coerce_payload(_minimal(_ocr_text="Paid $10 or Tk 1,200"), base_currency="GBP")
    stray_symbol = engine.coerce_payload(_minimal(currency="Rs", _ocr_text="Call $ hotline"), base_currency="GBP")

    assert (tied["currency"], tied["currency_assumed"]) == ("GBP", True)
    assert (stray_symbol["currency"], stray_symbol["currency_assumed"]) == ("GBP", True)


def test_without_any_base_currency_the_currency_stays_empty() -> None:
    rules = _rules()
    rules.pop("base_currency")
    engine = NormalizationRuleEngine(rules)

    payload = engine.coerce_payload(_minimal())

    assert payload["currency"] is None
    assert payload["currency_assumed"] is False
