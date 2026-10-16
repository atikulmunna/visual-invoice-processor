"""Checks each extracted value against the document's own text, and gives every field a confidence.

A PDF with a text layer lets us confirm that the vendor, invoice number, date, and total the model
returned are really printed on the document, in whatever format the document uses. Photos and
scans have no text, so their values are unverified and routing falls back to the model's score.
Dates are also checked for sense, which needs no text: not in the future, a due date not before
the invoice date.
"""

from __future__ import annotations

import re
import unicodedata
from datetime import date, timedelta
from typing import Any

from app.vendor_store import normalize_vendor_name

# A value from one of these that the document does not show sends the document to review.
KEY_FIELDS = ("vendor_name", "invoice_number", "invoice_date", "total_amount")
CONFIDENCE = {"found": 0.95, "unverified": None, "not_found": 0.3, "implausible": 0.1}
EARLIEST_DATE = date(2000, 1, 1)
VENDOR_WORDS_NEEDED = 0.6

MONTHS = [
    ("january", "jan"), ("february", "feb"), ("march", "mar"), ("april", "apr"), ("may", "may"), ("june", "jun"),
    ("july", "jul"), ("august", "aug"), ("september", "sep", "sept"), ("october", "oct"), ("november", "nov"),
    ("december", "dec"),
]
CURRENCY_MARKS = {
    "BDT": ("bdt", "tk", "taka", "৳"), "USD": ("usd", "us$", "$"), "EUR": ("eur", "€"), "GBP": ("gbp", "£"),
    "INR": ("inr", "rs", "₹"),
}


class DocumentText:
    """One document's text, prepared once for every kind of lookup."""

    def __init__(self, raw: str) -> None:
        text = unicodedata.normalize("NFKC", raw).casefold()
        # Words: letters, digits, and combining marks, single-spaced, for names and dates.
        words = "".join(ch if unicodedata.category(ch)[0] in "LNM" else " " for ch in text)
        self.words = " " + re.sub(r"(\d)(st|nd|rd|th)\b", r"\1", " ".join(words.split())) + " "
        # Dates keep their separators so 06-03-2025 and 06/03/2025 stay distinguishable from prose.
        self.dated = " ".join(re.sub(r"(\d)(st|nd|rd|th)\b", r"\1", text.replace(",", " ")).split())
        self.compact = re.sub(r"[^0-9a-z]", "", text)
        self.amounts = _amounts_in(text)
        self.raw = text


def _amounts_in(text: str) -> set[float]:
    """Every amount the text could mean, reading 1,954 and 1.954 both ways when unclear."""
    values: set[float] = set()
    for token in re.findall(r"\d[\d.,']*\d|\d", text):
        token = token.replace("'", "")
        candidates = {token.replace(",", ""), token.replace(".", "").replace(",", ".")}
        if "," in token and "." in token:
            decimal = "." if token.rfind(".") > token.rfind(",") else ","
            other = "," if decimal == "." else "."
            candidates = {token.replace(other, "").replace(decimal, ".")}
        for candidate in candidates:
            try:
                values.add(round(float(candidate), 2))
            except ValueError:
                continue
    return values


def _date_renderings(day: date) -> set[str]:
    """How a date might be printed: numeric in either order with any separator, or with the month named."""
    shapes: set[str] = set()
    days = {f"{day.day}", f"{day.day:02d}"}
    months = {f"{day.month}", f"{day.month:02d}"}
    years = {f"{day.year}", f"{day.year % 100:02d}"}
    for separator in ("/", "-", ".", " "):
        for d in days:
            for m in months:
                for y in years:
                    shapes.add(separator.join((d, m, y)))
                    shapes.add(separator.join((m, d, y)))
                shapes.add(separator.join((f"{day.year}", m, d)))
    for name in MONTHS[day.month - 1]:
        for d in days:
            for y in (f"{day.year}",):
                shapes.update({f"{d} {name} {y}", f"{name} {d} {y}", f"{d}-{name}-{y}", f"{d}.{name}.{y}"})
    return shapes


def _found_text(needle: str, haystack: str) -> bool:
    return re.search(rf"(?<![0-9a-z]){re.escape(needle)}(?![0-9a-z])", haystack) is not None


def _check_vendor(value: str, text: DocumentText) -> bool:
    words = (normalize_vendor_name(value) or "").split()
    if not words:
        return False
    present = sum(1 for word in words if f" {word} " in text.words or (len(word) >= 4 and word in text.compact))
    return present / len(words) >= VENDOR_WORDS_NEEDED


def _check_identifier(value: str, text: DocumentText) -> bool:
    compact = re.sub(r"[^0-9a-z]", "", unicodedata.normalize("NFKC", value).casefold())
    if not compact:
        return False
    # Short numbers occur everywhere, so they must stand alone as a word.
    return f" {compact} " in text.words if len(compact) < 4 else compact in text.compact


def _check_date(value: date, text: DocumentText) -> bool:
    return any(_found_text(shape, text.dated) for shape in _date_renderings(value))


def _check_amount(value: float, text: DocumentText) -> bool:
    return any(abs(value - candidate) <= 0.01 for candidate in text.amounts)


def _check_currency(value: str, text: DocumentText) -> bool:
    marks = CURRENCY_MARKS.get(value.upper(), (value.lower(),))
    return any((mark in text.raw) if not mark.isalpha() else _found_text(mark, text.words) for mark in marks)


def _as_date(value: Any) -> date | None:
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def _plausible(field: str, value: date, record: dict[str, Any], today: date) -> bool:
    if value > today + timedelta(days=1) and field == "invoice_date":
        return False
    if value < EARLIEST_DATE:
        return False
    if field == "due_date":
        issued = _as_date(record.get("invoice_date"))
        return issued is None or value >= issued
    return True


def assess_fields(record: dict[str, Any], document_text: str | None, today: date) -> dict[str, Any]:
    """Status and confidence for each value the document should show.

    Returns {"text_available": bool, "fields": {field: {"status", "confidence"}}}; a confidence of
    None means unverified. Empty values and zero amounts are left out: missing fields have their
    own review reasons, and documents rarely print a zero.
    """
    text = DocumentText(document_text) if document_text and document_text.strip() else None
    checks = {
        "vendor_name": lambda v: _check_vendor(str(v), text),
        "invoice_number": lambda v: _check_identifier(str(v), text),
        "vendor_tax_id": lambda v: _check_identifier(str(v), text),
        "currency": lambda v: _check_currency(str(v), text),
        "invoice_date": lambda v: _check_date(v, text),
        "due_date": lambda v: _check_date(v, text),
        "total_amount": lambda v: _check_amount(float(v), text),
        "subtotal": lambda v: _check_amount(float(v), text),
        "tax_amount": lambda v: _check_amount(float(v), text),
    }
    fields: dict[str, dict[str, Any]] = {}
    for field, check in checks.items():
        value = record.get(field)
        if value in (None, "") or (field in ("total_amount", "subtotal", "tax_amount") and not float(value or 0)):
            continue
        if field == "currency" and record.get("currency_assumed"):
            continue  # not read from the document, already marked as assumed
        if field in ("invoice_date", "due_date"):
            value = _as_date(value)
            if value is None:
                continue
            if not _plausible(field, value, record, today):
                fields[field] = {"status": "implausible", "confidence": CONFIDENCE["implausible"]}
                continue
        status = "unverified" if text is None else ("found" if check(value) else "not_found")
        fields[field] = {"status": status, "confidence": CONFIDENCE[status]}
    return {"text_available": text is not None, "fields": fields}


def evidence_reasons(evidence: dict[str, Any]) -> list[str]:
    """Review reasons the evidence raises: a key value not on the document, or a date that makes no sense."""
    fields = evidence["fields"]
    reasons = []
    if any(fields.get(field, {}).get("status") == "not_found" for field in KEY_FIELDS):
        reasons.append("value_not_on_document")
    if any(result["status"] == "implausible" for result in fields.values()):
        reasons.append("implausible_date")
    return reasons
