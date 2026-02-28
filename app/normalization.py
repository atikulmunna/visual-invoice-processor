from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Protocol


class CategoryModelClient(Protocol):
    def suggest_category(self, text: str) -> tuple[str, float]:
        """Return (category, confidence)."""


DEFAULT_VENDOR_RULES = {
    "walmart": "Walmart",
    "wal-mart": "Walmart",
    "wm supercenter": "Walmart",
    "amazon": "Amazon",
    "amzn": "Amazon",
    "starbucks": "Starbucks",
}

DEFAULT_CATEGORY_KEYWORDS = {
    "office_supplies": {"paper", "printer", "ink", "stationery"},
    "travel": {"uber", "lyft", "taxi", "flight", "hotel"},
    "food_beverage": {"coffee", "restaurant", "cafe", "meal", "lunch"},
    "software": {"subscription", "license", "cloud", "saas"},
}


def normalize_vendor_name(vendor_name: str, rules: dict[str, str] | None = None) -> str:
    active_rules = rules or DEFAULT_VENDOR_RULES
    normalized = re.sub(r"[^a-z0-9\s-]", "", vendor_name.lower()).strip()
    for pattern, canonical in active_rules.items():
        if pattern in normalized:
            return canonical
    return vendor_name.strip()


@dataclass(frozen=True)
class CategorySuggestion:
    category: str
    confidence: float
    source: str


def suggest_category(
    text: str,
    *,
    model_client: CategoryModelClient | None = None,
) -> CategorySuggestion:
    lowered = text.lower()

    for category, keywords in DEFAULT_CATEGORY_KEYWORDS.items():
        if any(keyword in lowered for keyword in keywords):
            return CategorySuggestion(category=category, confidence=0.85, source="rules")

    if model_client is not None:
        category, confidence = model_client.suggest_category(text)
        return CategorySuggestion(category=category, confidence=float(confidence), source="model")

    return CategorySuggestion(category="uncategorized", confidence=0.2, source="fallback")

