from __future__ import annotations

from app.normalization import normalize_vendor_name, suggest_category


class _FakeCategoryModel:
    def suggest_category(self, text: str) -> tuple[str, float]:
        _ = text
        return ("consulting", 0.67)


def test_vendor_normalization_maps_variants_to_canonical() -> None:
    assert normalize_vendor_name("WAL-MART SUPERCENTER #455") == "Walmart"
    assert normalize_vendor_name("AMZN Marketplace") == "Amazon"
    assert normalize_vendor_name("Starbucks Coffee") == "Starbucks"


def test_category_suggestion_uses_rules_with_confidence() -> None:
    suggestion = suggest_category("Printer ink and paper purchase")
    assert suggestion.category == "office_supplies"
    assert suggestion.confidence == 0.85
    assert suggestion.source == "rules"


def test_category_suggestion_uses_model_when_rules_do_not_match() -> None:
    suggestion = suggest_category("Domain-specific consulting fee", model_client=_FakeCategoryModel())
    assert suggestion.category == "consulting"
    assert suggestion.confidence == 0.67
    assert suggestion.source == "model"

