from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class NormalizationRuleEngine:
    def __init__(self, rules: dict[str, Any]) -> None:
        self.rules = rules
        self.field_aliases: dict[str, list[str]] = rules.get("field_aliases", {})
        self.line_item_aliases: dict[str, list[str]] = rules.get("line_item_aliases", {})
        self.payment_method_map: dict[str, list[str]] = rules.get("payment_method_map", {})
        self.line_item_ignore_keywords: list[str] = [
            str(x).lower() for x in rules.get("line_item_ignore_keywords", [])
        ]
        self.amount_tolerance: float = float(rules.get("amount_tolerance", 0.01))
        self.default_currency: str = str(rules.get("default_currency", "BDT")).upper()
        self.default_document_type: str = str(rules.get("default_document_type", "invoice")).lower()
        self.default_confidence: float = float(rules.get("default_confidence", 0.8))

    @classmethod
    def from_path(cls, path: str | Path) -> "NormalizationRuleEngine":
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls(payload)

    def _pick(self, data: dict[str, Any], field_name: str, default: Any = None) -> Any:
        aliases = self.field_aliases.get(field_name, [field_name])
        for alias in aliases:
            if "." in alias:
                value = self._nested_get(data, alias)
                if value not in (None, ""):
                    return value
                continue
            if alias in data and data[alias] not in (None, ""):
                return data[alias]
        return default

    @staticmethod
    def _nested_get(data: dict[str, Any], path: str) -> Any:
        cur: Any = data
        for key in path.split("."):
            if not isinstance(cur, dict) or key not in cur:
                return None
            cur = cur[key]
        return cur

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        if value is None or value == "":
            return default
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip()
        text = re.sub(r"[^0-9,.\-]", "", text).replace(",", "")
        if not text:
            return default
        try:
            return float(text)
        except ValueError:
            return default

    @staticmethod
    def _normalize_date(value: Any) -> str | None:
        if not value:
            return None
        text = str(value).strip()
        formats = (
            "%Y-%m-%d",
            "%d-%m-%Y",
            "%d/%m/%Y",
            "%m/%d/%Y",
            "%B %d, %Y",
            "%b %d, %Y",
        )
        for fmt in formats:
            try:
                return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _extract_date_from_ocr(self, text: str) -> str | None:
        candidates = re.findall(r"\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}[/-]\d{1,2}[/-]\d{1,2})\b", text)
        for candidate in candidates:
            normalized = self._normalize_date(candidate)
            if normalized:
                return normalized
        return None

    def _normalize_payment_method(self, value: Any) -> str:
        text = str(value or "").lower()
        for canonical, keywords in self.payment_method_map.items():
            if any(keyword.lower() in text for keyword in keywords):
                return canonical
        return "unknown"

    def _normalize_vendor_name(self, raw: dict[str, Any]) -> str:
        value = self._pick(raw, "vendor_name", default="Unknown Vendor")
        if isinstance(value, dict):
            name = value.get("name")
            if isinstance(name, str) and name.strip():
                return name.strip()
            return "Unknown Vendor"
        return str(value).strip() or "Unknown Vendor"

    def _normalize_line_items(self, raw: Any, ocr_text: str) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        if isinstance(raw, list):
            for item in raw:
                if not isinstance(item, dict):
                    continue
                desc = str(self._pick_item(item, "description", default="item")).strip()
                qty = self._safe_float(self._pick_item(item, "quantity", default=1.0), 1.0)
                unit = self._safe_float(self._pick_item(item, "unit_price", default=0.0), 0.0)
                total = self._safe_float(self._pick_item(item, "line_total", default=qty * unit), qty * unit)
                items.append(
                    {
                        "description": desc,
                        "quantity": max(qty, 0.0001),
                        "unit_price": max(unit, 0.0),
                        "line_total": max(total, 0.0),
                        "category": self._pick_item(item, "category"),
                    }
                )

        if items and any(i["line_total"] > 0 for i in items):
            return items

        recovered = self._recover_line_items_from_ocr(ocr_text)
        return recovered if recovered else items

    def _pick_item(self, data: dict[str, Any], field_name: str, default: Any = None) -> Any:
        aliases = self.line_item_aliases.get(field_name, [field_name])
        for alias in aliases:
            if alias in data and data[alias] not in (None, ""):
                return data[alias]
        return default

    def _should_ignore_line_item(self, description: str) -> bool:
        desc = description.strip().lower()
        if not desc:
            return True
        return any(keyword in desc for keyword in self.line_item_ignore_keywords)

    def _reconcile_line_items(self, items: list[dict[str, Any]], target_total: float) -> list[dict[str, Any]]:
        if target_total <= 0 or len(items) <= 1:
            return items
        cents = [int(round(self._safe_float(i.get("line_total"), 0.0) * 100)) for i in items]
        target = int(round(target_total * 100))
        total = sum(cents)
        tol = int(round(self.amount_tolerance * 100))
        if abs(total - target) <= tol:
            return items
        if total < target:
            return items

        # Subset-sum DP: pick subset closest to target without exceeding it.
        reachable: dict[int, list[int]] = {0: []}
        for idx, value in enumerate(cents):
            if value <= 0:
                continue
            updates: dict[int, list[int]] = {}
            for current_sum, picked in reachable.items():
                new_sum = current_sum + value
                if new_sum > target + tol:
                    continue
                if new_sum not in reachable and new_sum not in updates:
                    updates[new_sum] = picked + [idx]
            reachable.update(updates)

        best_sum = max(reachable.keys())
        if best_sum == 0:
            return items
        chosen = set(reachable[best_sum])
        reconciled = [items[i] for i in sorted(chosen)]
        if reconciled:
            return reconciled
        return items

    def _recover_line_items_from_ocr(self, text: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for line in text.splitlines():
            compact = line.strip()
            if len(compact) < 8:
                continue
            m = re.match(
                r"^(?P<desc>.+?)\s+(?P<qty>\d+(?:\.\d+)?)\s+(?P<unit>\$?\d[\d,]*(?:\.\d+)?)\s+(?P<total>\$?\d[\d,]*(?:\.\d+)?)$",
                compact,
            )
            if not m:
                continue
            desc = m.group("desc").strip()
            qty = self._safe_float(m.group("qty"), 1.0)
            unit = self._safe_float(m.group("unit"), 0.0)
            total = self._safe_float(m.group("total"), qty * unit)
            if total <= 0:
                continue
            if self._should_ignore_line_item(desc):
                continue
            rows.append(
                {
                    "description": desc,
                    "quantity": max(qty, 0.0001),
                    "unit_price": max(unit, 0.0),
                    "line_total": max(total, 0.0),
                    "category": None,
                }
            )
        return rows

    def coerce_payload(self, raw: dict[str, Any]) -> dict[str, Any]:
        ocr_text = str(raw.get("_ocr_text", "") or "")
        total = self._safe_float(self._pick(raw, "total_amount", default=0.0), 0.0)
        subtotal = self._safe_float(self._pick(raw, "subtotal_amount", default=total), total)
        tax_amount = self._safe_float(self._pick(raw, "tax_amount", default=max(total - subtotal, 0.0)), 0.0)
        confidence = self._safe_float(self._pick(raw, "model_confidence", default=self.default_confidence), self.default_confidence)
        confidence = max(0.0, min(confidence, 1.0))

        invoice_date = self._normalize_date(self._pick(raw, "invoice_date"))
        if not invoice_date and ocr_text:
            invoice_date = self._extract_date_from_ocr(ocr_text)
        if not invoice_date:
            invoice_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        line_items = self._normalize_line_items(self._pick(raw, "line_items", default=[]), ocr_text)
        line_items = [item for item in line_items if not self._should_ignore_line_item(str(item.get("description", "")))]
        line_items = self._reconcile_line_items(line_items, subtotal if subtotal > 0 else total)

        document_type = str(self._pick(raw, "document_type", default=self.default_document_type)).lower()
        if document_type not in {"invoice", "receipt"}:
            document_type = "invoice"

        currency = str(self._pick(raw, "currency", default=self.default_currency)).upper()
        if len(currency) != 3:
            currency = self.default_currency

        return {
            "document_type": document_type,
            "vendor_name": self._normalize_vendor_name(raw),
            "vendor_tax_id": self._pick(raw, "vendor_tax_id"),
            "invoice_number": self._pick(raw, "invoice_number"),
            "invoice_date": invoice_date,
            "due_date": self._normalize_date(self._pick(raw, "due_date")),
            "currency": currency,
            "subtotal": max(subtotal, 0.0),
            "tax_amount": max(tax_amount, 0.0),
            "total_amount": max(total, 0.0),
            "payment_method": self._normalize_payment_method(self._pick(raw, "payment_method")),
            "line_items": line_items,
            "model_confidence": confidence,
            "validation_score": confidence,
        }
