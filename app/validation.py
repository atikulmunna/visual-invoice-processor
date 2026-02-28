from __future__ import annotations

from typing import Any

from schemas.invoice_schema import InvoiceRecord


def validate_invoice_payload(payload: dict[str, Any]) -> InvoiceRecord:
    return InvoiceRecord.model_validate(payload)


def evaluate_business_rules(
    record: InvoiceRecord,
    *,
    amount_tolerance: float = 0.01,
) -> list[dict[str, Any]]:
    violations: list[dict[str, Any]] = []

    computed_total = round(record.subtotal + record.tax_amount, 2)
    declared_total = round(record.total_amount, 2)
    if abs(computed_total - declared_total) > amount_tolerance:
        violations.append(
            {
                "code": "amount_mismatch",
                "severity": "error",
                "message": "subtotal + tax does not match total_amount",
                "expected_total": computed_total,
                "actual_total": declared_total,
            }
        )

    if record.line_items:
        line_sum = round(sum(item.line_total for item in record.line_items), 2)
        subtotal = round(record.subtotal, 2)
        if line_sum <= amount_tolerance and subtotal > amount_tolerance:
            violations.append(
                {
                    "code": "line_items_incomplete",
                    "severity": "warning",
                    "message": "line items present but amounts are missing or zero",
                    "expected_subtotal": line_sum,
                    "actual_subtotal": subtotal,
                }
            )
        elif abs(line_sum - subtotal) > amount_tolerance:
            violations.append(
                {
                    "code": "line_item_sum_mismatch",
                    "severity": "error",
                    "message": "sum(line_items.line_total) does not match subtotal",
                    "expected_subtotal": line_sum,
                    "actual_subtotal": subtotal,
                }
            )

    if record.document_type == "invoice" and not (
        (record.invoice_number and record.invoice_number.strip())
        or (record.vendor_tax_id and record.vendor_tax_id.strip())
    ):
        violations.append(
            {
                "code": "missing_identifier",
                "severity": "warning",
                "message": "invoice should include invoice_number or vendor_tax_id",
            }
        )

    return violations


def validate_and_score(
    payload: dict[str, Any],
    *,
    amount_tolerance: float = 0.01,
) -> dict[str, Any]:
    record = validate_invoice_payload(payload)
    violations = evaluate_business_rules(record, amount_tolerance=amount_tolerance)
    total_rules = 3
    score = max(0.0, 1.0 - (len(violations) / total_rules))
    is_valid = not any(v["severity"] == "error" for v in violations)
    return {
        "record": record,
        "violations": violations,
        "validation_score": round(score, 4),
        "is_valid": is_valid,
    }
