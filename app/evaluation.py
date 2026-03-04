from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.config import load_dotenv
from app.extraction_service import ExtractionError, extract_document
from app.normalization_engine import NormalizationRuleEngine


NUMERIC_FIELDS = {"subtotal", "tax_amount", "total_amount", "model_confidence", "validation_score"}


@dataclass
class FieldResult:
    field: str
    expected: Any
    actual: Any
    matched: bool
    reason: str | None = None


def _normalize_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def _compare_numeric(expected: Any, actual: Any, tolerance: float) -> bool:
    try:
        exp = float(expected)
        act = float(actual)
    except (TypeError, ValueError):
        return False
    return abs(exp - act) <= tolerance


def _compare_scalar(field: str, expected: Any, actual: Any, tolerance: float) -> tuple[bool, str | None]:
    if field in NUMERIC_FIELDS:
        matched = _compare_numeric(expected, actual, tolerance)
        return matched, None if matched else f"numeric mismatch (tol={tolerance})"
    if isinstance(expected, str):
        matched = _normalize_text(expected) == _normalize_text(actual)
        return matched, None if matched else "text mismatch"
    matched = expected == actual
    return matched, None if matched else "value mismatch"


def _score_line_items(expected_items: list[dict[str, Any]], actual_items: list[dict[str, Any]], tolerance: float) -> list[FieldResult]:
    results: list[FieldResult] = []
    expected_count = len(expected_items)
    actual_count = len(actual_items)
    count_match = expected_count == actual_count
    results.append(
        FieldResult(
            field="line_items.count",
            expected=expected_count,
            actual=actual_count,
            matched=count_match,
            reason=None if count_match else "count mismatch",
        )
    )

    for index, expected_item in enumerate(expected_items):
        if index >= actual_count:
            results.append(
                FieldResult(
                    field=f"line_items[{index}]",
                    expected=expected_item,
                    actual=None,
                    matched=False,
                    reason="missing line item",
                )
            )
            continue
        actual_item = actual_items[index]
        for key, exp_val in expected_item.items():
            act_val = actual_item.get(key)
            matched, reason = _compare_scalar(f"line_items.{key}", exp_val, act_val, tolerance)
            results.append(
                FieldResult(
                    field=f"line_items[{index}].{key}",
                    expected=exp_val,
                    actual=act_val,
                    matched=matched,
                    reason=reason,
                )
            )
    return results


def evaluate_case(
    case: dict[str, Any],
    engine: NormalizationRuleEngine,
    provider: str,
    model_name: str,
    amount_tolerance: float,
) -> dict[str, Any]:
    source_path = Path(str(case.get("file_path", "")).strip())
    expected = case.get("expected", {})
    if not source_path.exists():
        return {
            "file_path": str(source_path),
            "status": "error",
            "error": "file_not_found",
            "details": f"Missing file: {source_path}",
        }
    if not isinstance(expected, dict) or not expected:
        return {
            "file_path": str(source_path),
            "status": "error",
            "error": "invalid_expected_payload",
            "details": "Each case requires a non-empty 'expected' object.",
        }

    try:
        extracted = extract_document(file_path=source_path, provider=provider, model_name=model_name)
    except ExtractionError as exc:
        return {
            "file_path": str(source_path),
            "status": "error",
            "error": exc.code,
            "details": str(exc),
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "file_path": str(source_path),
            "status": "error",
            "error": "unhandled_exception",
            "details": str(exc),
        }

    normalized = engine.coerce_payload(extracted)
    field_results: list[FieldResult] = []
    for field, expected_value in expected.items():
        if field == "line_items":
            if not isinstance(expected_value, list):
                field_results.append(
                    FieldResult(
                        field="line_items",
                        expected=expected_value,
                        actual=normalized.get("line_items"),
                        matched=False,
                        reason="'line_items' must be a list",
                    )
                )
                continue
            actual_items = normalized.get("line_items") or []
            if not isinstance(actual_items, list):
                actual_items = []
            field_results.extend(_score_line_items(expected_value, actual_items, amount_tolerance))
            continue

        actual_value = normalized.get(field)
        matched, reason = _compare_scalar(field, expected_value, actual_value, amount_tolerance)
        field_results.append(
            FieldResult(
                field=field,
                expected=expected_value,
                actual=actual_value,
                matched=matched,
                reason=reason,
            )
        )

    total_fields = len(field_results)
    matched_fields = sum(1 for result in field_results if result.matched)
    score = (matched_fields / total_fields) if total_fields else 0.0

    return {
        "file_path": str(source_path),
        "status": "ok",
        "used_provider": extracted.get("_provider", "unknown"),
        "score": round(score, 4),
        "matched_fields": matched_fields,
        "total_fields": total_fields,
        "field_results": [result.__dict__ for result in field_results],
        "normalized_output": normalized,
    }


def run_evaluation(
    dataset_path: Path,
    rules_path: Path,
    provider: str,
    model_name: str,
    amount_tolerance: float,
) -> dict[str, Any]:
    payload = json.loads(dataset_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Dataset JSON must be an object with a 'cases' array.")
    cases = payload.get("cases")
    if not isinstance(cases, list) or not cases:
        raise ValueError("Dataset JSON requires a non-empty 'cases' array.")

    engine = NormalizationRuleEngine.from_path(rules_path)
    results = [
        evaluate_case(
            case=case,
            engine=engine,
            provider=provider,
            model_name=model_name,
            amount_tolerance=amount_tolerance,
        )
        for case in cases
    ]
    ok_results = [r for r in results if r.get("status") == "ok"]
    error_results = [r for r in results if r.get("status") != "ok"]

    avg_score = 0.0
    if ok_results:
        avg_score = sum(float(r["score"]) for r in ok_results) / len(ok_results)

    provider_mix: dict[str, int] = {}
    for row in ok_results:
        provider_name = str(row.get("used_provider", "unknown"))
        provider_mix[provider_name] = provider_mix.get(provider_name, 0) + 1

    return {
        "dataset": str(dataset_path),
        "rules_path": str(rules_path),
        "provider": provider,
        "model_name": model_name,
        "amount_tolerance": amount_tolerance,
        "summary": {
            "cases_total": len(results),
            "ok_total": len(ok_results),
            "error_total": len(error_results),
            "avg_score": round(avg_score, 4),
            "provider_mix": provider_mix,
        },
        "results": results,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Evaluate extraction quality against a golden invoice set.")
    parser.add_argument(
        "--dataset",
        default="eval/golden_set.json",
        help="Path to dataset JSON with a top-level 'cases' array.",
    )
    parser.add_argument(
        "--rules-path",
        default="config/normalization_rules.json",
        help="Normalization rules JSON path.",
    )
    parser.add_argument("--provider", default="auto", help="Extraction provider (default: auto).")
    parser.add_argument("--model", default="auto", help="Model name (default: auto).")
    parser.add_argument(
        "--amount-tolerance",
        type=float,
        default=0.01,
        help="Numeric comparison tolerance for amount fields.",
    )
    parser.add_argument(
        "--output",
        default="logs/golden_eval_report.json",
        help="Where to write full JSON report.",
    )
    parser.add_argument(
        "--fail-under",
        type=float,
        default=0.0,
        help="Exit with code 1 if average score is below this value.",
    )
    return parser


def main() -> int:
    load_dotenv()
    parser = build_parser()
    args = parser.parse_args()

    dataset_path = Path(args.dataset)
    rules_path = Path(args.rules_path)
    output_path = Path(args.output)

    if not dataset_path.exists():
        parser.error(f"Dataset file not found: {dataset_path}")
    if not rules_path.exists():
        parser.error(f"Rules file not found: {rules_path}")

    report = run_evaluation(
        dataset_path=dataset_path,
        rules_path=rules_path,
        provider=args.provider,
        model_name=args.model,
        amount_tolerance=args.amount_tolerance,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    summary = report["summary"]
    print(
        "Golden set evaluation complete | "
        f"cases={summary['cases_total']} ok={summary['ok_total']} errors={summary['error_total']} "
        f"avg_score={summary['avg_score']}"
    )
    if summary["provider_mix"]:
        print(f"Provider mix: {summary['provider_mix']}")
    print(f"Report written to: {output_path}")

    fail_under = float(args.fail_under)
    if fail_under > 0 and float(summary["avg_score"]) < fail_under:
        print(f"Average score {summary['avg_score']} is below fail-under threshold {fail_under}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
