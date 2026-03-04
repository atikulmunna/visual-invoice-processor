from __future__ import annotations

import json
from pathlib import Path

from app.evaluation import run_evaluation


def test_run_evaluation_scores_case_with_tolerance(tmp_path: Path, monkeypatch) -> None:
    sample_file = tmp_path / "sample.pdf"
    sample_file.write_bytes(b"%PDF")

    dataset = {
        "cases": [
            {
                "file_path": str(sample_file),
                "expected": {
                    "vendor_name": "Acme Corp",
                    "total_amount": 100.0,
                    "currency": "USD",
                },
            }
        ]
    }
    dataset_path = tmp_path / "golden.json"
    dataset_path.write_text(json.dumps(dataset), encoding="utf-8")

    def _fake_extract_document(*args, **kwargs):
        _ = (args, kwargs)
        return {
            "vendor_name": "ACME CORP",
            "total_amount": 100.004,
            "currency": "usd",
            "_provider": "mistral",
        }

    monkeypatch.setattr("app.evaluation.extract_document", _fake_extract_document)

    report = run_evaluation(
        dataset_path=dataset_path,
        rules_path=Path("config/normalization_rules.json"),
        provider="auto",
        model_name="auto",
        amount_tolerance=0.01,
    )
    assert report["summary"]["cases_total"] == 1
    assert report["summary"]["error_total"] == 0
    assert report["summary"]["avg_score"] == 1.0
    assert report["summary"]["provider_mix"]["mistral"] == 1


def test_run_evaluation_flags_line_item_count_mismatch(tmp_path: Path, monkeypatch) -> None:
    sample_file = tmp_path / "sample2.pdf"
    sample_file.write_bytes(b"%PDF")

    dataset = {
        "cases": [
            {
                "file_path": str(sample_file),
                "expected": {
                    "line_items": [
                        {"description": "Item A", "quantity": 1, "line_total": 50.0},
                        {"description": "Item B", "quantity": 1, "line_total": 50.0},
                    ]
                },
            }
        ]
    }
    dataset_path = tmp_path / "golden2.json"
    dataset_path.write_text(json.dumps(dataset), encoding="utf-8")

    def _fake_extract_document(*args, **kwargs):
        _ = (args, kwargs)
        return {
            "line_items": [{"description": "Item A", "quantity": 1, "line_total": 100.0}],
            "_provider": "mistral",
        }

    monkeypatch.setattr("app.evaluation.extract_document", _fake_extract_document)

    report = run_evaluation(
        dataset_path=dataset_path,
        rules_path=Path("config/normalization_rules.json"),
        provider="auto",
        model_name="auto",
        amount_tolerance=0.01,
    )
    assert report["summary"]["avg_score"] < 1.0
    field_rows = report["results"][0]["field_results"]
    assert any(r["field"] == "line_items.count" and r["matched"] is False for r in field_rows)
