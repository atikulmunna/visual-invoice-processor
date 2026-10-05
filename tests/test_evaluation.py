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


def test_report_breaks_accuracy_down_by_field_and_document_type(tmp_path: Path, monkeypatch) -> None:
    samples = {name: tmp_path / f"{name}.pdf" for name in ("bill", "receipt", "missing")}
    for name in ("bill", "receipt"):
        samples[name].write_bytes(b"%PDF")
    lines = [{"description": "A", "quantity": 1.5, "line_total": 10.0}, {"description": "B", "quantity": 1, "line_total": 5.0}]
    dataset = {"cases": [
        {"file_path": str(samples["bill"]),
         "expected": {"document_type": "invoice", "currency": "USD", "shipping_amount": 2.0, "line_items": lines}},
        {"file_path": str(samples["receipt"]), "expected": {"document_type": "receipt", "currency": "BDT"}},
        {"file_path": str(samples["missing"]), "expected": {"document_type": "receipt", "currency": "BDT"}},
    ]}
    dataset_path = tmp_path / "golden.json"
    dataset_path.write_text(json.dumps(dataset), encoding="utf-8")
    outputs = {
        "bill": {"document_type": "invoice", "currency": "USD", "shipping_amount": 2.004, "line_items": [
            {"description": "A", "quantity": 1.504, "line_total": 10.003},
            {"description": "B", "quantity": 1, "line_total": 7.0},
        ]},
        "receipt": {"document_type": "receipt", "currency": "USD"},
    }
    monkeypatch.setattr("app.evaluation.extract_document", lambda file_path, **_: outputs[Path(file_path).stem])

    report = run_evaluation(
        dataset_path=dataset_path,
        rules_path=Path("config/normalization_rules.json"),
        provider="auto",
        model_name="auto",
        amount_tolerance=0.01,
    )

    by_field = report["by_field"]
    assert by_field["shipping_amount"] == {"matched": 1, "total": 1, "accuracy": 1.0}
    assert by_field["line_items.quantity"] == {"matched": 2, "total": 2, "accuracy": 1.0}
    assert by_field["line_items.line_total"] == {"matched": 1, "total": 2, "accuracy": 0.5}
    assert by_field["currency"] == {"matched": 1, "total": 2, "accuracy": 0.5}
    assert report["by_document_type"]["invoice"]["errors"] == 0
    assert report["by_document_type"]["receipt"] == {"cases": 2, "errors": 1, "avg_score": 0.5}
    assert report["summary"]["currency_assumed_total"] == 0


def test_report_counts_currencies_that_were_only_assumed(tmp_path: Path, monkeypatch) -> None:
    sample = tmp_path / "ryans.pdf"
    sample.write_bytes(b"%PDF")
    dataset_path = tmp_path / "golden.json"
    dataset_path.write_text(json.dumps({"cases": [{"file_path": str(sample), "expected": {"currency": "BDT"}}]}),
                            encoding="utf-8")
    monkeypatch.setattr("app.evaluation.extract_document", lambda *_, **__: {"currency": None, "total_amount": 400.0})

    report = run_evaluation(
        dataset_path=dataset_path,
        rules_path=Path("config/normalization_rules.json"),
        provider="auto",
        model_name="auto",
        amount_tolerance=0.01,
    )

    assert report["summary"]["avg_score"] == 1.0
    assert report["summary"]["currency_assumed_total"] == 1

