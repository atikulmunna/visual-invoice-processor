from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from app.main import run_review_list, run_review_resolve


def test_run_review_list_prints_active_items(tmp_path: Path, monkeypatch, capsys) -> None:
    queue = tmp_path / "review_queue"
    queue.mkdir()
    (queue / "doc-21.json").write_text(
        json.dumps(
            {
                "document_id": "doc-21",
                "status": "REVIEW_REQUIRED",
                "reason_codes": ["low_confidence"],
                "created_at_utc": "2026-03-20T00:00:00+00:00",
                "metadata": {"source_file_id": "inbox/doc-21.pdf"},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("app.main.Settings.from_env", classmethod(lambda cls: SimpleNamespace(log_level="INFO")))
    monkeypatch.setattr("app.main.configure_logging", lambda level: None)

    code = run_review_list(queue_dir=str(queue))
    output = capsys.readouterr().out

    assert code == 0
    assert "doc-21" in output
    assert "low_confidence" in output


def test_run_review_resolve_uses_normalized_record_and_marks_resolved(tmp_path: Path, monkeypatch) -> None:
    queue = tmp_path / "review_queue"
    queue.mkdir()
    review_file = queue / "doc-31.json"
    review_file.write_text(
        json.dumps(
            {
                "document_id": "doc-31",
                "status": "REVIEW_REQUIRED",
                "reason_codes": ["validation_failed"],
                "metadata": {
                    "source_file_id": "inbox/doc-31.pdf",
                    "file_hash": "hash-31",
                    "used_provider": "mistral",
                    "normalized_record": {
                        "document_type": "invoice",
                        "vendor_name": "Acme",
                        "vendor_tax_id": None,
                        "invoice_number": "INV-31",
                        "invoice_date": "2026-03-20",
                        "due_date": None,
                        "currency": "BDT",
                        "subtotal": 100.0,
                        "tax_amount": 0.0,
                        "total_amount": 100.0,
                        "payment_method": "cash",
                        "line_items": [],
                        "model_confidence": 0.95,
                        "validation_score": 0.95,
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr("app.main.Settings.from_env", classmethod(lambda cls: SimpleNamespace(log_level="INFO")))
    monkeypatch.setattr("app.main.configure_logging", lambda level: None)

    append_calls: list[tuple[dict, dict]] = []

    def _fake_append_record(*, record: dict, metadata: dict) -> dict:
        append_calls.append((record, metadata))
        return {"status": "appended", "row_id": 99}

    monkeypatch.setattr("app.main.append_record", _fake_append_record)

    code = run_review_resolve(
        document_id="doc-31",
        queue_dir=str(queue),
        record_path=None,
        note="approved",
    )

    assert code == 0
    assert len(append_calls) == 1
    resolved_payload = json.loads(review_file.read_text(encoding="utf-8"))
    assert resolved_payload["status"] == "RESOLVED_STORED"
    assert resolved_payload["storage_result"]["row_id"] == 99
    assert resolved_payload["resolved_record"]["vendor_name"] == "Acme"
