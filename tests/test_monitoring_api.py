from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from app.monitoring_api import (
    _active_dead_letters,
    _active_review_items,
    _active_review_queue_size,
    create_monitoring_app,
)


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")


def test_monitoring_endpoints_expose_stats_backlog_and_failures(tmp_path: Path) -> None:
    metrics = tmp_path / "logs" / "metrics.jsonl"
    dead = tmp_path / "logs" / "dead_letter.jsonl"
    queue = tmp_path / "review_queue"
    queue.mkdir(parents=True, exist_ok=True)
    (queue / "doc-1.json").write_text(
        json.dumps(
            {
                "document_id": "doc-1",
                "status": "REVIEW_REQUIRED",
                "metadata": {
                    "file_hash": "hash-open",
                    "source_file_id": "inbox/doc-1.pdf",
                    "normalized_record": {
                        "vendor_name": "Acme",
                        "currency": "BDT",
                        "total_amount": 125.0,
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    _write_jsonl(
        metrics,
        [
            {"metric": "documents_processed_total", "value": 3},
            {"metric": "documents_failed_total", "value": 1},
        ],
    )
    _write_jsonl(
        dead,
        [
            {"document_id": "doc-a", "status": "FAILED"},
            {"document_id": "doc-b", "status": "REVIEW_REQUIRED"},
        ],
    )

    app = create_monitoring_app(metrics_path=metrics, dead_letter_path=dead, review_queue_dir=queue)
    client = TestClient(app)

    health = client.get("/health")
    stats = client.get("/stats")
    backlog = client.get("/backlog")
    failures = client.get("/failures")
    review_items = client.get("/review-items")
    dashboard = client.get("/dashboard")
    dashboard_data = client.get("/dashboard/data")

    assert health.status_code == 200
    assert health.json()["status"] == "ok"
    assert stats.status_code == 200
    assert stats.json()["documents_processed_total"] == 3
    assert stats.json()["dead_letter_total"] == 2
    assert stats.json()["review_queue_total"] == 1
    assert backlog.status_code == 200
    assert backlog.json()["attention_total"] == 3
    assert failures.status_code == 200
    assert failures.json()["count"] == 2
    assert review_items.status_code == 200
    assert review_items.json()["count"] == 1
    assert review_items.json()["items"][0]["vendor_name"] == "Acme"
    assert dashboard.status_code == 200
    assert "Invoice Operations Dashboard" in dashboard.text
    assert dashboard_data.status_code == 200
    assert "kpis" in dashboard_data.json()


def test_active_backlog_filters_resolved_hashes(tmp_path: Path) -> None:
    dead = tmp_path / "logs" / "dead_letter.jsonl"
    review = tmp_path / "review_queue"
    review.mkdir(parents=True, exist_ok=True)
    _write_jsonl(
        dead,
        [
            {"document_id": "a", "status": "FAILED", "file_hash": "hash-a"},
            {"document_id": "b", "status": "REVIEW_REQUIRED", "file_hash": "hash-b"},
        ],
    )
    (review / "a.json").write_text(
        json.dumps({"document_id": "a", "status": "REVIEW_REQUIRED", "metadata": {"file_hash": "hash-a"}}),
        encoding="utf-8",
    )
    (review / "b.json").write_text(
        json.dumps({"document_id": "b", "status": "REVIEW_REQUIRED", "metadata": {"file_hash": "hash-b"}}),
        encoding="utf-8",
    )

    resolved = {"hash-a"}
    assert len(_active_dead_letters(dead, resolved)) == 1
    assert _active_review_queue_size(review, resolved) == 1
    assert len(_active_review_items(review, resolved)) == 1


def test_review_resolve_endpoint_uses_shared_resolution_flow(tmp_path: Path, monkeypatch) -> None:
    queue = tmp_path / "review_queue"
    queue.mkdir(parents=True, exist_ok=True)
    (queue / "doc-2.json").write_text(
        json.dumps(
            {
                "document_id": "doc-2",
                "status": "REVIEW_REQUIRED",
                "metadata": {
                    "file_hash": "hash-2",
                    "source_file_id": "inbox/doc-2.pdf",
                    "normalized_record": {"vendor_name": "Beta", "total_amount": 42.0},
                },
            }
        ),
        encoding="utf-8",
    )

    called: dict[str, object] = {}

    def _fake_resolve_review_item(
        document_id: str,
        *,
        queue_dir: str | Path,
        record_path: str | None = None,
        record_override: dict | None = None,
        note: str | None = None,
    ) -> dict[str, object]:
        called["document_id"] = document_id
        called["queue_dir"] = str(queue_dir)
        called["record_path"] = record_path
        called["record_override"] = record_override
        called["note"] = note
        return {
            "storage_result": {"status": "appended", "row_id": 10},
            "review_item": {"status": "RESOLVED_STORED"},
            "resolved_record": {"vendor_name": "Beta"},
        }

    monkeypatch.setattr("app.monitoring_api.resolve_review_item", _fake_resolve_review_item)

    app = create_monitoring_app(review_queue_dir=queue)
    client = TestClient(app)
    response = client.post(
        "/review-items/doc-2/resolve",
        json={"note": "approved", "corrected_record": {"vendor_name": "Gamma", "total_amount": 45.0}},
    )

    assert response.status_code == 200
    assert response.json()["review_status"] == "RESOLVED_STORED"
    assert called["document_id"] == "doc-2"
    assert called["note"] == "approved"
    assert called["record_override"] == {"vendor_name": "Gamma", "total_amount": 45.0}


def test_review_action_endpoint_supports_duplicate_and_reject(tmp_path: Path, monkeypatch) -> None:
    queue = tmp_path / "review_queue"
    queue.mkdir(parents=True, exist_ok=True)
    (queue / "doc-3.json").write_text(
        json.dumps(
            {
                "document_id": "doc-3",
                "status": "REVIEW_REQUIRED",
                "metadata": {
                    "file_hash": "hash-3",
                    "source_file_id": "inbox/doc-3.pdf",
                    "normalized_record": {"vendor_name": "Delta", "total_amount": 50.0},
                },
            }
        ),
        encoding="utf-8",
    )

    dismiss_calls: list[tuple[str, str, str | None]] = []

    def _fake_dismiss_review_item(
        document_id: str,
        *,
        queue_dir: str | Path,
        resolution_status: str,
        note: str | None = None,
    ) -> dict[str, object]:
        dismiss_calls.append((document_id, resolution_status, note))
        return {
            "storage_result": {"status": "dismissed", "action": resolution_status},
            "review_item": {"status": resolution_status},
            "resolved_record": None,
        }

    monkeypatch.setattr("app.monitoring_api.dismiss_review_item", _fake_dismiss_review_item)

    app = create_monitoring_app(review_queue_dir=queue)
    client = TestClient(app)

    duplicate_response = client.post("/review-items/doc-3/resolve", json={"action": "duplicate", "note": "already stored"})
    reject_response = client.post("/review-items/doc-3/resolve", json={"action": "reject", "note": "invalid document"})

    assert duplicate_response.status_code == 200
    assert duplicate_response.json()["review_status"] == "RESOLVED_DUPLICATE_MANUAL"
    assert reject_response.status_code == 200
    assert reject_response.json()["review_status"] == "REJECTED"
    assert dismiss_calls == [
        ("doc-3", "RESOLVED_DUPLICATE_MANUAL", "already stored"),
        ("doc-3", "REJECTED", "invalid document"),
    ]
