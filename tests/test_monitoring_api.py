from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from app.monitoring_api import _active_dead_letters, _active_review_queue_size, create_monitoring_app


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
                "metadata": {"file_hash": "hash-open"},
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
