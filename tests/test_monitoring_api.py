from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from app.monitoring_api import create_monitoring_app


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
    (queue / "doc-1.json").write_text("{}", encoding="utf-8")

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

