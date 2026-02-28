from __future__ import annotations

import json
import logging
from pathlib import Path

from app.logger import JsonFormatter, log_document_event
from app.metrics import JsonlMetricsSink, MetricsCollector


def test_log_document_event_includes_document_id() -> None:
    logger = logging.getLogger("test-observability")
    record = logger.makeRecord(
        name=logger.name,
        level=logging.INFO,
        fn="test",
        lno=1,
        msg="processing",
        args=(),
        exc_info=None,
        extra={
            "document_id": "doc-1",
            "stage": "extraction",
            "latency_ms": 120,
            "outcome": "success",
        },
    )
    formatter = JsonFormatter()
    payload = json.loads(formatter.format(record))
    assert payload["document_id"] == "doc-1"
    assert payload["stage"] == "extraction"
    assert payload["latency_ms"] == 120
    assert payload["outcome"] == "success"


def test_metrics_collector_snapshot() -> None:
    metrics = MetricsCollector()
    metrics.increment("documents_processed_total")
    metrics.increment("documents_success_total")
    metrics.increment("documents_duplicate_skipped_total", 2)
    metrics.observe_latency(50)
    metrics.observe_latency(200)
    metrics.observe_latency(100)

    snapshot = metrics.snapshot()
    assert snapshot["throughput_total"] == 1
    assert snapshot["success_total"] == 1
    assert snapshot["duplicate_skips_total"] == 2
    assert snapshot["latency_p95_ms"] >= 100


def test_jsonl_metrics_sink_writes_event(tmp_path: Path) -> None:
    sink = JsonlMetricsSink(path=tmp_path / "metrics.jsonl")
    sink.emit({"metric": "documents_processed_total", "value": 1})
    lines = (tmp_path / "metrics.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["metric"] == "documents_processed_total"
    assert payload["value"] == 1


def test_log_document_event_helper_does_not_raise() -> None:
    logger = logging.getLogger("test-observability-helper")
    log_document_event(
        logger,
        logging.INFO,
        "done",
        document_id="doc-22",
        drive_file_id="drive-22",
        state="STORED",
        stage="storage",
        latency_ms=33,
        outcome="success",
    )

