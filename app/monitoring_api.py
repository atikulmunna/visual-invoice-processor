from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI


def create_monitoring_app(
    *,
    metrics_path: str | Path = "logs/metrics.jsonl",
    dead_letter_path: str | Path = "logs/dead_letter.jsonl",
    review_queue_dir: str | Path = "review_queue",
) -> FastAPI:
    app = FastAPI(title="Invoice Processor Monitoring API", version="0.1.0")

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/stats")
    def stats() -> dict[str, Any]:
        metric_events = _read_jsonl(metrics_path)
        dead_letters = _read_jsonl(dead_letter_path)
        queue_size = _review_queue_size(review_queue_dir)
        counters = _aggregate_metrics(metric_events)
        counters["dead_letter_total"] = len(dead_letters)
        counters["review_queue_total"] = queue_size
        return counters

    @app.get("/failures")
    def failures(limit: int = 50) -> dict[str, Any]:
        items = _read_jsonl(dead_letter_path)
        return {"count": len(items), "items": items[-limit:]}

    @app.get("/backlog")
    def backlog() -> dict[str, Any]:
        queue_size = _review_queue_size(review_queue_dir)
        dead_letters = len(_read_jsonl(dead_letter_path))
        return {
            "review_queue_total": queue_size,
            "dead_letter_total": dead_letters,
            "attention_total": queue_size + dead_letters,
        }

    return app


def _read_jsonl(path: str | Path) -> list[dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in p.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rows.append(json.loads(line))
    return rows


def _review_queue_size(path: str | Path) -> int:
    p = Path(path)
    if not p.exists():
        return 0
    return len([x for x in p.glob("*.json") if x.is_file()])


def _aggregate_metrics(events: list[dict[str, Any]]) -> dict[str, Any]:
    counters: dict[str, int] = {}
    for event in events:
        name = event.get("metric")
        value = event.get("value")
        if isinstance(name, str) and isinstance(value, int):
            counters[name] = counters.get(name, 0) + value
    return counters

