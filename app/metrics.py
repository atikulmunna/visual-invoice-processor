from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class MetricsCollector:
    counters: Counter[str] = field(default_factory=Counter)
    latencies_ms: list[int] = field(default_factory=list)

    def increment(self, name: str, value: int = 1) -> None:
        self.counters[name] += value

    def observe_latency(self, value_ms: int) -> None:
        self.latencies_ms.append(value_ms)

    def snapshot(self) -> dict[str, Any]:
        p95 = 0
        if self.latencies_ms:
            ordered = sorted(self.latencies_ms)
            idx = int(0.95 * (len(ordered) - 1))
            p95 = ordered[idx]
        return {
            "throughput_total": self.counters.get("documents_processed_total", 0),
            "success_total": self.counters.get("documents_success_total", 0),
            "review_total": self.counters.get("documents_review_total", 0),
            "failure_total": self.counters.get("documents_failed_total", 0),
            "duplicate_skips_total": self.counters.get("documents_duplicate_skipped_total", 0),
            "latency_p95_ms": p95,
        }


class JsonlMetricsSink:
    def __init__(self, path: str | Path = "logs/metrics.jsonl") -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def emit(self, event: dict[str, Any]) -> None:
        payload = {
            "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
            **event,
        }
        with self._path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=True) + "\n")

