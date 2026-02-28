from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class DeadLetterStore:
    def __init__(self, file_path: str | Path = "logs/dead_letter.jsonl") -> None:
        self._path = Path(file_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def write_failure(self, payload: dict[str, Any]) -> None:
        event = {
            "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
            **payload,
        }
        with self._path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(event, ensure_ascii=True) + "\n")

    def list_failures(self, status: str | None = None) -> list[dict[str, Any]]:
        if not self._path.exists():
            return []
        items: list[dict[str, Any]] = []
        for line in self._path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            event = json.loads(line)
            if status and event.get("status") != status:
                continue
            items.append(event)
        return items

