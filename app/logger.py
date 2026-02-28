from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if hasattr(record, "document_id"):
            payload["document_id"] = getattr(record, "document_id")
        if hasattr(record, "drive_file_id"):
            payload["drive_file_id"] = getattr(record, "drive_file_id")
        if hasattr(record, "state"):
            payload["state"] = getattr(record, "state")
        if hasattr(record, "stage"):
            payload["stage"] = getattr(record, "stage")
        if hasattr(record, "latency_ms"):
            payload["latency_ms"] = getattr(record, "latency_ms")
        if hasattr(record, "outcome"):
            payload["outcome"] = getattr(record, "outcome")
        return json.dumps(payload, ensure_ascii=True)


def configure_logging(level: str = "INFO") -> None:
    root = logging.getLogger()
    root.setLevel(level.upper())
    if root.handlers:
        for handler in root.handlers:
            handler.setFormatter(JsonFormatter())
        return
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)


def log_document_event(
    logger: logging.Logger,
    level: int,
    message: str,
    *,
    document_id: str,
    drive_file_id: str | None = None,
    state: str | None = None,
    stage: str | None = None,
    latency_ms: int | None = None,
    outcome: str | None = None,
) -> None:
    extra: dict[str, Any] = {"document_id": document_id}
    if drive_file_id is not None:
        extra["drive_file_id"] = drive_file_id
    if state is not None:
        extra["state"] = state
    if stage is not None:
        extra["stage"] = stage
    if latency_ms is not None:
        extra["latency_ms"] = latency_ms
    if outcome is not None:
        extra["outcome"] = outcome
    logger.log(level, message, extra=extra)
