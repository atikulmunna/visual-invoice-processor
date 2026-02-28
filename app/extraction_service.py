from __future__ import annotations

from pathlib import Path
from typing import Any


class ExtractionError(RuntimeError):
    """Raised when model extraction fails."""


def extract_document(file_path: str | Path, model_name: str = "gpt-4o") -> dict[str, Any]:
    _ = (file_path, model_name)
    raise NotImplementedError("Extraction service implementation is planned in P1-03.")

