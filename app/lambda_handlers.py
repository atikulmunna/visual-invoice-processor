from __future__ import annotations

from typing import Any

from app.config import load_aws_parameter_secrets

load_aws_parameter_secrets()

from app.monitoring_api import create_monitoring_app
from app.serverless_worker import handle_s3_event

try:
    from mangum import Mangum
except ImportError as exc:  # pragma: no cover - validated in the Lambda image
    raise RuntimeError("mangum is required for the Lambda web handler") from exc


web_handler = Mangum(create_monitoring_app(), lifespan="off")


def s3_handler(event: dict[str, Any], _: Any) -> dict[str, Any]:
    results = handle_s3_event(event)
    return {"processed": len(results), "results": results}
