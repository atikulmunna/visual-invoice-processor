from __future__ import annotations

import json
import logging

from app.logger import JsonFormatter


def _format(**kwargs: object) -> dict[str, object]:
    record = logging.getLogger("test-observability").makeRecord(
        name="test-observability", level=logging.ERROR, fn="test", lno=1, msg="Pipeline failed for %s",
        args=("inbox/a.pdf",), **kwargs,
    )
    return json.loads(JsonFormatter().format(record))


def test_log_lines_are_json_with_the_message_filled_in() -> None:
    payload = _format(exc_info=None)

    assert (payload["level"], payload["logger"]) == ("ERROR", "test-observability")
    assert payload["message"] == "Pipeline failed for inbox/a.pdf"
    assert "exception" not in payload


def test_logged_exceptions_keep_their_traceback() -> None:
    try:
        raise RuntimeError("database unreachable")
    except RuntimeError:
        import sys

        payload = _format(exc_info=sys.exc_info())

    assert "Traceback" in str(payload["exception"])
    assert "RuntimeError: database unreachable" in str(payload["exception"])
