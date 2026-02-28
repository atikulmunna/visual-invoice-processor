from __future__ import annotations

from pathlib import Path

import pytest

from app.dead_letter import DeadLetterStore
from app.retry_utils import RetryExhaustedError, RetryPolicy, run_with_retry


class _TransientError(RuntimeError):
    pass


class _FatalError(RuntimeError):
    pass


def test_run_with_retry_succeeds_after_transient_failures() -> None:
    state = {"count": 0}
    sleeps: list[float] = []

    def _op() -> str:
        state["count"] += 1
        if state["count"] < 3:
            raise _TransientError("temporary")
        return "ok"

    result = run_with_retry(
        operation=_op,
        should_retry=lambda e: isinstance(e, _TransientError),
        policy=RetryPolicy(max_attempts=4, base_delay_seconds=0.01, max_delay_seconds=0.02),
        sleep_fn=sleeps.append,
    )
    assert result == "ok"
    assert len(sleeps) == 2


def test_run_with_retry_stops_on_non_retryable_error() -> None:
    def _op() -> str:
        raise _FatalError("bad request")

    with pytest.raises(RetryExhaustedError):
        run_with_retry(
            operation=_op,
            should_retry=lambda e: isinstance(e, _TransientError),
            policy=RetryPolicy(max_attempts=5, base_delay_seconds=0.01),
            sleep_fn=lambda _: None,
        )


def test_dead_letter_store_write_and_query(tmp_path: Path) -> None:
    store = DeadLetterStore(file_path=tmp_path / "dead_letter.jsonl")
    store.write_failure(
        {
            "document_id": "doc-1",
            "status": "FAILED",
            "error_code": "timeout",
            "attempts": 3,
        }
    )
    store.write_failure(
        {
            "document_id": "doc-2",
            "status": "REVIEW_REQUIRED",
            "error_code": "invalid_json",
            "attempts": 2,
        }
    )

    all_items = store.list_failures()
    failed_items = store.list_failures(status="FAILED")

    assert len(all_items) == 2
    assert len(failed_items) == 1
    assert failed_items[0]["document_id"] == "doc-1"

