from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Callable, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    base_delay_seconds: float = 0.5
    max_delay_seconds: float = 8.0
    jitter_ratio: float = 0.25

    def delay_for_attempt(self, attempt: int) -> float:
        backoff = min(self.base_delay_seconds * (2 ** (attempt - 1)), self.max_delay_seconds)
        jitter = backoff * self.jitter_ratio * random.random()
        return backoff + jitter


class RetryExhaustedError(RuntimeError):
    pass


def run_with_retry(
    operation: Callable[[], T],
    should_retry: Callable[[Exception], bool],
    policy: RetryPolicy | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> T:
    active = policy or RetryPolicy()
    last_error: Exception | None = None
    for attempt in range(1, active.max_attempts + 1):
        try:
            return operation()
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt >= active.max_attempts or not should_retry(exc):
                break
            sleep_fn(active.delay_for_attempt(attempt))
    raise RetryExhaustedError("Operation failed after max retry attempts") from last_error

