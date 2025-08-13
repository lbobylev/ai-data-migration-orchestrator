from time import time
from typing import Callable, TypeVar
import requests
import random

from logger import get_logger

logger = get_logger(__name__)


def _is_transient_error(e: Exception) -> bool:
    return isinstance(
        e, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)
    )


def _sleep_backoff(attempt: int, base: float = 0.5, cap: float = 8.0):
    # exp backoff with full jitter
    delay = min(cap, base * (2 ** (attempt - 1)))
    time.sleep(random.uniform(0, delay))

OutputT = TypeVar("OutputT")


def retry_call(
    fn: Callable[[], OutputT], *, max_attempts: int = 3
) -> OutputT:
    attempt = 1
    while attempt <= max_attempts:
        try:
            return fn()
        except Exception as e:
            if attempt >= max_attempts or not _is_transient_error(e):
                raise
            logger.warning(f"Transient error on attempt {attempt}: {e}")
            _sleep_backoff(attempt)
            attempt += 1
    raise RuntimeError("Unreachable code reached in retry_call")

