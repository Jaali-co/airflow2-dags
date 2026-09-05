"""Retry K8s avec backoff exponentiel."""

from __future__ import annotations

import logging
import time
from typing import Callable, TypeVar

from kubernetes.client.exceptions import ApiException

T = TypeVar("T")
logger = logging.getLogger(__name__)

RETRYABLE = frozenset({429, 503})


def k8s_call_with_retry(
    fn: Callable[..., T],
    *args,
    max_retries: int = 3,
    base_delay: float = 2.0,
    log: logging.Logger | None = None,
    **kwargs,
) -> T:
    """Appelle ``fn`` ; retry sur 429/503 avec backoff 2s, 4s, 8s…"""
    log = log or logger
    last: Exception | None = None
    for attempt in range(max_retries):
        try:
            return fn(*args, **kwargs)
        except ApiException as exc:
            last = exc
            if exc.status in RETRYABLE and attempt < max_retries - 1:
                delay = base_delay * (2**attempt)
                log.warning("K8s API %s, retry in %ss (attempt %s/%s)", exc.status, delay, attempt + 1, max_retries)
                time.sleep(delay)
                continue
            raise
    assert last is not None
    raise last
