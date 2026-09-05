"""Graceful shutdown SparkApplication / pods."""

from __future__ import annotations

import logging
import time
from typing import Any, Callable

logger = logging.getLogger(__name__)


def handle_job_timeout(
    *,
    elapsed: float,
    timeout: float,
    namespace: str,
    app_name: str,
    delete_app: Callable[[], None],
    force_kill_pods: Callable[[], None],
    log: logging.Logger | None = None,
    warn_before: float = 60.0,
    force_after: float = 30.0,
) -> None:
    """
    Approche timeout → warning, delete SparkApplication (SIGTERM driver),
    puis force-kill des pods si toujours présents.
    """
    log = log or logger
    if elapsed >= timeout - warn_before and elapsed < timeout:
        log.warning(
            "SparkApplication %s/%s approaching timeout (%.0fs / %.0fs)",
            namespace,
            app_name,
            elapsed,
            timeout,
        )
    if elapsed >= timeout:
        log.error("Timeout job atteint (%.0fs) — suppression SparkApplication %s", timeout, app_name)
        delete_app()
    if elapsed >= timeout + force_after:
        log.error("Force-kill pods pour %s/%s après timeout+%.0fs", namespace, app_name, force_after)
        force_kill_pods()


def wait_until(
    predicate: Callable[[], bool],
    *,
    timeout: float,
    poll: float = 2.0,
    label: str = "condition",
) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(poll)
    logger.warning("Timeout waiting for %s (%.0fs)", label, timeout)
    return False


def resource_summary(spec: dict[str, Any]) -> dict[str, Any]:
    """Résumé ressources pour dry_run / pré-vol capacité."""
    s = spec.get("spec") or {}
    driver = s.get("driver") or {}
    executor = s.get("executor") or {}
    instances = int(executor.get("instances") or 0)
    return {
        "name": (spec.get("metadata") or {}).get("name"),
        "namespace": (spec.get("metadata") or {}).get("namespace"),
        "driver": {
            "cores": driver.get("cores"),
            "coreRequest": driver.get("coreRequest"),
            "memory": driver.get("memory"),
        },
        "executor": {
            "instances": instances,
            "cores": executor.get("cores"),
            "coreRequest": executor.get("coreRequest"),
            "memory": executor.get("memory"),
        },
        "labels": {
            "metadata": (spec.get("metadata") or {}).get("labels") or {},
            "driver": driver.get("labels") or {},
            "executor": executor.get("labels") or {},
        },
    }
