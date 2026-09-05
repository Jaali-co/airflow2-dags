"""Extraction et sanitization des labels Airflow pour pods Spark."""

from __future__ import annotations

import re
from typing import Any, Mapping

_LABEL_MAX = 63
_VALID = re.compile(r"^[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?$")


def sanitize_label_value(value: str, max_len: int = _LABEL_MAX) -> str:
    """Rend une valeur compatible label K8s (≤63, charset restreint)."""
    raw = (value or "").strip()
    if not raw:
        return "unknown"
    cleaned = re.sub(r"[^A-Za-z0-9._-]", "-", raw)
    cleaned = re.sub(r"[-_.]{2,}", "-", cleaned).strip("-_.")
    if not cleaned:
        cleaned = "unknown"
    if len(cleaned) > max_len:
        cleaned = cleaned[:max_len].rstrip("-_.")
    if not _VALID.match(cleaned):
        cleaned = re.sub(r"[^A-Za-z0-9]", "", cleaned)[:max_len] or "unknown"
    return cleaned


def airflow_labels(
    context: Mapping[str, Any],
    *,
    managed_by: str | None = None,
) -> dict[str, str]:
    """
    Labels ``dag_id`` / ``task_id`` / ``run_id`` depuis le contexte Airflow.

    ``managed_by`` est optionnel (ex. ``\"vigie\"`` pour le collecteur capacité).
    """
    dag = context.get("dag")
    task = context.get("task")
    dag_id = getattr(dag, "dag_id", None) or context.get("dag_id") or "unknown"
    task_id = getattr(task, "task_id", None) or context.get("task_id") or "unknown"
    run_id = context.get("run_id") or context.get("dag_run_id") or "unknown"

    out = {
        "dag_id": sanitize_label_value(str(dag_id)),
        "task_id": sanitize_label_value(str(task_id)),
        "run_id": sanitize_label_value(str(run_id)),
    }
    if managed_by:
        out["managed-by"] = sanitize_label_value(str(managed_by))
    return out


def merge_labels(*parts: Mapping[str, str] | None) -> dict[str, str]:
    out: dict[str, str] = {}
    for part in parts:
        if not part:
            continue
        for k, v in part.items():
            if k is None or v is None:
                continue
            out[str(k)] = sanitize_label_value(str(v))
    return out
