"""Airflow 2 / 3 compatibility shims for Vigie operators."""

from __future__ import annotations

from typing import Any


def get_base_airflow_version_tuple() -> tuple[int, int, int]:
    try:
        from airflow import __version__ as v
    except Exception:
        return (0, 0, 0)
    parts = []
    for p in str(v).split(".")[:3]:
        try:
            parts.append(int("".join(ch for ch in p if ch.isdigit()) or "0"))
        except ValueError:
            parts.append(0)
    while len(parts) < 3:
        parts.append(0)
    return parts[0], parts[1], parts[2]


AIRFLOW_V_3_0_PLUS = get_base_airflow_version_tuple() >= (3, 0, 0)


def _load_base_operator() -> Any:
    if AIRFLOW_V_3_0_PLUS:
        try:
            from airflow.sdk import BaseOperator as BO  # type: ignore

            return BO
        except Exception:
            pass
    try:
        from airflow.models import BaseOperator as BO  # type: ignore

        return BO
    except Exception:
        from airflow.models.baseoperator import BaseOperator as BO  # type: ignore

        return BO


def _load_variable() -> Any:
    if AIRFLOW_V_3_0_PLUS:
        try:
            from airflow.sdk import Variable as V  # type: ignore

            return V
        except Exception:
            pass
    try:
        from airflow.models import Variable as V  # type: ignore

        return V
    except Exception:
        from airflow.models.variable import Variable as V  # type: ignore

        return V


BaseOperator = _load_base_operator()


def __getattr__(name: str) -> Any:
    if name == "Variable":
        return _load_variable()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "AIRFLOW_V_3_0_PLUS",
    "BaseOperator",
    "Variable",
    "get_base_airflow_version_tuple",
]
