"""vigie_spark_operator — SparkK8sOperator (Airflow → SparkApplication K8s)."""

from __future__ import annotations

from typing import Any

__version__ = "0.1.2"
__all__ = ["SparkK8sOperator", "__version__"]


def __getattr__(name: str) -> Any:
    if name == "SparkK8sOperator":
        from vigie_spark_operator.operator import SparkK8sOperator

        return SparkK8sOperator
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
