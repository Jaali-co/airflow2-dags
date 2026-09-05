"""Shim de compat — préférer ``from vigie_spark_operator import SparkK8sOperator``."""

from vigie_spark_operator import SparkK8sOperator

__all__ = ["SparkK8sOperator"]
