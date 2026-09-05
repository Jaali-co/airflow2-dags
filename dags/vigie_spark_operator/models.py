"""Modèles de config volumes pour SparkK8sOperator."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class PvcConfig:
    claim_name: str
    mount_path: str
    name: str | None = None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        if d.get("name") is None:
            d.pop("name", None)
        return {k: v for k, v in d.items() if v is not None}


@dataclass(frozen=True)
class EmptyDirConfig:
    name: str
    mount_path: str
    size_limit: str | None = None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        return {k: v for k, v in d.items() if v is not None}


def normalize_pvc_list(items: list | None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in items or []:
        if isinstance(item, PvcConfig):
            out.append(item.to_dict())
        else:
            out.append(dict(item))
    return out


def normalize_emptydir_list(items: list | None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in items or []:
        if isinstance(item, EmptyDirConfig):
            out.append(item.to_dict())
        else:
            out.append(dict(item))
    return out
