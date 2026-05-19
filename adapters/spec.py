from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from nautilus_trader.core.nautilus_pyo3 import CacheConfig


@dataclass(frozen=True)
class AdapterSpec:
    cache: CacheConfig
    data: dict[str, tuple[Any, Any]]
    exec: dict[str, tuple[Any, Any]]

