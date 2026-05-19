from __future__ import annotations

from importlib import import_module

from adapters.bundle import ClientBundle


# 按 exchange.name 动态导入当前 venue 的 data/exec client 构造器。
def build_client_bundle(settings: dict) -> ClientBundle:
    name = settings["exchange"]["name"].lower().replace("-", "_")
    try:
        module = import_module(f"adapters.{name}")
    except ModuleNotFoundError as exc:
        if exc.name != f"adapters.{name}":
            raise
        raise ValueError(f"Unsupported exchange.name: {settings['exchange']['name']}") from exc
    return module.build_client_bundle(settings)
