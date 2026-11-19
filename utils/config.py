from __future__ import annotations

import importlib
import os
from typing import Any

import yaml

from utils.constants import GLOBAL_CONFIG_PATH
from utils.constants import LIVE_CONFIG_FILENAME
from utils.constants import STRATEGIES_DIR


def _required(mapping: dict[str, Any], keys: tuple[str, ...], location: str) -> None:
    missing = [key for key in keys if key not in mapping]
    if missing:
        raise KeyError(f"{location} missing required keys: {', '.join(missing)}")


def _only(mapping: dict[str, Any], keys: set[str], location: str) -> None:
    unknown = sorted(set(mapping) - keys)
    if unknown:
        raise KeyError(f"{location} has unsupported keys: {', '.join(unknown)}")


def _import_path(value: object, location: str) -> str:
    path = str(value)
    if path.count(":") != 1:
        raise ValueError(f"{location} must use module:Class format")
    return path


def _artifacts(entry: dict[str, Any], location: str) -> None:
    artifacts = entry.get("artifacts", {})
    if not isinstance(artifacts, dict):
        raise TypeError(f"{location}.artifacts must be a mapping")
    for field, filename in artifacts.items():
        if not isinstance(field, str) or not isinstance(filename, str):
            raise TypeError(f"{location}.artifacts must map config fields to relative filenames")


# 策略只接受 NT 原生 import path、config 和可选批量参数。
def normalize_strategies(settings: dict[str, Any]) -> None:
    defaults = settings.pop("strategy_defaults")
    if not isinstance(defaults, dict):
        raise TypeError("strategy_defaults must be a mapping")
    strategies = settings["strategy"]
    if not isinstance(strategies, dict) or not strategies:
        raise ValueError("strategy must contain at least one instance")

    enabled = 0
    allowed = {"enabled", "strategy_path", "config_path", "config", "artifacts"}
    for name, entry in strategies.items():
        location = f"strategy.{name}"
        if not isinstance(entry, dict):
            raise TypeError(f"{location} must be a mapping")
        _only(entry, allowed, location)
        _required(entry, ("enabled", "strategy_path", "config_path", "config"), location)
        if not isinstance(entry["enabled"], bool):
            raise TypeError(f"{location}.enabled must be bool")
        entry["strategy_path"] = _import_path(entry["strategy_path"], f"{location}.strategy_path")
        entry["config_path"] = _import_path(entry["config_path"], f"{location}.config_path")
        if not isinstance(entry["config"], dict):
            raise TypeError(f"{location}.config must be a mapping")
        entry["config"] = {**defaults, **entry["config"]}
        _artifacts(entry, location)
        enabled += int(entry["enabled"])
    if enabled == 0:
        raise ValueError("at least one strategy must be enabled")


def normalize_actors(settings: dict[str, Any]) -> None:
    actors = settings["node"]["actors"]
    if not isinstance(actors, dict):
        raise TypeError("node.actors must be a mapping")
    allowed = {"actor_path", "config_path", "config", "artifacts"}
    for name, entry in actors.items():
        location = f"node.actors.{name}"
        if not isinstance(entry, dict):
            raise TypeError(f"{location} must be a mapping")
        _only(entry, allowed, location)
        _required(entry, ("actor_path", "config_path", "config"), location)
        entry["actor_path"] = _import_path(entry["actor_path"], f"{location}.actor_path")
        entry["config_path"] = _import_path(entry["config_path"], f"{location}.config_path")
        if not isinstance(entry["config"], dict):
            raise TypeError(f"{location}.config must be a mapping")
        _artifacts(entry, location)


# Client 的市场规则完全由对应 adapter 解释。
def normalize_live_clients(settings: dict[str, Any]) -> None:
    for role in ("data", "exec"):
        clients = settings["node"][role]["clients"]
        if not isinstance(clients, dict):
            raise TypeError(f"node.{role}.clients must be a mapping")
        ids: set[str] = set()
        for name, cfg in clients.items():
            location = f"node.{role}.clients.{name}"
            if not isinstance(cfg, dict):
                raise TypeError(f"{location} must be a mapping")
            _required(cfg, ("enabled", "adapter", "client_id"), location)
            if not isinstance(cfg["enabled"], bool):
                raise TypeError(f"{location}.enabled must be bool")
            if not cfg["enabled"]:
                continue
            client_id = str(cfg["client_id"])
            if client_id in ids:
                raise ValueError(f"duplicate {role} client_id: {client_id}")
            ids.add(client_id)
            importlib.import_module(f"adapters.{cfg['adapter']}").normalize_client(cfg)


def normalize_settings(settings: dict[str, Any]) -> None:
    normalize_strategies(settings)
    normalize_actors(settings)
    normalize_live_clients(settings)


def config_names() -> list[str]:
    return sorted(
        path.name
        for path in STRATEGIES_DIR.iterdir()
        if path.is_dir() and (path / LIVE_CONFIG_FILENAME).is_file()
    )


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


# 加载 global 与单个策略 set，并立即转换为唯一有效 schema。
def load_settings(config_name: str) -> dict[str, Any]:
    if config_name not in config_names():
        raise ValueError(f"Unknown live strategy: {config_name}")
    path = STRATEGIES_DIR / config_name / LIVE_CONFIG_FILENAME
    with GLOBAL_CONFIG_PATH.open("r", encoding="utf-8") as stream:
        global_settings = yaml.safe_load(stream)
    with path.open("r", encoding="utf-8") as stream:
        strategy_settings = yaml.safe_load(stream)
    settings = deep_merge(global_settings, strategy_settings)
    settings["project"]["config_name"] = config_name
    settings["project"]["config_path"] = str(path)
    settings["project"]["strategy_dir"] = str(path.parent)
    normalize_settings(settings)
    return settings


# Windows 使用本机代理；Linux 运行时不读取 Windows 代理地址。
def proxy_url() -> str | None:
    if os.name != "nt":
        return None
    return os.environ.get("PROXY_URL")
