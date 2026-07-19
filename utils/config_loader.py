from __future__ import annotations

import importlib
import os
from pathlib import Path
from typing import Any

import yaml

from utils.arguments import DEFAULT_CONFIG_NAME


ROOT = Path(__file__).resolve().parent.parent
STRATEGIES_DIR = ROOT / "strategies"
GLOBAL_CONFIG_PATH = STRATEGIES_DIR / "global.yaml"
CONFIG_FILES = {
    "backtest": "backtest_config.yaml",
    "live": "live_config.yaml",
    "testnet": "live_config.yaml",
}


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


def normalize_batch(entry: dict[str, Any], location: str) -> None:
    batch = entry.get("batch")
    if batch is None:
        return
    if not isinstance(batch, dict):
        raise TypeError(f"{location}.batch must be a mapping")
    _only(batch, {"cases", "grid"}, f"{location}.batch")
    cases = batch.get("cases", [{}])
    if not isinstance(cases, list) or not cases:
        raise ValueError(f"{location}.batch.cases must be a non-empty list")
    if any(not isinstance(case, dict) for case in cases):
        raise TypeError(f"{location}.batch.cases entries must be mappings")
    grid = batch.get("grid", {})
    if not isinstance(grid, dict):
        raise TypeError(f"{location}.batch.grid must be a mapping")
    for name, values in grid.items():
        if not isinstance(values, list) or not values:
            raise ValueError(f"{location}.batch.grid.{name} must be a non-empty list")


# 策略只接受 NT 原生 import path、config 和可选批量参数。
def normalize_strategies(settings: dict[str, Any], mode: str) -> None:
    defaults = settings.pop("strategy_defaults")
    if not isinstance(defaults, dict):
        raise TypeError("strategy_defaults must be a mapping")
    strategies = settings["strategy"]
    if not isinstance(strategies, dict) or not strategies:
        raise ValueError("strategy must contain at least one instance")

    enabled = 0
    allowed = {"enabled", "strategy_path", "config_path", "config", "artifacts"}
    if mode == "backtest":
        allowed.add("batch")
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
        if mode == "backtest":
            normalize_batch(entry, location)
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


# Backtest venue 同时拥有市场、合成 instrument 和撮合账户配置。
def normalize_backtest(settings: dict[str, Any]) -> None:
    backtest = settings["backtest"]
    venues = backtest["venues"]
    if not isinstance(venues, dict) or not venues:
        raise ValueError("backtest.venues must contain at least one venue")
    shared_instrument = backtest.pop("instrument")
    if not isinstance(shared_instrument, dict):
        raise TypeError("backtest.instrument must be a mapping")

    venue_ids: set[str] = set()
    for name, venue in venues.items():
        location = f"backtest.venues.{name}"
        if not isinstance(venue, dict):
            raise TypeError(f"{location} must be a mapping")
        _required(
            venue,
            (
                "adapter",
                "venue",
                "instrument_kind",
                "markets",
                "oms_type",
                "account_type",
                "starting_balances",
            ),
            location,
        )
        venue_id = str(venue["venue"])
        if venue_id in venue_ids:
            raise ValueError(f"duplicate backtest venue: {venue_id}")
        venue_ids.add(venue_id)
        venue["instrument"] = {**shared_instrument, **venue.get("instrument", {})}
        balances = venue["starting_balances"]
        if not isinstance(balances, list) or not balances:
            raise ValueError(f"{location}.starting_balances must be a non-empty list")
        importlib.import_module(f"adapters.{venue['adapter']}").normalize_client(venue)
        if venue["markets_all"] or not venue["markets"]:
            raise ValueError(f"{location}.markets must explicitly list at least one market")

    datasets = backtest["datasets"]
    if not isinstance(datasets, list) or not datasets:
        raise ValueError("backtest.datasets must be a non-empty list")
    for index, dataset in enumerate(datasets):
        location = f"backtest.datasets[{index}]"
        if not isinstance(dataset, dict):
            raise TypeError(f"{location} must be a mapping")
        _required(dataset, ("type", "path"), location)
        data_type = dataset["type"]
        allowed = {"type", "path"}
        if data_type == "bars":
            allowed.update(("instrument_id", "bar_type"))
            _required(dataset, ("instrument_id", "bar_type"), location)
        elif data_type == "trade_tick_catalog":
            allowed.add("venue")
            _required(dataset, ("venue",), location)
        elif data_type not in {"quote_tick_objects", "quote_ticks", "trade_ticks"}:
            raise ValueError(f"{location}.type is unsupported: {data_type}")
        _only(dataset, allowed, location)
        relative = Path(dataset["path"])
        if relative.is_absolute():
            raise ValueError(f"{location}.path must be relative")
        (ROOT / relative).resolve().relative_to(ROOT.resolve())

    settings.pop("node")


def normalize_settings(settings: dict[str, Any], mode: str) -> None:
    if mode not in CONFIG_FILES:
        raise ValueError(f"Unsupported mode: {mode}")
    normalize_strategies(settings, mode)
    if mode == "backtest":
        normalize_backtest(settings)
        return
    settings.pop("backtest")
    normalize_actors(settings)
    normalize_live_clients(settings)


def config_names(mode: str | None = None) -> list[str]:
    return sorted(
        path.name
        for path in STRATEGIES_DIR.iterdir()
        if path.is_dir() and has_config(path, mode)
    )


def has_config(path: Path, mode: str | None = None) -> bool:
    if mode is not None:
        return (path / config_filename(mode)).exists()
    return any((path / filename).exists() for filename in set(CONFIG_FILES.values()))


def config_filename(mode: str) -> str:
    if mode not in CONFIG_FILES:
        raise ValueError(f"Unsupported mode: {mode}")
    return CONFIG_FILES[mode]


def config_path(config_name: str, mode: str) -> Path:
    path = STRATEGIES_DIR / config_name / config_filename(mode)
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path.relative_to(ROOT)}")
    return path


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


# 加载 global 与单个策略 set，并立即转换为唯一有效 schema。
def load_settings(config_name: str | None = None, mode: str = "live") -> dict[str, Any]:
    name = config_name or DEFAULT_CONFIG_NAME
    path = config_path(name, mode)
    with GLOBAL_CONFIG_PATH.open("r", encoding="utf-8") as stream:
        global_settings = yaml.safe_load(stream)
    with path.open("r", encoding="utf-8") as stream:
        strategy_settings = yaml.safe_load(stream)
    settings = deep_merge(global_settings, strategy_settings)
    settings["mode"] = mode
    settings["project"]["config_name"] = name
    settings["project"]["config_path"] = str(path)
    settings["project"]["strategy_dir"] = str(path.parent)
    normalize_settings(settings, mode)
    return settings


# Windows 使用本机代理；Linux 运行时不读取 Windows 代理地址。
def proxy_url() -> str | None:
    if os.name != "nt":
        return None
    return os.environ.get("PROXY_URL")
