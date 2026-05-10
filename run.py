from __future__ import annotations

import os
import sys
from pathlib import Path

import backtest
import live
from utils.arguments import MODE_OPTIONS
from utils.arguments import RUN_MODES


def config_names() -> list[str]:
    config_dir = Path(__file__).resolve().parent / "config"
    return sorted(path.stem for path in config_dir.glob("*.yaml") if path.stem != "global")


def clear_screen() -> None:
    os.system("cls")


# 读取 Windows 控制台方向键和回车。
def read_key() -> str:
    import msvcrt

    key = msvcrt.getch()

    if key == b"\r":
        return "enter"

    if key in (b"\x00", b"\xe0"):
        direction = msvcrt.getch()
        if direction == b"H":
            return "up"
        if direction == b"P":
            return "down"

    return ""


def render_menu(title: str, options: list[str], selected: int) -> None:
    clear_screen()

    print(title)
    print()

    for index, option in enumerate(options):
        prefix = "> " if index == selected else "  "
        print(f"{prefix}{option}")


def choose(title: str, options: list[str]) -> str:
    if not options:
        raise ValueError(f"No options for: {title}")

    selected = 0

    while True:
        render_menu(title, options, selected)

        key = read_key()

        if key == "enter":
            return options[selected]

        if key == "up":
            selected = (selected - 1) % len(options)

        elif key == "down":
            selected = (selected + 1) % len(options)


def parse_mode(raw: str) -> str:
    if raw not in RUN_MODES:
        raise ValueError(f"mode must be one of: {', '.join(RUN_MODES)}")
    return raw


def run(config_name: str, mode: str) -> None:
    if mode == "backtest":
        backtest.main(config_name)
    else:
        live.main(config_name, mode=mode)


def main() -> None:
    if len(sys.argv) == 3:
        run(sys.argv[1], parse_mode(sys.argv[2]))
        return

    if len(sys.argv) != 1:
        raise ValueError("Usage: python run.py [config_name] [backtest|testnet|live]")

    if not sys.stdin.isatty():
        raise RuntimeError("交互模式需要真实终端；非终端环境请用 python run.py 配置名 模式")

    selected_config = choose("请选择配置：", config_names())
    selected_label = choose("请选择模式：", [label for label, _mode in MODE_OPTIONS])
    mode = dict(MODE_OPTIONS)[selected_label]
    if mode == "live":
        input(f"确认运行实盘 配置：{selected_config}，回车继续：")

    run(selected_config, mode)


if __name__ == "__main__":
    main()
