from __future__ import annotations

import msvcrt
import os
import sys
from pathlib import Path

import backtest
import live


MODE_OPTIONS = [
    ("模拟盘", "testnet"),("回测", "backtest"),("实盘", "live"),
]



# 返回 config 目录下可运行的 set 名称。
def config_names() -> list[str]:
    config_dir = Path(__file__).resolve().parent / "config"
    return sorted(path.stem for path in config_dir.glob("*.yaml") if path.stem != "global")


# 清屏后渲染一个简单的上下键选择菜单。
def render_menu(title: str, options: list[str], selected: int) -> None:
    os.system("cls")
    print(title)
    print()
    for index, option in enumerate(options):
        prefix = "> " if index == selected else "  "
        print(f"{prefix}{option}")


# 使用上下键和回车选择一个选项。
def choose(title: str, options: list[str]) -> str:
    selected = 0
    while True:
        render_menu(title, options, selected)
        key = msvcrt.getch()
        if key == b"\r":
            return options[selected]
        if key == b"\xe0":
            direction = msvcrt.getch()
            if direction == b"H":
                selected = (selected - 1) % len(options)
            elif direction == b"P":
                selected = (selected + 1) % len(options)


# 把命令行模式名转换成内部模式。
def parse_mode(raw: str) -> str:
    if raw not in ("backtest", "testnet", "live"):
        raise ValueError("mode must be one of: backtest, testnet, live")
    return raw


# 根据模式运行回测、模拟盘或实盘。
def run(config_name: str, mode: str) -> None:
    if mode == "live":
        input(f"确认运行实盘 配置：{config_name}，回车继续：")
    if mode == "backtest":
        backtest.main(config_name)
    else:
        live.main(config_name, mode=mode)


# 主入口：无参数走交互选择，有参数直接运行。
def main() -> None:
    if len(sys.argv) == 3:
        run(sys.argv[1], parse_mode(sys.argv[2]))
        return
    if len(sys.argv) != 1:
        raise ValueError("Usage: python run.py [config_name] [backtest|testnet|live]")

    selected_config = choose("请选择配置：", config_names())
    selected_label = choose("请选择模式：", [label for label, _mode in MODE_OPTIONS])
    mode = dict(MODE_OPTIONS)[selected_label]
    run(selected_config, mode)


if __name__ == "__main__":
    main()
