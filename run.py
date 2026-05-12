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
    os.system("cls" if os.name == "nt" else "clear")


# 读取跨平台方向键。
def read_key() -> str:
    if os.name == "nt":
        import msvcrt

        key = msvcrt.getch()
        if key == b"\r":
            return "enter"
        if key == b"\x1b":
            return "back"
        if key in (b"\x00", b"\xe0"):
            direction = msvcrt.getch()
            if direction == b"H":
                return "up"
            if direction == b"P":
                return "down"
        if key in (b"q", b"Q"):
            return "back"
        return ""

    import select
    import termios
    import tty

    fd = sys.stdin.fileno()
    old = termios.tcgetattr(fd)
    try:
        tty.setraw(fd)
        key = sys.stdin.read(1)
        if key in ("\r", "\n"):
            return "enter"
        if key in ("q", "Q"):
            return "back"
        if key == "\x1b":
            ready, _, _ = select.select([sys.stdin], [], [], 0.05)
            if not ready:
                return "back"
            seq = sys.stdin.read(2)
            if seq == "[A":
                return "up"
            if seq == "[B":
                return "down"
            return "back"
        return ""
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)


# 渲染方向键菜单。
def render_menu(title: str, options: list[str], selected: int) -> None:
    clear_screen()

    print(title)
    print()

    for index, option in enumerate(options):
        prefix = "> " if index == selected else "  "
        print(f"{prefix}{option}")

    print()
    print("↑/↓ 选择，Enter 确认，Esc/q 返回")


def choose(title: str, options: list[str]) -> str | None:
    if not options:
        raise ValueError(f"No options for: {title}")

    selected = 0
    while True:
        render_menu(title, options, selected)
        key = read_key()
        if key == "back":
            return None
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

    selected_config = None
    while True:
        if selected_config is None:
            selected_config = choose("请选择配置：", config_names())
            if selected_config is None:
                return

        selected_label = choose(
            f"配置：{selected_config}\n请选择模式：",
            [label for label, _mode in MODE_OPTIONS],
        )
        if selected_label is None:
            selected_config = None
            continue

        mode = dict(MODE_OPTIONS)[selected_label]
        if mode == "live":
            confirm = choose(
                f"确认运行实盘\n配置：{selected_config}",
                ["确认运行"],
            )
            if confirm is None:
                continue

        run(selected_config, mode)
        return


if __name__ == "__main__":
    main()
