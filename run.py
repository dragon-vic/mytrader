from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

from rich.columns import Columns
from rich.console import Console
from rich.table import Table

from utils.arguments import MODE_OPTIONS
from utils.arguments import RUN_MODES
from utils.arguments import SUMMARY_FILE
from utils.config_loader import ROOT
from utils.config_loader import config_names
from utils.config_loader import config_path
from utils.config_loader import load_settings


REPORT_OPTION = "查看总结"


def clear_screen() -> None:
    os.system("cls" if os.name == "nt" else "clear")


# 读取跨平台方向键，平台专用库只在对应分支内导入。
def read_key() -> str:
    if os.name == "nt":
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
            if direction == b"K":
                return "left"
            if direction == b"M":
                return "right"
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
        key = os.read(fd, 1)
        if key in (b"\r", b"\n"):
            return "enter"
        if key in (b"q", b"Q"):
            return "back"
        if key == b"\x1b":
            ready, _, _ = select.select([fd], [], [], 0.2)
            if not ready:
                return ""
            seq = os.read(fd, 1)
            ready, _, _ = select.select([fd], [], [], 0.05)
            if ready:
                seq += os.read(fd, 1)
            if seq == b"[A":
                return "up"
            if seq == b"[B":
                return "down"
            if seq == b"[D":
                return "left"
            if seq == b"[C":
                return "right"
            return ""
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
    print("↑/↓/←/→ 选择，Enter 确认，q 返回")


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
        if key in {"up", "left"}:
            selected = (selected - 1) % len(options)
        elif key in {"down", "right"}:
            selected = (selected + 1) % len(options)


def render_main_menu(left: list[str], right: list[str], col: int, row: int) -> None:
    clear_screen()
    print("请选择配置：")
    print()
    print(f"{'配置':<32}运行中 / 总结")
    print(f"{'-' * 24:<32}{'-' * 24}")
    count = max(len(left), len(right))
    for index in range(count):
        left_text = menu_cell(left, 0, index, col, row)
        right_text = menu_cell(right, 1, index, col, row)
        print(f"{left_text:<32}{right_text}")
    print()
    print("↑/↓ 选择，←/→ 切换列，Enter 确认，q 返回")


def menu_cell(options: list[str], cell_col: int, index: int, col: int, row: int) -> str:
    if index >= len(options):
        return ""
    prefix = "> " if col == cell_col and row == index else "  "
    return f"{prefix}{options[index]}"


def choose_main(left: list[str], right: list[str]) -> str | None:
    if not left and not right:
        raise ValueError("No main menu options")
    columns = [left, right]
    col = 1 if len(right) > 1 else 0
    if not columns[col]:
        col = 1 - col
    row = 0
    while True:
        render_main_menu(left, right, col, row)
        key = read_key()
        if key == "back":
            return None
        if key == "enter":
            return columns[col][row]
        if key in {"left", "right"}:
            next_col = 0 if key == "left" else 1
            if columns[next_col]:
                col = next_col
                row = min(row, len(columns[col]) - 1)
        elif key == "up":
            row = (row - 1) % len(columns[col])
        elif key == "down":
            row = (row + 1) % len(columns[col])


def parse_mode(raw: str) -> str:
    if raw not in RUN_MODES:
        raise ValueError(f"mode must be one of: {', '.join(RUN_MODES)}")
    return raw


def run(config_name: str, mode: str) -> None:
    if mode == "backtest":
        import backtest

        backtest.main(config_name)
    else:
        import live

        live.main(config_name, mode=mode)


def show_backtest_running(config_name: str) -> None:
    if os.name != "nt":
        return
    clear_screen()
    print(f"配置：{config_name}")
    print("回测中...")
    sys.stdout.flush()


def tmux_available() -> bool:
    return os.name != "nt" and shutil.which("tmux") is not None


def tmux_sessions() -> list[str]:
    if not tmux_available():
        return []
    result = subprocess.run(
        ["tmux", "list-sessions", "-F", "#{session_name}"],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if result.returncode != 0:
        return []
    return sorted(line.strip() for line in result.stdout.splitlines() if line.strip())


def strategy_name(config_name: str) -> str:
    return config_name


def running_options() -> list[str]:
    strategies = {strategy_name(name) for name in config_names()}
    rows = []
    for session in tmux_sessions():
        if any(session.startswith(f"{name}-") for name in strategies):
            rows.append(f"运行中：{session}")
    return rows


def session_strategy(session_name: str) -> str:
    for name in {strategy_name(config) for config in config_names()}:
        if session_name.startswith(f"{name}-"):
            return name
    raise RuntimeError(f"无法识别运行中的策略：{session_name}")


def strategy_dir(strategy: str) -> Path:
    for config in config_names():
        if strategy_name(config) == strategy:
            return config_path(config).parent
    raise RuntimeError(f"找不到策略目录：{strategy}")


def live_report_dir(folder: Path, session_name: str) -> Path:
    report_root = folder / "report"
    reports = sorted(report_root.glob("live-*"), key=lambda path: path.stat().st_mtime, reverse=True)
    if not reports:
        raise RuntimeError(f"没有找到运行报告目录：{report_root}")
    session_time = session_name.rsplit("-", 1)[-1]
    if not session_time.isdigit() or len(session_time) != 14:
        raise RuntimeError(f"运行 session 名格式不正确：{session_name}")
    after_start = [path for path in reports if path.name.removeprefix("live-") >= session_time]
    if not after_start:
        raise RuntimeError(f"没有找到 session 对应的 report：{session_name}")
    return sorted(after_start, key=lambda path: path.name)[0]


def run_monitor(session_name: str) -> None:
    strategy = session_strategy(session_name)
    folder = strategy_dir(strategy)
    monitor = folder / "monitor.py"
    if not monitor.exists():
        raise RuntimeError(f"当前策略没有 monitor.py：{folder}")
    report_dir = live_report_dir(folder, session_name)
    os.chdir(ROOT)
    os.execv(sys.executable, [sys.executable, str(monitor), str(report_dir), session_name])


def ensure_strategy_not_running(config_name: str) -> None:
    if not tmux_available():
        return
    strategy = strategy_name(config_name)
    for session in tmux_sessions():
        if session.startswith(f"{strategy}-"):
            raise RuntimeError(
                f"策略已在运行：{session}\n"
                f"同一个策略默认只允许一个 node。请先 tmux attach -t {session} 后停止旧进程。",
            )


def run_background(config_name: str, mode: str) -> None:
    if not tmux_available():
        raise RuntimeError("后台运行需要 tmux，请先安装 tmux，或选择前台运行。")
    ensure_strategy_not_running(config_name)
    strategy = strategy_name(config_name)
    start_time = timestamp_name()
    session_name = f"{strategy}-{start_time}"
    report_dir = background_report_dir(config_name, mode, start_time)
    report_dir.mkdir(parents=True, exist_ok=True)
    tmux_log = report_dir / "tmux.log"
    command = (
        f"export NT_RUN_STARTED_AT={shlex_quote(start_time)}; "
        f"exec {shlex_quote(sys.executable)} -u run.py {shlex_quote(config_name)} {shlex_quote(mode)} "
        f">> {shlex_quote(str(tmux_log))} 2>&1"
    )
    shell_command = f"bash -lc {shlex_quote(command)}"
    subprocess.run(
        ["tmux", "new-session", "-d", "-s", session_name, "-c", str(ROOT), shell_command],
        check=True,
    )
    pid = subprocess.check_output(
        ["tmux", "display-message", "-p", "-t", session_name, "#{pane_pid}"],
        cwd=ROOT,
        text=True,
    ).strip()
    print(f"后台进程已启动：{config_name} {mode}")
    print(f"tmux会话：{session_name}")
    print(f"PID：{pid}")
    print(f"NT日志：{report_dir}/node.log")
    print(f"tmux日志：{tmux_log}")
    print("监控：重新运行 start，选择对应的运行中策略")
    print(f"停止：tmux attach -t {session_name} 后按 Ctrl+C")


# 后台运行先创建目录，保证 Python 启动失败也能留下 tmux.log。
def background_report_dir(config_name: str, mode: str, start_time: str) -> Path:
    settings = load_settings(config_name, mode=mode)
    root = Path(settings["reports"]["root"])
    if not root.is_absolute():
        root = Path(settings["project"]["strategy_dir"]) / root
    run_kind = "backtest" if mode == "backtest" else "live"
    return root / f"{run_kind}-{start_time}"


def timestamp_name() -> str:
    from datetime import datetime
    from zoneinfo import ZoneInfo

    return datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y%m%d%H%M%S")


def shlex_quote(value: str) -> str:
    import shlex

    return shlex.quote(value)


def report_dirs() -> list[Path]:
    dirs = []
    for root in (ROOT / "strategies").glob("*/report"):
        dirs.extend(path for path in root.iterdir() if path.is_dir())
    return sorted(dirs, key=lambda path: path.stat().st_mtime, reverse=True)


def latest_report_strategy_dir() -> Path | None:
    dirs = report_dirs()
    if not dirs:
        return None
    return dirs[0].parent.parent


# 主菜单只把最新 report 所属策略提前，其余配置保持字母顺序，避免旧报告导致全量重排。
def menu_config_names() -> list[str]:
    names = config_names()
    latest_dir = latest_report_strategy_dir()
    if latest_dir is None:
        return names
    top = [name for name in names if config_path(name).parent == latest_dir]
    return top + [name for name in names if name not in top]


def latest_summary() -> Path | None:
    for report_dir in report_dirs():
        summary = report_dir / SUMMARY_FILE
        if summary.exists():
            return summary
    return None


def show_latest_report(wait: bool = True, clear: bool = True) -> None:
    summary = latest_summary()
    if clear:
        clear_screen()
    if summary is None:
        print("没有找到已完成报告 summary.json")
        if wait:
            wait_key()
        return
    print(f"总结：{summary.relative_to(ROOT)}")
    print()
    print_summary_tables(summary)
    if wait:
        wait_key()


def wait_key() -> None:
    if not sys.stdin.isatty():
        return
    print()
    print("按任意键返回")
    read_key()


def print_summary_tables(summary: Path) -> None:
    sections = parse_summary_json(summary)
    if not sections:
        Console().print(summary.read_text(encoding="utf-8-sig"))
        return
    console = Console()
    top = [table for title, table in sections if title != "标的统计"]
    instruments = [table for title, table in sections if title == "标的统计"]
    if top:
        console.print(Columns(top, equal=True, expand=True))
    for table in instruments:
        console.print(table)


def parse_summary_json(summary: Path) -> list[tuple[str, Table]]:
    payload = json.loads(summary.read_text(encoding="utf-8-sig"))
    sections: list[tuple[str, Table]] = []
    for section in payload["sections"]:
        title = str(section["title"])
        table = Table(title=title)
        headers = [str(header) for header in section["headers"]]
        for index, header in enumerate(headers):
            table.add_column(header, justify="left" if index == 0 else "right")
        for row in section["rows"]:
            values = [str(value) for value in row]
            table.add_row(*values)
        sections.append((title, table))
    return sections


def run_interactive() -> None:
    selected_config = None
    while True:
        if selected_config is None:
            selected_config = choose_main(menu_config_names(), running_options() + [REPORT_OPTION])
            if selected_config is None:
                return
            if selected_config == REPORT_OPTION:
                show_latest_report(wait=False)
                return
            if selected_config.startswith("运行中："):
                run_monitor(selected_config.removeprefix("运行中："))
                return

        if os.name == "nt":
            selected_label = choose(
                f"配置：{selected_config}\n请选择模式：",
                [label for label, _mode in MODE_OPTIONS],
            )
            if selected_label is None:
                selected_config = None
                continue
            mode = dict(MODE_OPTIONS)[selected_label]
        else:
            mode = "live"

        if os.name != "nt":
            style = choose("请选择运行方式：", ["后台运行", "前台运行"])
            if style is None:
                continue
            if style == "后台运行":
                run_background(selected_config, mode)
                return

        if mode == "backtest":
            show_backtest_running(selected_config)
        run(selected_config, mode)
        return


def main() -> None:
    if len(sys.argv) == 2 and sys.argv[1] in {"report", "reports", "summary"}:
        show_latest_report(wait=False, clear=False)
        return

    if len(sys.argv) == 3:
        run(sys.argv[1], parse_mode(sys.argv[2]))
        return

    if len(sys.argv) != 1:
        raise ValueError("Usage: python run.py [config_name] [backtest|testnet|live]")

    if not sys.stdin.isatty():
        raise RuntimeError("交互模式需要真实终端；非终端环境请用 python run.py 配置名 模式")

    run_interactive()


if __name__ == "__main__":
    main()
