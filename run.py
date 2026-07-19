from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

from utils.config import config_names
from utils.config import has_config
from utils.config import load_settings
from utils.constants import MODE_OPTIONS
from utils.constants import PROJECT_ROOT
from utils.constants import RUN_MODES
from utils.constants import STRATEGIES_DIR
from utils.constants import SUMMARY_FILE
from utils.summary import print_saved_summary


MENU_LAUNCH = "启动"
MENU_RUNNING = "运行中"
MENU_SUMMARY = "总结"


def clear_screen() -> None:
    os.system("cls" if os.name == "nt" else "clear")


# 读取跨平台方向键，平台专用库只在对应分支内导入。
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
            if direction == b"K":
                return "left"
            if direction == b"M":
                return "right"
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
        if key == b"\x1b":
            ready, _, _ = select.select([fd], [], [], 0.2)
            if not ready:
                return "back"
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
    print("↑/↓/←/→ 选择，Enter 确认，Esc 返回")


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


def render_main_menu(columns: list[tuple[str, list[str]]], col: int, row: int) -> None:
    clear_screen()
    print("请选择配置：")
    print()
    width = 38
    print("".join(f"{name:<{width}}" for name, _options in columns))
    print("".join(f"{'-' * 24:<{width}}" for _name, _options in columns))
    count = max(len(options) for _name, options in columns)
    for index in range(count):
        print("".join(f"{menu_cell(options, cell_col, index, col, row):<{width}}" for cell_col, (_name, options) in enumerate(columns)))
    print()
    print("↑/↓ 选择，←/→ 切换列，Enter 确认，Esc 返回")


def menu_cell(options: list[str], cell_col: int, index: int, col: int, row: int) -> str:
    if index >= len(options):
        return ""
    prefix = "> " if col == cell_col and row == index else "  "
    return f"{prefix}{options[index]}"


def choose_main(launch: list[str], running: list[str], summaries: list[str]) -> tuple[str, str] | None:
    if not launch and not running and not summaries:
        raise ValueError("No main menu options")
    columns = [(MENU_LAUNCH, launch), (MENU_RUNNING, running), (MENU_SUMMARY, summaries)]
    col = 1 if running else 0
    if not columns[col][1]:
        col = next(index for index, (_name, options) in enumerate(columns) if options)
    row = 0
    while True:
        render_main_menu(columns, col, row)
        key = read_key()
        if key == "back":
            return None
        if key == "enter":
            clear_screen()
            return columns[col][0], columns[col][1][row]
        if key in {"left", "right"}:
            step = -1 if key == "left" else 1
            for offset in range(1, len(columns) + 1):
                next_col = (col + step * offset) % len(columns)
                if columns[next_col][1]:
                    col = next_col
                    row = min(row, len(columns[col][1]) - 1)
                    break
        elif key == "up":
            row = (row - 1) % len(columns[col][1])
        elif key == "down":
            row = (row + 1) % len(columns[col][1])


def parse_mode(raw: str) -> str:
    if raw not in RUN_MODES:
        raise ValueError(f"mode must be one of: {', '.join(RUN_MODES)}")
    return raw


def parse_style(raw: str) -> str:
    aliases = {
        "background": "background",
        "bg": "background",
        "后台": "background",
        "foreground": "foreground",
        "fg": "foreground",
        "前台": "foreground",
    }
    if raw not in aliases:
        raise ValueError("style must be one of: background|bg|后台|foreground|fg|前台")
    return aliases[raw]


def run(config_name: str, mode: str) -> None:
    if mode == "backtest":
        import backtest

        started = time.perf_counter()
        result = backtest.main(config_name)
        elapsed = result.get("total_elapsed_sec") if isinstance(result, dict) else None
        if elapsed is None:
            elapsed = time.perf_counter() - started
        print(f"回测完成，耗时：{format_duration(elapsed)}")
    else:
        import live

        live.main(config_name, mode=mode)


def show_backtest_running(config_name: str) -> None:
    if os.name == "nt":
        clear_screen()


def tmux_available() -> bool:
    return os.name != "nt" and shutil.which("tmux") is not None


def tmux_sessions() -> list[str]:
    if not tmux_available():
        return []
    result = subprocess.run(
        ["tmux", "list-sessions", "-F", "#{session_name}"],
        cwd=PROJECT_ROOT,
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
    folder = STRATEGIES_DIR / strategy
    if not folder.is_dir() or not has_config(folder):
        raise RuntimeError(f"找不到策略目录：{strategy}")
    return folder


def live_report_dir(folder: Path, session_name: str) -> Path:
    session_time = session_name.rsplit("-", 1)[-1]
    if not session_time.isdigit() or len(session_time) != 14:
        raise RuntimeError(f"运行 session 名格式不正确：{session_name}")
    report_dir = folder / "report" / f"live-{session_time}"
    if not report_dir.is_dir():
        raise RuntimeError(f"没有找到 session 对应的 report：{session_name}")
    return report_dir


def run_monitor(session_name: str) -> None:
    strategy = session_strategy(session_name)
    folder = strategy_dir(strategy)
    monitor = folder / "monitor.py"
    if not monitor.exists():
        raise RuntimeError(f"当前策略没有 monitor.py：{folder}")
    report_dir = live_report_dir(folder, session_name)
    os.chdir(PROJECT_ROOT)
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
        ["tmux", "new-session", "-d", "-s", session_name, "-c", str(PROJECT_ROOT), shell_command],
        check=True,
    )
    pid = subprocess.check_output(
        ["tmux", "display-message", "-p", "-t", session_name, "#{pane_pid}"],
        cwd=PROJECT_ROOT,
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


def format_duration(seconds: float) -> str:
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    if seconds < 60:
        return f"{seconds:.2f}s"
    minutes, rest = divmod(seconds, 60)
    return f"{int(minutes)}m{rest:.1f}s"


def shlex_quote(value: str) -> str:
    import shlex

    return shlex.quote(value)


def report_dirs() -> list[Path]:
    dirs: list[Path] = []
    for root in STRATEGIES_DIR.glob("*/report"):
        dirs.extend(summary.parent for summary in root.glob(f"**/{SUMMARY_FILE}"))
    return sorted(dirs, key=lambda path: path.stat().st_mtime, reverse=True)


def report_label(report_dir: Path) -> str:
    strategy = report_dir.relative_to(STRATEGIES_DIR).parts[0]
    return f"{strategy}-{report_dir.name}"


def report_options(limit: int = 10) -> list[str]:
    options = []
    for path in report_dirs():
        options.append(report_label(path))
        if len(options) >= limit:
            break
    return options


def report_by_label(label: str) -> Path:
    for report_dir in report_dirs():
        if report_label(report_dir) == label:
            return report_dir
    raise RuntimeError(f"找不到报告：{label}")


def latest_report_strategy_dir() -> Path | None:
    dirs = report_dirs()
    if not dirs:
        return None
    strategy = dirs[0].relative_to(STRATEGIES_DIR).parts[0]
    return STRATEGIES_DIR / strategy


# 主菜单只把最新 report 所属策略提前，其余配置保持字母顺序，避免旧报告导致全量重排。
def menu_config_names(mode: str | None = None) -> list[str]:
    names = config_names(mode)
    latest_dir = latest_report_strategy_dir()
    if latest_dir is None:
        return names
    top = [name for name in names if STRATEGIES_DIR / name == latest_dir]
    return top + [name for name in names if name not in top]


def mode_options(config_name: str) -> list[tuple[str, str]]:
    folder = STRATEGIES_DIR / config_name
    return [(label, mode) for label, mode in MODE_OPTIONS if has_config(folder, mode)]


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
    print(f"总结：{summary.relative_to(PROJECT_ROOT)}")
    print()
    print_saved_summary(summary)
    if wait:
        wait_key()


def show_report(report_dir: Path, wait: bool = True, clear: bool = True) -> None:
    if clear:
        clear_screen()
    summary = report_dir / SUMMARY_FILE
    if not summary.exists():
        print(f"未找到：{summary.relative_to(PROJECT_ROOT)}")
        if wait:
            wait_key()
        return
    print(f"总结：{summary.relative_to(PROJECT_ROOT)}")
    print()
    print_saved_summary(summary)
    if wait:
        wait_key()


def wait_key() -> None:
    if not sys.stdin.isatty():
        return
    print()
    print("按任意键返回")
    read_key()


def run_interactive() -> None:
    selected_config = None
    while True:
        if selected_config is None:
            launch_mode = None if os.name == "nt" else "live"
            selected = choose_main(menu_config_names(launch_mode), running_options(), report_options())
            if selected is None:
                return
            menu, value = selected
            if menu == MENU_SUMMARY:
                show_report(report_by_label(value), wait=False, clear=False)
                return
            if menu == MENU_RUNNING:
                run_monitor(value.removeprefix("运行中："))
                return
            selected_config = value

        if os.name == "nt":
            options = mode_options(selected_config)
            selected_label = choose(
                f"配置：{selected_config}\n请选择模式：",
                [label for label, _mode in options],
            )
            if selected_label is None:
                selected_config = None
                continue
            mode = dict(options)[selected_label]
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

    if len(sys.argv) == 4:
        config_name = sys.argv[1]
        mode = parse_mode(sys.argv[2])
        style = parse_style(sys.argv[3])
        if style == "background":
            run_background(config_name, mode)
        else:
            if mode == "backtest":
                show_backtest_running(config_name)
            run(config_name, mode)
        return

    if len(sys.argv) != 1:
        raise ValueError("Usage: python run.py [config_name] [backtest|testnet|live] [background|foreground]")

    if not sys.stdin.isatty():
        raise RuntimeError("交互模式需要真实终端；非终端环境请用 python run.py 配置名 模式")

    run_interactive()


if __name__ == "__main__":
    main()
