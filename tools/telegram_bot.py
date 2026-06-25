from __future__ import annotations

import html
import json
import os
import shutil
import subprocess
import time
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

from utils.config_loader import ROOT
from utils.config_loader import config_names
from utils.config_loader import config_path
from utils.config_loader import load_settings


BEIJING_TZ = timezone(timedelta(hours=8))
SNAPSHOT_COMMANDS = {"查询快照", "/snapshot", "/snap"}
MAX_MESSAGE = 3800


# 使用 Telegram Bot HTTP API，避免额外 SDK 依赖。
class TeleBot:
    def __init__(self, token: str):
        self.base = f"https://api.telegram.org/bot{token}"
        self.offset = 0

    def get_updates(self) -> list[dict[str, Any]]:
        response = requests.get(
            f"{self.base}/getUpdates",
            params={"offset": self.offset, "timeout": 25, "allowed_updates": json.dumps(["message"])},
            timeout=35,
        )
        response.raise_for_status()
        payload = response.json()
        if not payload.get("ok"):
            raise RuntimeError(str(payload))
        updates = payload.get("result", [])
        for update in updates:
            self.offset = max(self.offset, int(update["update_id"]) + 1)
        return updates

    def send_text(self, chat_id: str, text: str) -> None:
        for part in split_text(text):
            response = requests.post(
                f"{self.base}/sendMessage",
                json={
                    "chat_id": chat_id,
                    "text": f"<pre>{html.escape(part)}</pre>",
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=15,
            )
            response.raise_for_status()


def split_text(text: str) -> list[str]:
    if len(text) <= MAX_MESSAGE:
        return [text]
    parts: list[str] = []
    remaining = text
    while len(remaining) > MAX_MESSAGE:
        cut = remaining.rfind("\n", 0, MAX_MESSAGE)
        if cut <= 0:
            cut = MAX_MESSAGE
        parts.append(remaining[:cut])
        remaining = remaining[cut:].lstrip("\n")
    if remaining:
        parts.append(remaining)
    return parts


def tmux_sessions() -> list[str]:
    if os.name == "nt" or shutil.which("tmux") is None:
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


def strategy_dirs() -> dict[str, Path]:
    rows: dict[str, Path] = {}
    for config in config_names():
        rows[strategy_name(config)] = config_path(config).parent
    return rows


def running_reports() -> list[tuple[str, Path]]:
    dirs = strategy_dirs()
    reports: list[tuple[str, Path]] = []
    for session in tmux_sessions():
        strategy = next((name for name in dirs if session.startswith(f"{name}-")), None)
        if strategy is None:
            continue
        report = session_report(dirs[strategy], session)
        if report is not None:
            reports.append((session, report))
    return reports


def session_report(folder: Path, session: str) -> Path | None:
    root = folder / "report"
    if not root.exists():
        return None
    reports = sorted(root.glob("live-*"), key=lambda path: path.stat().st_mtime, reverse=True)
    session_time = session.rsplit("-", 1)[-1]
    if not session_time.isdigit():
        return reports[0] if reports else None
    after_start = [path for path in reports if path.name.removeprefix("live-") >= session_time]
    return sorted(after_start, key=lambda path: path.name)[0] if after_start else None


def latest_snapshot_reports() -> list[tuple[str, Path]]:
    reports = []
    for root in (ROOT / "strategies").glob("*/report"):
        for report in root.glob("live-*"):
            if list(report.glob("*_snapshot.json")):
                reports.append((report.parent.parent.name, report))
    reports.sort(key=lambda row: row[1].stat().st_mtime, reverse=True)
    return reports[:1]


def snapshot_files(report: Path) -> list[Path]:
    return sorted(report.glob("*_snapshot.json"), key=lambda path: path.stat().st_mtime, reverse=True)


def snapshot_text() -> str:
    reports = running_reports()
    title = "运行中快照"
    if not reports:
        reports = latest_snapshot_reports()
        title = "最近快照"
    if not reports:
        return "没有找到运行中的 node 或 snapshot.json"

    blocks = [f"{title} | 北京时间 {datetime.now(tz=BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')}"]
    for name, report in reports:
        files = snapshot_files(report)
        if not files:
            blocks.append(f"\n{name}\n{report.relative_to(ROOT)}\n没有 snapshot.json")
            continue
        for path in files:
            payload = json.loads(path.read_text(encoding="utf-8"))
            blocks.append(format_snapshot(name, report, path, payload))
    return "\n\n".join(blocks)


def format_snapshot(name: str, report: Path, path: Path, payload: dict[str, Any]) -> str:
    mtime = datetime.fromtimestamp(path.stat().st_mtime, BEIJING_TZ).strftime("%m-%d %H:%M:%S")
    lines = [
        "",
        f"{name}",
        f"report: {report.relative_to(ROOT)}",
        f"snapshot: {mtime}",
    ]
    summary = payload.get("summary") or {}
    if summary:
        lines.extend(["", "Summary", row_line(["asset", "inv", "realU", "unrlbps", "totalbps"], [10, 5, 8, 8, 8])])
        for asset, row in summary.items():
            lines.append(
                row_line(
                    [
                        str(asset),
                        str(row.get("inventory", "-")),
                        str(row.get("realized_usdt", "-")),
                        str(row.get("unrealized_bps", "-")),
                        str(row.get("total_bps", "-")),
                    ],
                    [10, 5, 8, 8, 8],
                ),
            )

    rows = payload.get("rows") or []
    if rows:
        lines.extend(["", "Market", row_line(["asset", "state", "inv", "edge", "mean", "std"], [10, 10, 5, 8, 8, 7])])
        for row in rows:
            lines.append(
                row_line(
                    [
                        str(row.get("asset", "-")),
                        side_name(str(row.get("state", "-"))),
                        str(row.get("inventory", "-")),
                        str(row.get("edge", "-")),
                        str(row.get("mean", "-")),
                        str(row.get("std", "-")),
                    ],
                    [10, 10, 5, 8, 8, 7],
                ),
            )

    actions = payload.get("action_rows") or []
    if actions:
        lines.extend(["", "Actions", row_line(["time", "asset", "act", "qty", "sig", "slip", "inv"], [12, 9, 6, 5, 7, 7, 7])])
        for row in reversed(actions[-8:]):
            action = side_name(str(row.get("edge_side", row.get("action", "-"))))
            lines.append(
                row_line(
                    [
                        str(row.get("time", "-")),
                        str(row.get("asset", "-")),
                        action,
                        str(row.get("qty", "-")),
                        str(row.get("signal_edge", "-")),
                        str(row.get("edge_slippage", "-")),
                        str(row.get("inventory", "-")),
                    ],
                    [12, 9, 6, 5, 7, 7, 7],
                ),
            )
    return "\n".join(lines)


def row_line(values: list[str], widths: list[int]) -> str:
    return " ".join(fit(value, width) for value, width in zip(values, widths, strict=True))


def fit(value: str, width: int) -> str:
    text = value.replace("\n", " ")
    if len(text) > width:
        text = text[: max(1, width - 1)] + "…"
    return text.ljust(width)


def side_name(value: str) -> str:
    if value == "long_edge":
        return "long"
    if value == "short_edge":
        return "short"
    return value


def handle_message(bot: TeleBot, message: dict[str, Any], allowed_chat: str | None) -> None:
    chat_id = str(message["chat"]["id"])
    if allowed_chat and chat_id != allowed_chat:
        return
    text = str(message.get("text", "")).strip()
    command = text.split(maxsplit=1)[0].lower() if text.startswith("/") else text
    if command.startswith("/") and "@" in command:
        command = command.split("@", 1)[0]
    if command in SNAPSHOT_COMMANDS:
        bot.send_text(chat_id, snapshot_text())
    elif command in {"/start", "/help"}:
        bot.send_text(chat_id, "命令：查询快照 /snapshot /snap")


def main() -> None:
    load_dotenv(ROOT / ".env")
    token = os.environ["TELEGRAM_BOT_TOKEN"]
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    bot = TeleBot(token)
    if chat_id:
        bot.send_text(chat_id, "bot已启动")
    while True:
        try:
            for update in bot.get_updates():
                message = update.get("message")
                if message:
                    handle_message(bot, message, chat_id)
        except KeyboardInterrupt:
            return
        except Exception as exc:
            print(f"telegram_bot_error {type(exc).__name__}: {exc}", flush=True)
            time.sleep(5)


if __name__ == "__main__":
    main()
