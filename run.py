from __future__ import annotations

import argparse
import json
import re
import shlex
import shutil
import subprocess
import sys
from datetime import datetime

from utils.config import config_names, load_settings
from utils.constants import LOCAL_TZ, PROJECT_ROOT, SUMMARY_FILE
from utils.runtime_setup import claim_run, prepare_run_dir, reports_root


def active_sessions():
    if shutil.which("tmux") is None:
        return []
    result = subprocess.run(
        ["tmux", "list-sessions", "-F", "#{session_name}"],
        capture_output=True, text=True,
    )
    if result.returncode:
        if "no server running" in result.stderr or "No such file or directory" in result.stderr:
            return []
        raise RuntimeError(result.stderr.strip())
    sessions = []
    names = set(config_names())
    for line in result.stdout.splitlines():
        match = re.fullmatch(r"(.+)-(\d{14})", line)
        if match and match[1] in names:
            sessions.append({"strategy": match[1], "session": line, "started_at": match[2]})
    return sorted(sessions, key=lambda row: row["session"])


def start_background(strategy):
    if shutil.which("tmux") is None:
        raise RuntimeError("Background mode requires tmux")
    if any(row["strategy"] == strategy for row in active_sessions()):
        raise RuntimeError(f"Strategy already running: {strategy}")
    started_at = datetime.now(LOCAL_TZ).strftime("%Y%m%d%H%M%S")
    session = f"{strategy}-{started_at}"
    folder = prepare_run_dir(claim_run(load_settings(strategy), started_at))
    log = folder / "tmux.log"
    command = shlex.join([
        "env", f"NT_RUN_STARTED_AT={started_at}", sys.executable, "-u", "run.py", "start", strategy,
    ])
    subprocess.run(
        ["tmux", "new-session", "-d", "-s", session, "-c", str(PROJECT_ROOT),
         f"exec {command} >> {shlex.quote(str(log))} 2>&1"],
        check=True,
    )
    print(json.dumps({"strategy": strategy, "session": session, "report_dir": str(folder)}))


def stop_background(strategy):
    sessions = [row for row in active_sessions() if row["strategy"] == strategy]
    if len(sessions) != 1:
        raise RuntimeError(f"Expected one tmux session for {strategy}, found {len(sessions)}")
    session = sessions[0]["session"]
    subprocess.run(["tmux", "send-keys", "-t", f"={session}:", "C-c"], check=True)
    print(json.dumps({"session": session, "status": "stop_requested"}))


def latest_report(strategy):
    root = reports_root(load_settings(strategy))
    reports = sorted(root.glob(f"live-*/{SUMMARY_FILE}"), reverse=True)
    if not reports:
        raise FileNotFoundError(f"No completed report for {strategy} in {root}")
    print(reports[0].read_text(encoding="utf-8"))


def main():
    parser = argparse.ArgumentParser(description="NT live strategy commands")
    commands = parser.add_subparsers(dest="command", required=True)
    names = config_names()
    start = commands.add_parser("start", help="Start a live strategy")
    start.add_argument("strategy", choices=names)
    start.add_argument("--background", action="store_true")
    commands.add_parser("list", help="List configured strategies")
    commands.add_parser("status", help="List running tmux strategy sessions")
    commands.add_parser("stop", help="Request a managed stop")
    commands.add_parser("report", help="Read the latest completed report as JSON")
    for command in ("stop", "report"):
        commands.choices[command].add_argument("strategy", choices=names)
    args = parser.parse_args()

    if args.command == "start":
        if args.background:
            start_background(args.strategy)
        else:
            import live

            live.main(args.strategy)
    elif args.command == "list":
        print(json.dumps(names))
    elif args.command == "status":
        print(json.dumps(active_sessions()))
    elif args.command == "stop":
        stop_background(args.strategy)
    else:
        latest_report(args.strategy)


if __name__ == "__main__":
    main()
