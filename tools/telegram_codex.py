from __future__ import annotations

import html
import json
import os
import signal
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
os.environ['PATH'] = f"{Path.home() / '.local' / 'bin'}:{os.environ.get('PATH', '')}"
SESSION_FILE = Path(__file__).resolve().parent / ".telegram_codex_session"
MAX_MESSAGE = 3800
POLL_TIMEOUT = 5
TMUX_SESSION = "telegram-codex"
SYSTEM_PROMPT = """你是通过 Telegram 操作本机项目 /home/ubuntu/pycharm_nt 的 Codex。用户发来的每条 Telegram 消息都是真实任务指令，不是闲聊模拟。

默认使用中文，回答要简短、直接、可执行。除非用户要求解释，不要长篇分析。能直接做的事就直接做；需要运行命令或改文件时可以执行。你拥有当前项目的完整读写和运行权限，但不要改动与任务无关的文件。

当前项目运行在 Linux 的 ubuntu 用户下，项目路径是 /home/ubuntu/pycharm_nt，默认不要使用 root 用户路径或 Windows 路径，除非用户明确要求操作远端或其它环境。

重启当前 Telegram Codex bot 时，在 nt 环境运行 bot 脚本即可，它会自己检查/创建 telegram-codex tmux：cd /home/ubuntu/pycharm_nt && /home/ubuntu/miniconda/envs/nt/bin/python tools/telegram_codex.py

用户的命令默认都和当前项目内代码、配置、report、运行状态、git 状态或开发维护任务有关。除非用户明确指定外部主题，否则优先在当前项目上下文里理解和执行，不要泛泛回答。

这个 Telegram bot 会话的上下文和记忆应当作为独立上下文使用，只服务当前 bot 对话。不要让 bot 对话里的临时偏好、状态或任务污染其它对话、其它入口或项目持久规则；也不要假设其它对话里的临时上下文会影响当前 bot 会话。只有用户明确要求持久化到代码、配置、文档或 skill 时，才把信息写入项目文件。

最终回答必须使用 Telegram Bot API 支持的 HTML 格式。只使用这些标签：<b>、<i>、<u>、<s>、<code>、<pre>、<a href="...">。不要使用 Markdown，不要使用 table/div/style/h1/ul/ol/li/br 等 Telegram 不支持或不稳定的标签。普通文本里的 <、>、& 必须转义为 HTML 实体。

根据任务复杂度和回复长度设计返回 HTML 的结构。简单任务用一两句话即可，不要硬拆块；复杂任务可以用 <b>短标题</b> 分块，用空行分隔，用 <code>...</code> 标记路径、命令、数值或关键词，用 <pre>...</pre> 放多行代码、表格文本或日志摘录。

除非用户明确指定，不要主动执行长耗时、高 CPU、高内存、海量 IO、全仓库大规模计算或可能长期占用资源的任务。需要这类任务时，先说明你将采用较轻量的检查或给出建议。

不要在最终回答里倾倒完整日志。只汇报关键结果、改了什么、验证了什么、还有什么风险。路径、命令、数值要写清楚。"""


class Telegram:
    def __init__(self, token: str):
        self.base = f"https://api.telegram.org/bot{token}"
        self.offset = 0

    def updates(self) -> list[dict[str, Any]]:
        response = requests.get(
            f"{self.base}/getUpdates",
            params={"offset": self.offset, "timeout": POLL_TIMEOUT, "allowed_updates": json.dumps(["message"])},
            timeout=POLL_TIMEOUT + 10,
        )
        response.raise_for_status()
        payload = response.json()
        if not payload.get("ok"):
            raise RuntimeError(str(payload))
        updates = payload.get("result", [])
        for update in updates:
            self.offset = max(self.offset, int(update["update_id"]) + 1)
        return updates

    def send(self, chat_id: str, text: str) -> list[int]:
        message_ids: list[int] = []
        for part in split_text(text):
            try:
                message_ids.append(self._send_html(chat_id, part))
            except requests.HTTPError:
                message_ids.append(self._send_html(chat_id, f"<pre>{html.escape(part)}</pre>"))
        return message_ids

    def react(self, chat_id: str, message_id: int, emoji: str) -> None:
        try:
            response = requests.post(
                f"{self.base}/setMessageReaction",
                json={
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "reaction": [{"type": "emoji", "emoji": emoji}],
                },
                timeout=5,
            )
            if not response.ok:
                print(
                    f"telegram_reaction_error status={response.status_code} body={response.text[:200]}",
                    flush=True,
                )
        except requests.RequestException as exc:
            print(f"telegram_reaction_error {type(exc).__name__}", flush=True)

    def _send_html(self, chat_id: str, text: str) -> int:
        response = requests.post(
            f"{self.base}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=15,
        )
        response.raise_for_status()
        payload = response.json()
        return int((payload.get("result") or {}).get("message_id") or 0)


class CodexRunner:
    def __init__(self, bot: Telegram, session_file: Path):
        self.bot = bot
        self.session_file = session_file
        self.lock = threading.Lock()
        self.busy = False
        self.pending: list[tuple[int, str]] = []
        self.queued_ids: set[int] = set()
        self.proc: subprocess.Popen[str] | None = None
        self.cancel_requested = False

    def submit(self, chat_id: str, message_id: int, prompt: str) -> None:
        proc = None
        with self.lock:
            if self.busy:
                self.pending.append((message_id, prompt))
                self.queued_ids.add(message_id)
                self.cancel_requested = True
                proc = self.proc
                busy = True
            else:
                self.busy = True
                busy = False
        if busy:
            self.bot.react(chat_id, message_id, "⚡")
            stop_proc(proc)
            return
        thread = threading.Thread(target=self._run, args=(chat_id, [(message_id, prompt)]), daemon=True)
        thread.start()

    def _run(self, chat_id: str, batch: list[tuple[int, str]]) -> None:
        start = time.monotonic()
        while True:
            for message_id, _prompt in batch:
                if not self._is_queued(message_id):
                    self.bot.react(chat_id, message_id, "👀")
            try:
                result, usage = run_codex(merge_prompts(batch), self.session_file, self._set_proc, self._clear_proc)
            except CodexCancelled:
                batch = self._merge_pending(batch)
                continue
            except Exception as exc:
                batch = self._finish(batch)
                for message_id, _prompt in batch:
                    self.bot.react(chat_id, message_id, "😱")
                detail = str(exc) if isinstance(exc, RuntimeError) else f"{type(exc).__name__}: {exc}"
                text = html.escape(f"Codex 运行失败：{detail}")
                self.bot.send(chat_id, with_stats(text, start, None))
                return

            with self.lock:
                if self.pending:
                    batch.extend(self.pending)
                    self.pending = []
                    self.cancel_requested = False
                    continue
                self.busy = False
                self._clear_queued(batch)
            for message_id, _prompt in batch:
                self.bot.react(chat_id, message_id, "👌")
            self.bot.send(chat_id, with_stats(result or "Codex 没有返回内容", start, usage))
            return

    def _set_proc(self, proc: subprocess.Popen[str]) -> None:
        with self.lock:
            self.proc = proc
            should_cancel = self.cancel_requested
        if should_cancel:
            stop_proc(proc)

    def _clear_proc(self, proc: subprocess.Popen[str]) -> bool:
        with self.lock:
            cancelled = self.cancel_requested
            if self.proc is proc:
                self.proc = None
            return cancelled

    def _is_queued(self, message_id: int) -> bool:
        with self.lock:
            return message_id in self.queued_ids

    def _clear_queued(self, batch: list[tuple[int, str]]) -> None:
        for message_id, _prompt in batch:
            self.queued_ids.discard(message_id)

    def _merge_pending(self, batch: list[tuple[int, str]]) -> list[tuple[int, str]]:
        with self.lock:
            batch.extend(self.pending)
            self.pending = []
            self.cancel_requested = False
        return batch

    def _finish(self, batch: list[tuple[int, str]]) -> list[tuple[int, str]]:
        with self.lock:
            batch.extend(self.pending)
            self.busy = False
            self.pending = []
            self.queued_ids.clear()
            self.cancel_requested = False
            self.proc = None
        return batch


def split_text(text: str) -> list[str]:
    if len(text) <= MAX_MESSAGE:
        return [text]
    parts: list[str] = []
    rest = text
    while len(rest) > MAX_MESSAGE:
        idx = rest.rfind("\n", 0, MAX_MESSAGE)
        if idx <= 0:
            idx = MAX_MESSAGE
        parts.append(rest[:idx])
        rest = rest[idx:].lstrip("\n")
    if rest:
        parts.append(rest)
    return parts


def with_stats(text: str, start: float, usage: dict[str, Any] | None) -> str:
    elapsed = time.monotonic() - start
    token_text = ""
    if usage:
        input_tokens = int(usage.get("input_tokens") or 0)
        cached_tokens = int(usage.get("cached_input_tokens") or 0)
        output_tokens = int(usage.get("output_tokens") or 0)
        reasoning_tokens = int(usage.get("reasoning_output_tokens") or 0)
        uncached = max(0, input_tokens - cached_tokens) + output_tokens + reasoning_tokens
        token_text = f" | 非缓存:{format_k(uncached)}"
    return f"{text}\n\n<i>耗时:{elapsed:.1f}s{token_text}</i>"


def format_k(value: int) -> str:
    if value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    return f"{value / 1000:.1f}K"


class CodexCancelled(Exception):
    pass


def merge_prompts(batch: list[tuple[int, str]]) -> str:
    if len(batch) == 1:
        return batch[0][1]
    parts = ["用户在上一轮思考中连续发送了多条消息。请把它们作为同一个最新任务一起处理："]
    for index, (_message_id, prompt) in enumerate(batch, start=1):
        parts.append(f"消息 {index}:\n{prompt}")
    return "\n\n".join(parts)


def stop_proc(proc: subprocess.Popen[str] | None) -> None:
    if proc is None or proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
        proc.wait(timeout=3)
    except ProcessLookupError:
        return
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGKILL)
        proc.wait(timeout=3)


def run_codex(
    prompt: str,
    session_file: Path,
    set_proc: Any,
    clear_proc: Any,
) -> tuple[str, dict[str, Any] | None]:
    session_id = read_session(session_file)
    prompt = build_prompt(prompt)
    if session_id:
        cmd = [
            "codex",
            "exec",
            "resume",
            "--json",
            "--dangerously-bypass-approvals-and-sandbox",
            session_id,
            prompt,
        ]
    else:
        cmd = [
            "codex",
            "exec",
            "--json",
            "--sandbox",
            "danger-full-access",
            prompt,
        ]
    proc = subprocess.Popen(
        cmd,
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    set_proc(proc)
    stdout, stderr = proc.communicate()
    cancelled = clear_proc(proc)
    if cancelled:
        raise CodexCancelled()
    output, new_session, usage = parse_codex_output(stdout)
    if new_session and not session_id:
        session_file.parent.mkdir(parents=True, exist_ok=True)
        session_file.write_text(new_session, encoding="utf-8")
    if proc.returncode != 0:
        detail = stderr.strip() or output or f"exit code {proc.returncode}"
        raise RuntimeError(detail[-MAX_MESSAGE:])
    return output, usage


def build_prompt(prompt: str) -> str:
    return f"{SYSTEM_PROMPT}\n\n用户消息：\n{prompt}"


def read_session(path: Path) -> str | None:
    if not path.exists():
        return None
    text = path.read_text(encoding="utf-8").strip()
    return text or None


def parse_codex_output(stdout: str) -> tuple[str, str | None, dict[str, Any] | None]:
    session_id = None
    usage = None
    messages: list[str] = []
    errors: list[str] = []
    fallback: list[str] = []
    for line in stdout.splitlines():
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            fallback.append(line)
            continue
        if event.get("type") == "thread.started":
            session_id = str(event.get("thread_id") or "")
        if event.get("type") == "turn.completed":
            raw_usage = event.get("usage")
            if isinstance(raw_usage, dict):
                usage = raw_usage
        if event.get("type") == "error":
            message = event.get("message")
            if message:
                errors.append(str(message))
        if event.get("type") == "turn.failed":
            error = event.get("error")
            if isinstance(error, dict):
                message = error.get("message")
                if message:
                    errors.append(str(message))
        item = event.get("item") or {}
        if item.get("type") == "agent_message":
            text = item.get("text")
            if text:
                messages.append(str(text))
    if messages:
        return messages[-1], session_id, usage
    if errors:
        return errors[-1], session_id, usage
    return "\n".join(fallback).strip(), session_id, usage


def allowed(chat_id: str, allowed_chat: str | None) -> bool:
    return allowed_chat is None or chat_id == allowed_chat


def ensure_tmux() -> bool:
    if os.environ.get("TELEGRAM_CODEX_NO_TMUX") == "1":
        return True
    if shutil.which("tmux") is None:
        print("tmux_not_found running_foreground", flush=True)
        return True
    session = os.environ.get("TELEGRAM_CODEX_TMUX_SESSION", TMUX_SESSION)
    if os.environ.get("TMUX"):
        current = subprocess.run(
            ["tmux", "display-message", "-p", "#{session_name}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            check=False,
        )
        if current.returncode == 0 and current.stdout.strip() == session:
            return True
    exists = subprocess.run(
        ["tmux", "has-session", "-t", session],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if exists.returncode == 0:
        subprocess.run(["tmux", "kill-session", "-t", session], check=False)
        print(f"tmux_session_restarting {session}", flush=True)
    command = f"cd {sh_quote(str(ROOT))} && exec {sh_quote(sys.executable)} {sh_quote(str(Path(__file__).resolve()))}"
    subprocess.run(["tmux", "new-session", "-d", "-s", session, command], check=True)
    print(f"tmux_session_started {session}", flush=True)
    return False


def sh_quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"


def main() -> None:
    if not ensure_tmux():
        return
    load_dotenv(ROOT / ".env")
    token = os.environ["TELEGRAM_BOT_TOKEN"]
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    session_file = Path(os.environ.get("TELEGRAM_CODEX_SESSION_FILE", SESSION_FILE))
    bot = Telegram(token)
    runner = CodexRunner(bot, session_file)
    if chat_id:
        bot.send(chat_id, "<i>已连接Agent</i>")
    try:
        while True:
            try:
                for update in bot.updates():
                    message = update.get("message") or {}
                    chat = message.get("chat") or {}
                    incoming_chat = str(chat.get("id", ""))
                    print(
                        f"telegram_message chat_id={incoming_chat} type={chat.get('type')} "
                        f"username={chat.get('username')} first_name={chat.get('first_name')}",
                        flush=True,
                    )
                    if not allowed(incoming_chat, chat_id):
                        continue
                    text = str(message.get("text") or "").strip()
                    if text:
                        runner.submit(incoming_chat, int(message["message_id"]), text)
            except Exception as exc:
                print(f"telegram_codex_error {type(exc).__name__}: {exc}", flush=True)
                time.sleep(5)
    except KeyboardInterrupt:
        pass
    finally:
        if chat_id:
            bot.send(chat_id, "<i>已断开Agent</i>")


if __name__ == "__main__":
    main()
