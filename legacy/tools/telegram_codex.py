from __future__ import annotations

import html
import json
import os
import re
import signal
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
PHOTO_ROOT = Path("/tmp/telegram_codex")
os.environ['PATH'] = f"{Path.home() / '.local' / 'bin'}:{os.environ.get('PATH', '')}"
SESSION_FILE = Path(__file__).resolve().parent / ".telegram_codex_session"
MAX_MESSAGE = 3800
POLL_TIMEOUT = 5
SEND_RETRIES = 3
MAX_PHOTO_BYTES = 10 * 1024 * 1024
TMUX_SESSION = "telegram-codex"
PHOTO_RE = re.compile(r'^[ \t]*图片：“([^”\r\n]+)”[ \t]*$', re.MULTILINE)
EMPTY_FINAL_PROMPT = "上一轮没有返回最终答案。请继续完成用户最新任务，并只输出完整最终答案。"
ORDER_PROMPT = """给我当前正在运行的策略的 snapshot 信息。

重点看订单/动作记录。每一行代表一次 long 或 short，用 ↑ 表示 long，用 ↓ 表示 short。

输出字段要窄，适合手机 Telegram 查看：
- 北京时间只显示小时:分钟
- 方向用 ↑/↓
- actual edge
- signal edge
- 累计 qty
- 列名写完整，使用 <pre> 等宽文本对齐

参考格式：
<pre>方向  actual  signal  累计qty  时间
↑     554     557     2        16:30
↓     520     525     1        16:42</pre>

数值默认不要小数点；除非不带小数会影响判断。不要输出宽表，不要倾倒完整 JSON。"""
SYSTEM_PROMPT = """你是通过 Telegram 操作本机项目 /home/ubuntu/pycharm_nt 的 Codex。用户发来的每条 Telegram 消息都是真实任务指令，不是闲聊模拟。

默认使用中文，回答要简短、直接、可执行。除非用户要求解释，不要长篇分析。能直接做的事就直接做；需要运行命令或改文件时可以执行。你拥有当前项目的完整读写和运行权限，但不要改动与任务无关的文件。

当前项目运行在 Linux 的 ubuntu 用户下，项目路径是 /home/ubuntu/pycharm_nt，默认不要使用 root 用户路径或 Windows 路径，除非用户明确要求操作远端或其它环境。

重启当前 Telegram Codex bot 时，在 nt 环境运行 bot 脚本即可，它会自己检查/创建 telegram-codex tmux：cd /home/ubuntu/pycharm_nt && /home/ubuntu/miniconda/envs/nt/bin/python tools/telegram_codex.py

用户的命令默认都和当前项目内代码、配置、report、运行状态、git 状态或开发维护任务有关。除非用户明确指定外部主题，否则优先在当前项目上下文里理解和执行，不要泛泛回答。

这个 Telegram bot 会话的上下文和记忆应当作为独立上下文使用，只服务当前 bot 对话。不要让 bot 对话里的临时偏好、状态或任务污染其它对话、其它入口或项目持久规则；也不要假设其它对话里的临时上下文会影响当前 bot 会话。只有用户明确要求持久化到代码、配置、文档或 skill 时，才把信息写入项目文件。

最终回答必须使用 Telegram Bot API 支持的 HTML 格式。只使用这些标签：<b>、<i>、<u>、<s>、<code>、<pre>、<a href="...">。不要使用 Markdown，不要使用 table/div/style/h1/ul/ol/li/br 等 Telegram 不支持或不稳定的标签。普通文本里的 <、>、& 必须转义为 HTML 实体。

根据任务复杂度和回复长度设计返回 HTML 的结构。简单任务用一两句话即可，不要硬拆块；复杂任务可以用 <b>短标题</b> 分块，用空行分隔，用 <code>...</code> 标记路径、命令、数值或关键词，用 <pre>...</pre> 放多行代码、表格文本或日志摘录。

用户主要在手机上查看 Telegram 回复。发送数据表、snapshot、订单、持仓、日志摘要时，列要窄，字段名要短，避免宽表；数值默认不要带小数点，除非小数对判断很关键或用户明确要求。

除非用户明确指定，不要主动执行长耗时、高 CPU、高内存、海量 IO、全仓库大规模计算或可能长期占用资源的任务。需要这类任务时，先说明你将采用较轻量的检查或给出建议。

不要主动重启 Telegram bot、live 策略、collector、tmux session 或其它本机服务；如果判断需要重启，只说明原因和建议命令，等待用户明确下令后再执行。

不要在最终回答里倾倒完整日志。只汇报关键结果、改了什么、验证了什么、还有什么风险。路径、命令、数值要写清楚。

需要让 Telegram 发送本机图片时，先把图片保存到 /tmp/telegram_codex，再在最终回答中额外输出单独一行：图片：“绝对路径”。每张图片各写一行；其余内容仍使用 Telegram HTML。Bot 会移除图片指令并上传对应文件。"""


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

    def confirm_updates(self) -> None:
        requests.get(
            f"{self.base}/getUpdates",
            params={"offset": self.offset, "timeout": 0, "allowed_updates": json.dumps(["message"])},
            timeout=5,
        ).raise_for_status()

    def send(self, chat_id: str, text: str) -> list[int]:
        message_ids: list[int] = []
        for part in split_text(text):
            try:
                message_ids.append(self._send_html(chat_id, part))
            except requests.HTTPError:
                message_ids.append(self._send_html(chat_id, f"<pre>{html.escape(part)}</pre>"))
        return message_ids

    def delete(self, chat_id: str, message_ids: list[int]) -> None:
        for message_id in message_ids:
            if not message_id:
                continue
            try:
                requests.post(
                    f"{self.base}/deleteMessage",
                    json={"chat_id": chat_id, "message_id": message_id},
                    timeout=5,
                )
            except requests.RequestException as exc:
                print(f"telegram_delete_error {type(exc).__name__}", flush=True)

    # 上传项目内由 Codex 生成的图片。
    def send_photo(self, chat_id: str, path: Path) -> int:
        for attempt in range(SEND_RETRIES):
            try:
                with path.open("rb") as photo:
                    response = requests.post(
                        f"{self.base}/sendPhoto",
                        data={"chat_id": chat_id},
                        files={"photo": (path.name, photo)},
                        timeout=30,
                    )
                response.raise_for_status()
                result = response.json()
                return int((result.get("result") or {}).get("message_id") or 0)
            except (requests.ConnectionError, requests.Timeout):
                if attempt + 1 == SEND_RETRIES:
                    raise
                time.sleep(0.5 * (attempt + 1))
        raise RuntimeError("telegram photo retry exhausted")

    def _send_html(self, chat_id: str, text: str) -> int:
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        for attempt in range(SEND_RETRIES):
            try:
                response = requests.post(
                    f"{self.base}/sendMessage",
                    json=payload,
                    timeout=15,
                )
                response.raise_for_status()
                result = response.json()
                return int((result.get("result") or {}).get("message_id") or 0)
            except (requests.ConnectionError, requests.Timeout):
                if attempt + 1 == SEND_RETRIES:
                    raise
                time.sleep(0.5 * (attempt + 1))
        raise RuntimeError("telegram send retry exhausted")


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
        self.notice_ids: list[int] = []
        self.notice_open = False
        self.notice_gen = 0
        self.notice_sent = False

    def submit(self, chat_id: str, message_id: int, prompt: str) -> None:
        proc = None
        with self.lock:
            if self.busy:
                self.pending.append((message_id, prompt))
                self.queued_ids.add(message_id)
                self.cancel_requested = True
                proc = self.proc
                notice_gen = self.notice_gen
                busy = True
            else:
                self.busy = True
                self.notice_open = True
                self.notice_gen += 1
                self.notice_sent = False
                notice_gen = self.notice_gen
                busy = False
        if busy:
            self._notice(chat_id, "<i>插入</i>", notice_gen)
            stop_proc(proc)
            return
        thread = threading.Thread(target=self._run, args=(chat_id, [(message_id, prompt)], notice_gen), daemon=True)
        thread.start()

    def _run(self, chat_id: str, batch: list[tuple[int, str]], notice_gen: int) -> None:
        start = time.monotonic()
        while True:
            if any(not self._is_queued(message_id) for message_id, _prompt in batch):
                self._notice(chat_id, "<i>推理中</i>", notice_gen)
            try:
                result, usage = run_codex(merge_prompts(batch), self.session_file, self._set_proc, self._clear_proc)
                result, photos = extract_photos(result)
            except CodexCancelled:
                batch = self._merge_pending(batch)
                continue
            except Exception as exc:
                batch = self._finish(batch)
                detail = str(exc) if isinstance(exc, RuntimeError) else f"{type(exc).__name__}: {exc}"
                text = html.escape(detail)
                self._delete_notices(chat_id, notice_gen)
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
            self._delete_notices(chat_id, notice_gen)
            photo_errors = []
            for path in photos:
                try:
                    self.bot.send_photo(chat_id, path)
                except (OSError, requests.RequestException) as exc:
                    print(f"telegram_photo_error {path} {type(exc).__name__}", flush=True)
                    photo_errors.append(path.name)
            if photo_errors:
                failed = html.escape(", ".join(photo_errors))
                result = f"{result}\n\n<i>图片发送失败：{failed}</i>".strip()
            fallback = "图片已发送" if photos else "Codex 没有返回内容"
            self.bot.send(chat_id, with_stats(result or fallback, start, usage))
            return

    def _notice(self, chat_id: str, text: str, notice_gen: int) -> None:
        with self.lock:
            if notice_gen != self.notice_gen or self.notice_sent:
                return
            self.notice_sent = True
        thread = threading.Thread(target=self._send_notice, args=(chat_id, text, notice_gen), daemon=True)
        thread.start()

    def _send_notice(self, chat_id: str, text: str, notice_gen: int) -> None:
        message_ids = self.bot.send(chat_id, text)
        should_delete = False
        with self.lock:
            if self.notice_open and notice_gen == self.notice_gen:
                self.notice_ids.extend(message_ids)
            else:
                should_delete = True
        if should_delete:
            self.bot.delete(chat_id, message_ids)

    def _delete_notices(self, chat_id: str, notice_gen: int) -> None:
        with self.lock:
            if notice_gen != self.notice_gen:
                message_ids = []
            else:
                self.notice_open = False
                self.notice_sent = False
                message_ids = self.notice_ids
                self.notice_ids = []
        self.bot.delete(chat_id, message_ids)

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

    def status_html(self) -> str:
        return codex_status_html(self.session_file)


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


# 提取图片指令，并限制只能读取专用临时目录内的常见图片文件。
def extract_photos(text: str) -> tuple[str, list[Path]]:
    photos: list[Path] = []

    def extract(match: re.Match[str]) -> str:
        path = Path(match.group(1)).expanduser().resolve()
        try:
            path.relative_to(PHOTO_ROOT)
        except ValueError as exc:
            raise ValueError(f"图片路径不在临时图片目录内: {path}") from exc
        if not path.is_file():
            raise ValueError(f"图片不存在: {path}")
        if path.suffix.lower() not in {".jpg", ".jpeg", ".png"}:
            raise ValueError(f"不支持的图片格式: {path.suffix}")
        if path.stat().st_size > MAX_PHOTO_BYTES:
            raise ValueError(f"图片超过 10MB: {path}")
        photos.append(path)
        return ""

    clean = PHOTO_RE.sub(extract, text)
    clean = re.sub(r"\n{3,}", "\n\n", clean).strip()
    return clean, photos


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


# 读取最近的 Codex token_count 事件，给 /status 展示额度和上下文。
def codex_status_html(session_file: Path) -> str:
    event = latest_token_count(session_file)
    if event is None:
        return "<b>Codex</b>\n\n<i>还没有找到用量记录</i>"
    payload = event.get("payload") or {}
    info = payload.get("info") or {}
    limits = payload.get("rate_limits") or {}
    last_usage = info.get("last_token_usage") or {}
    context_used = int(last_usage.get("input_tokens") or 0)
    context_total = int(info.get("model_context_window") or 0)
    return (
        "<b>Codex</b>\n"
        "<pre>"
        f"5h  {limit_line(limits.get('primary'))}\n"
        f"7d  {limit_line(limits.get('secondary'))}\n"
        f"上下文 {format_tokens(context_used)}/{format_tokens(context_total)}"
        "</pre>"
    )


# 优先找当前 Telegram resume session，找不到再退到最近的 Codex session 文件。
def latest_token_count(session_file: Path) -> dict[str, Any] | None:
    session_path = codex_session_path(read_session(session_file))
    paths = [session_path] if session_path else []
    paths.extend(recent_codex_sessions())
    seen: set[Path] = set()
    for path in paths:
        if path in seen:
            continue
        seen.add(path)
        event = last_token_count(path)
        if event is not None:
            return event
    return None


def codex_session_path(session_id: str | None) -> Path | None:
    if not session_id:
        return None
    root = Path.home() / ".codex" / "sessions"
    matches = list(root.glob(f"**/*{session_id}.jsonl"))
    if not matches:
        return None
    return max(matches, key=lambda path: path.stat().st_mtime)


def recent_codex_sessions() -> list[Path]:
    root = Path.home() / ".codex" / "sessions"
    if not root.exists():
        return []
    return sorted(root.glob("**/*.jsonl"), key=lambda path: path.stat().st_mtime, reverse=True)


def last_token_count(path: Path) -> dict[str, Any] | None:
    last = None
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    for line in lines:
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        payload = event.get("payload") or {}
        if event.get("type") == "event_msg" and payload.get("type") == "token_count":
            last = event
    return last


def limit_line(raw_limit: Any) -> str:
    if not isinstance(raw_limit, dict):
        return "剩余 ? | 刷新 ?"
    used = float(raw_limit.get("used_percent") or 0)
    remain = max(0.0, 100.0 - used)
    return f"剩余 {remain:.0f}% | 刷新 {format_reset(raw_limit.get('resets_at'))}"


def format_reset(value: Any) -> str:
    if not value:
        return "?"
    tz = timezone(timedelta(hours=8))
    return datetime.fromtimestamp(int(value), tz).strftime("%m-%d %H:%M")


def format_tokens(value: int) -> str:
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if value >= 1000:
        return f"{value / 1000:.1f}K"
    return str(value)


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
    output, usage = _run_once(prompt, session_file, set_proc, clear_proc)
    if output:
        return output, usage
    return _run_once(EMPTY_FINAL_PROMPT, session_file, set_proc, clear_proc)


# 使用 CLI 的最终消息文件，避免把 commentary 误当成 final。
def _run_once(
    prompt: str,
    session_file: Path,
    set_proc: Any,
    clear_proc: Any,
) -> tuple[str, dict[str, Any] | None]:
    session_id = read_session(session_file)
    prompt = build_prompt(prompt)
    final_file = tempfile.NamedTemporaryFile(prefix="telegram_codex_", suffix=".txt", delete=False)
    final_path = Path(final_file.name)
    final_file.close()
    if session_id:
        cmd = [
            "codex",
            "exec",
            "resume",
            "--model",
            "gpt-5.6-luna",
            "-c",
            'model_reasoning_effort="max"',
            "-c",
            'service_tier="fast"',
            "-c",
            "features.fast_mode=true",
            "--json",
            "--output-last-message",
            str(final_path),
            "--dangerously-bypass-approvals-and-sandbox",
            session_id,
            prompt,
        ]
    else:
        cmd = [
            "codex",
            "exec",
            "--model",
            "gpt-5.6-luna",
            "-c",
            'model_reasoning_effort="max"',
            "-c",
            'service_tier="fast"',
            "-c",
            "features.fast_mode=true",
            "--json",
            "--output-last-message",
            str(final_path),
            "--sandbox",
            "danger-full-access",
            prompt,
        ]
    try:
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
        diagnostic, new_session, usage = parse_codex_output(stdout)
        if new_session and not session_id:
            session_file.parent.mkdir(parents=True, exist_ok=True)
            session_file.write_text(new_session, encoding="utf-8")
        if proc.returncode != 0:
            detail = "\n\n".join(part for part in [stderr.strip(), diagnostic] if part).strip()
            if not detail:
                detail = f"codex exited with code {proc.returncode}"
            raise RuntimeError(detail[-MAX_MESSAGE:])
        return final_path.read_text(encoding="utf-8").strip(), usage
    finally:
        final_path.unlink(missing_ok=True)


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
            text = codex_error_text(event)
            if text:
                errors.append(text)
        if event.get("type") == "turn.failed":
            error = event.get("error")
            text = codex_error_text(error)
            if text:
                errors.append(text)
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


def codex_error_text(error: Any) -> str:
    if isinstance(error, dict):
        return json.dumps(error, ensure_ascii=False, indent=2)
    if error:
        return str(error)
    return ""


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


def bot_command(text: str) -> str:
    command = text.split(maxsplit=1)[0].split("@", maxsplit=1)[0]
    return command.lower()


def is_reset_command(text: str) -> bool:
    return bot_command(text) == "/reset"


def is_status_command(text: str) -> bool:
    return bot_command(text) == "/status"


def is_order_command(text: str) -> bool:
    return bot_command(text) == "/order"


def restart_bot(bot: Telegram, chat_id: str) -> None:
    bot.send(chat_id, "<i>正在重启Bot</i>")
    bot.confirm_updates()
    restart_self()
    os._exit(0)


# 从 tmux 内触发重启时，先在 tmux 外启动一个同脚本进程，让 ensure_tmux 重建会话。
def restart_self() -> None:
    env = os.environ.copy()
    env.pop("TMUX", None)
    script = Path(__file__).resolve()
    command = f"sleep 0.5; exec {sh_quote(sys.executable)} {sh_quote(str(script))}"
    subprocess.Popen(
        ["/bin/sh", "-c", command],
        cwd=ROOT,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )


def handle_bot_command(bot: Telegram, runner: CodexRunner, chat_id: str, message_id: int, text: str) -> bool:
    if is_status_command(text):
        bot.send(chat_id, runner.status_html())
        return True
    if is_reset_command(text):
        restart_bot(bot, chat_id)
        return True
    if is_order_command(text):
        runner.submit(chat_id, message_id, ORDER_PROMPT)
        return True
    return False


def main() -> None:
    if not ensure_tmux():
        return
    load_dotenv(ROOT / ".env")
    token = os.environ["TELEGRAM_CODEX_BOT_TOKEN"]
    chat_id = os.environ.get("TELEGRAM_CODEX_CHAT_ID")
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
                    message_id = int(message["message_id"])
                    if text and handle_bot_command(bot, runner, incoming_chat, message_id, text):
                        continue
                    if text:
                        runner.submit(incoming_chat, message_id, text)
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
