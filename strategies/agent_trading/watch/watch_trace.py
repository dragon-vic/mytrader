from __future__ import annotations

import json
import threading
import time
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl
from urllib.parse import urlencode
from urllib.parse import urlsplit
from urllib.parse import urlunsplit


CACHE_HEADERS = (
    "Age",
    "Cache-Control",
    "CF-Cache-Status",
    "Date",
    "ETag",
    "Expires",
    "Last-Modified",
    "Via",
    "X-Cache",
)


class WatchTrace:
    def __init__(self, event_id: str, watch_dir: Path) -> None:
        self.event_id = event_id
        self.watch_dir = watch_dir.resolve()
        self.watch_dir.mkdir(parents=True, exist_ok=True)
        self.path = self.watch_dir / "trace.jsonl"
        self.started = time.perf_counter()
        self.lock = threading.Lock()

    def record(self, source: str, stage: str, **values: Any) -> None:
        payload = {
            "recorded_at": datetime.now(UTC).isoformat(),
            "recorded_ns": time.time_ns(),
            "elapsed_ms": round((time.perf_counter() - self.started) * 1000, 3),
            "event_id": self.event_id,
            "source": source,
            "stage": stage,
            **values,
        }
        line = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        with self.lock:
            with self.path.open("a", encoding="utf-8") as stream:
                stream.write(line + "\n")

    def write_summary(self, payload: dict[str, Any]) -> None:
        path = self.watch_dir / "summary.json"
        temporary = path.with_suffix(".json.tmp")
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temporary.replace(path)


def fresh_url(url: str) -> str:
    parsed = urlsplit(url)
    query = parse_qsl(parsed.query, keep_blank_values=True)
    query.append(("_nt", str(time.time_ns())))
    return urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, urlencode(query), parsed.fragment),
    )


def cache_metadata(headers) -> dict[str, str]:
    return {
        name: value
        for name in CACHE_HEADERS
        if (value := headers.get(name)) is not None
    }
