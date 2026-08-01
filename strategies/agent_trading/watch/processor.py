from __future__ import annotations

import hashlib
import json
import logging
import re
import xml.etree.ElementTree as ET
from html.parser import HTMLParser
from io import BytesIO
from pathlib import Path
from urllib.parse import urlsplit

from pypdf import PdfReader

from strategies.agent_trading.watch.models import DisclosureFile


LOG = logging.getLogger(__name__)
__all__ = ["DisclosureProcessor"]


class DisclosureProcessor:
    # 根据文件内容生成便于 Agent 阅读的副本，原文件始终保持不变。
    def process(
        self,
        event_dir: Path,
        raw_path: Path,
        document_type: str,
        description: str,
        source_url: str,
        content_type: str,
    ) -> DisclosureFile:
        data = raw_path.read_bytes()
        content_format = _detect_format(data, content_type, source_url)
        relative_raw = raw_path.relative_to(event_dir).as_posix()
        common = {
            "document_type": document_type,
            "description": description,
            "source_url": source_url,
            "content_type": content_type,
            "content_format": content_format,
            "size_bytes": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
            "raw_path": relative_raw,
        }
        if content_format == "binary":
            return DisclosureFile(
                **common,
                analysis_path=relative_raw,
                processing_status="raw_only",
                processing_error=None,
            )

        try:
            suffix, text = _prepare(data, content_format, content_type)
            output = raw_path.parent.parent / "processed" / f"{raw_path.name}.{suffix}"
            _write_text(output, text)
        except Exception as exc:
            LOG.warning("disclosure preprocessing failed path=%s error=%s", raw_path, exc)
            return DisclosureFile(
                **common,
                analysis_path=relative_raw,
                processing_status="failed",
                processing_error=f"{type(exc).__name__}: {exc}",
            )

        return DisclosureFile(
            **common,
            analysis_path=output.relative_to(event_dir).as_posix(),
            processing_status="processed",
            processing_error=None,
        )


class _ReadableHtmlParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.skipped = 0
        self.cells = 0

    def handle_starttag(self, tag: str, _attrs) -> None:
        tag = tag.lower()
        if tag in {"script", "style", "noscript", "svg"}:
            self.skipped += 1
            return
        if self.skipped:
            return
        if tag in {"p", "div", "section", "article", "blockquote", "tr"}:
            self.parts.append("\n")
        elif tag == "br":
            self.parts.append("\n")
        elif tag in {"ul", "ol"}:
            self.parts.append("\n")
        elif tag == "li":
            self.parts.append("\n- ")
        elif tag in {"th", "td"}:
            if self.cells:
                self.parts.append(" | ")
            self.cells += 1
        elif tag in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            self.parts.append(f"\n\n{'#' * int(tag[1])} ")

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in {"script", "style", "noscript", "svg"}:
            self.skipped = max(0, self.skipped - 1)
            return
        if self.skipped:
            return
        if tag == "tr":
            self.parts.append("\n")
            self.cells = 0
        elif tag in {"p", "div", "section", "article", "blockquote"}:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if self.skipped:
            return
        text = re.sub(r"\s+", " ", data)
        if text.strip():
            self.parts.append(text)

    def markdown(self) -> str:
        lines = [re.sub(r"[ \t]+", " ", line).strip() for line in "".join(self.parts).splitlines()]
        output: list[str] = []
        for line in lines:
            if line or output and output[-1]:
                output.append(line)
        return "\n".join(output).strip() + "\n"


def _detect_format(data: bytes, content_type: str, source_url: str) -> str:
    media_type = content_type.partition(";")[0].strip().lower()
    suffix = Path(urlsplit(source_url).path).suffix.lower()
    leading = data.lstrip()[:256].lower()

    if data.startswith(b"%PDF-") or media_type == "application/pdf" or suffix == ".pdf":
        return "pdf"
    if (
        media_type in {"application/json", "application/ld+json"}
        or media_type.endswith("+json")
        or suffix == ".json"
        or leading.startswith((b"{", b"["))
    ):
        return "json"
    if (
        media_type in {"text/html", "application/xhtml+xml"}
        or suffix in {".htm", ".html", ".xhtml"}
        or b"<html" in leading
        or leading.startswith(b"<!doctype html")
    ):
        return "html"
    if (
        media_type in {"application/xml", "text/xml"}
        or media_type.endswith("+xml")
        or suffix in {".xml", ".xbrl"}
        or leading.startswith(b"<?xml")
    ):
        return "xml"
    if media_type.startswith("text/") or suffix in {".txt", ".md", ".csv"}:
        return "text"
    try:
        data.decode(_charset(content_type))
    except (LookupError, UnicodeDecodeError):
        return "binary"
    return "text"


def _prepare(data: bytes, content_format: str, content_type: str) -> tuple[str, str]:
    if content_format == "pdf":
        pages = []
        for number, page in enumerate(PdfReader(BytesIO(data)).pages, start=1):
            pages.append(f"# Page {number}\n\n{page.extract_text() or ''}")
        return "md", "\n\n".join(pages).strip() + "\n"

    text = data.decode(_charset(content_type), errors="replace")
    if content_format == "html":
        parser = _ReadableHtmlParser()
        parser.feed(text)
        return "md", parser.markdown()
    if content_format == "json":
        payload = json.loads(text)
        return "json", json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    if content_format == "xml":
        root = ET.fromstring(text)
        ET.indent(root)
        return "xml", ET.tostring(root, encoding="unicode") + "\n"
    return "txt", text


def _charset(content_type: str) -> str:
    match = re.search(r"charset=([^;\s]+)", content_type, re.IGNORECASE)
    return match.group(1).strip('"\'') if match else "utf-8"


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_text(text, encoding="utf-8")
    temporary.replace(path)
