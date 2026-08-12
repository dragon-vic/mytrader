from __future__ import annotations

import asyncio
import hashlib
import logging
import re
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urljoin, urlsplit

import aiohttp

from strategies.agent_trading.watch.disclosure_preprocessor import DisclosureProcessor
from strategies.agent_trading.watch.watch_data_models import DisclosureFile, WatchTarget

LOG = logging.getLogger(__name__)
_CONTEXT_LIMIT = 320
_SKIPPED_TAGS = {"script", "style", "noscript", "svg", "template"}
_CONTEXT_BOUNDARIES = {
    "article",
    "div",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "li",
    "p",
    "section",
    "tr",
}
_GENERIC_PDF_LABEL = re.compile(
    r"^(?:(?:download|view)\s+(?:as\s+)?)?"
    r"(?:this\s+press\s+release\s+)?pdf(?:\s+(?:format|version))?$",
    re.IGNORECASE,
)
_EXCLUDED_LINK = re.compile(
    r"\b(?:6-k|8-k|10-q|10-k|20-f|40-f|annual report|proxy|governance|esg|"
    r"sustainability|political|annual meeting|investor day|company overview|"
    r"fact sheet|transcript)\b",
    re.IGNORECASE,
)
_DOCUMENT_RULES = (
    (
        "SHAREHOLDER_LETTER",
        re.compile(
            r"\b(?:shareholder letter|letter to (?:shareholders|stockholders))\b",
            re.IGNORECASE,
        ),
    ),
    (
        "FINANCIAL_UPDATE",
        re.compile(r"\bfinancial update\b", re.IGNORECASE),
    ),
    (
        "PREPARED_REMARKS",
        re.compile(
            r"\b(?:prepared remarks?|published script|"
            r"earnings call (?:published )?script)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "CFO_COMMENTARY",
        re.compile(r"\bcfo commentary\b", re.IGNORECASE),
    ),
    (
        "OUTLOOK_PRESENTATION",
        re.compile(
            r"\b(?:(?:outlook|guidance).{0,50}(?:presentation|slides)|"
            r"(?:presentation|slides).{0,50}(?:outlook|guidance))\b",
            re.IGNORECASE,
        ),
    ),
    (
        "SUPPLEMENTAL_INFORMATION",
        re.compile(
            r"\bsupplement(?:al|ary) (?:information|data|slides|materials)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "NON_GAAP_RECONCILIATION",
        re.compile(r"\bnon-gaap reconciliations?\b", re.IGNORECASE),
    ),
    (
        "RESULTS_SNAPSHOT",
        re.compile(r"\bresults snapshot\b", re.IGNORECASE),
    ),
    (
        "QUARTERLY_TREND",
        re.compile(
            r"\b(?:quarterly revenue trend|revenue by market.{0,30}trend|"
            r"historical trending information)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "EARNINGS_PRESENTATION",
        re.compile(
            r"\b(?:(?:earnings|financial results?|quarterly results?|conference call|"
            r"q[1-4](?:\s*fy)?\s*\d{2,4}).{0,60}(?:presentation|slides|deck)|"
            r"(?:investor presentation)|"
            r"(?:presentation|slides|deck).{0,60}(?:earnings|financial results?|"
            r"quarterly results?|q[1-4](?:\s*fy)?\s*\d{2,4}))\b",
            re.IGNORECASE,
        ),
    ),
    (
        "EARNINGS_RELEASE",
        re.compile(
            r"\b(?:full disclosure|earnings release|financial results|"
            r"quarterly results|results for (?:the )?(?:first|second|third|fourth) "
            r"quarter)\b",
            re.IGNORECASE,
        ),
    ),
)


@dataclass(frozen=True)
class PdfAttachment:
    title: str
    url: str
    document_type: str


class NewsPdfAttachmentCollector:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        processor: DisclosureProcessor,
    ) -> None:
        self.session = session
        self.processor = processor

    async def collect(
        self,
        target: WatchTarget,
        html: bytes,
        base_url: str,
        source: str,
        user_agent: str | None = None,
    ) -> tuple[DisclosureFile, ...]:
        attachments = find_relevant_pdf_attachments(html, base_url)
        if not attachments:
            return ()
        results = await asyncio.gather(
            *(
                self._download(target, attachment, source, user_agent)
                for attachment in attachments
            ),
            return_exceptions=True,
        )
        files: list[DisclosureFile] = []
        for attachment, result in zip(attachments, results, strict=True):
            if isinstance(result, BaseException):
                target.trace.record(
                    source,
                    "pdf_attachment_failed",
                    url=attachment.url,
                    document_type=attachment.document_type,
                    error=f"{type(result).__name__}: {result}",
                )
                LOG.warning(
                    "news PDF attachment failed event_id=%s url=%s error=%r",
                    target.plan.event_id,
                    attachment.url,
                    result,
                )
                continue
            if result is not None:
                files.append(result)
        return tuple(files)

    async def _download(
        self,
        target: WatchTarget,
        attachment: PdfAttachment,
        source: str,
        user_agent: str | None,
    ) -> DisclosureFile | None:
        headers = {"User-Agent": user_agent} if user_agent else None
        async with self.session.get(attachment.url, headers=headers) as response:
            response.raise_for_status()
            data = await response.read()
            content_type = response.headers.get("Content-Type", "")
            source_url = str(response.url)
        if not data.startswith(b"%PDF-"):
            target.trace.record(
                source,
                "pdf_attachment_skipped",
                url=source_url,
                reason="response is not a PDF",
            )
            return None

        folder = target.analysis_input_dir / "disclosure" / source / "attachments"
        folder.mkdir(parents=True, exist_ok=True)
        path = _available_path(folder, source_url)
        path.write_bytes(data)
        processed = await asyncio.to_thread(
            self.processor.process,
            target.analysis_input_dir,
            path,
            source,
            attachment.document_type,
            attachment.title or attachment.document_type.replace("_", " ").title(),
            source_url,
            content_type or "application/pdf",
        )
        target.trace.record(
            source,
            "pdf_attachment_processed",
            url=source_url,
            document_type=attachment.document_type,
            size_bytes=len(data),
            processing_status=processed.processing_status,
        )
        return processed


class _PdfLinkParser(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.context = ""
        self.skipped = 0
        self.href: str | None = None
        self.prefix = ""
        self.anchor_parts: list[str] = []
        self.attachments: list[PdfAttachment] = []

    def handle_starttag(self, tag: str, attrs) -> None:
        tag = tag.lower()
        if tag in _SKIPPED_TAGS:
            self.skipped += 1
            return
        if self.skipped:
            return
        if tag in _CONTEXT_BOUNDARIES and self.href is None:
            self.context = ""
        if tag != "a":
            return
        href = dict(attrs).get("href")
        if isinstance(href, str) and href.strip():
            self.href = urljoin(self.base_url, href.strip())
            self.prefix = self.context
            self.anchor_parts = []

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in _SKIPPED_TAGS:
            self.skipped = max(0, self.skipped - 1)
            return
        if self.skipped:
            return
        if tag in _CONTEXT_BOUNDARIES and self.href is None:
            self.context = ""
            return
        if tag != "a" or self.href is None:
            return
        title = " ".join(self.anchor_parts).strip()
        attachment = _classify_link(title, self.href, self.prefix)
        if attachment is not None:
            self.attachments.append(attachment)
        self._append_context(title)
        self.href = None
        self.prefix = ""
        self.anchor_parts = []

    def handle_data(self, data: str) -> None:
        if self.skipped:
            return
        text = " ".join(data.split())
        if not text:
            return
        if self.href is not None:
            self.anchor_parts.append(text)
        else:
            self._append_context(text)

    def _append_context(self, text: str) -> None:
        self.context = f"{self.context} {text}"[-_CONTEXT_LIMIT:]


def find_relevant_pdf_attachments(
    html: bytes,
    base_url: str,
) -> tuple[PdfAttachment, ...]:
    parser = _PdfLinkParser(base_url)
    parser.feed(html.decode("utf-8", errors="replace"))
    unique: dict[str, PdfAttachment] = {}
    for attachment in parser.attachments:
        unique.setdefault(attachment.url, attachment)
    return tuple(unique.values())


def _classify_link(
    title: str,
    url: str,
    prefix: str,
) -> PdfAttachment | None:
    if not _looks_like_pdf_link(title, url):
        return None
    if _GENERIC_PDF_LABEL.fullmatch(" ".join(title.split())):
        return None
    direct_text = _normalized_link_text(title, url)
    if _EXCLUDED_LINK.search(direct_text):
        return None
    for document_type, rule in _DOCUMENT_RULES:
        if rule.search(direct_text):
            return PdfAttachment(title, url, document_type)
    context = f"{prefix} {direct_text}"
    for document_type, rule in _DOCUMENT_RULES:
        if rule.search(context):
            return PdfAttachment(title, url, document_type)
    return None


def _looks_like_pdf_link(title: str, url: str) -> bool:
    parts = urlsplit(url)
    path = unquote(parts.path)
    return bool(
        Path(path).suffix.lower() == ".pdf"
        or path.rstrip("/").lower().endswith("/pdf")
        or "/static-files/" in path.lower()
        or re.search(r"\bpdf\b", f"{title} {parts.query}", re.IGNORECASE)
    )


def _normalized_link_text(title: str, url: str) -> str:
    path = unquote(urlsplit(url).path)
    normalized_path = re.sub(r"[^A-Za-z0-9]+", " ", path)
    return f"{title} {normalized_path}"


def _available_path(folder: Path, url: str) -> Path:
    url_path = Path(unquote(urlsplit(url).path))
    name = re.sub(r"[^A-Za-z0-9._-]", "_", url_path.name)
    if not name or Path(name).suffix.lower() != ".pdf":
        name = f"attachment-{hashlib.sha256(url.encode('utf-8')).hexdigest()[:8]}.pdf"
    path = folder / name
    if not path.exists():
        return path
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:8]
    return folder / f"{path.stem}-{digest}{path.suffix}"
