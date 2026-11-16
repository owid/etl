"""Fetch a dataset's producer documentation in parallel and keep it as plain text.

Used by `prepare.py`; importable on its own. Every link found in the snapshot `.dvc` files, the metadata and the
step code is fetched at once (20 s timeout each), converted to text (HTML and PDF), and written under
`<dossier>/docs/` with an `index.json` that records, per link: its status, the route it came by (direct, Wayback
Machine or the cache), the fetch date and a hash. The reviewer then reads and quotes the saved text instead of
fetching each page in its own turn.

A saved document is reused for `CACHE_DAYS` days. The dossier directory is keyed by dataset version, so a new
version starts with an empty cache. Data files are never cached: `check_freshness` downloads the bytes each run and
compares their md5 with the snapshot's.
"""

import datetime as dt
import hashlib
import io
import json
import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from pathlib import Path
from urllib.parse import urljoin, urlsplit

import requests

CACHE_DAYS = 7
TIMEOUT = 20
MAX_DOC_BYTES = 40 * 1024 * 1024
MAX_WORKERS = 8
# Fewer characters than this means the page is a JavaScript shell or a block page, not documentation.
THIN_CHARS = 400
# Producers serve documentation to browsers; many refuse the default `python-requests` agent.
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/pdf,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

URL_RE = re.compile(r"https?://[^\s\"'<>\]\[`|]+")
DATA_SUFFIXES = (".csv", ".xlsx", ".xls", ".zip", ".parquet", ".feather", ".dta", ".sav", ".rds", ".gz", ".7z", ".nc")
# Links that are ours or that say nothing about the producer's data.
SKIP_HOSTS = (
    "ourworldindata.org",
    "owid.io",
    "owid.cloud",
    "creativecommons.org",
    "github.com/owid",
    "docs.owid.io",
    "localhost",
    "example.com",
)
# One hop from the dataset page: links whose text or address suggests documentation.
DOC_HINTS = re.compile(
    r"methodolog|codebook|data dictionary|dictionary|technical note|technical paper|definition|metadata|"
    r"documentation|release note|what.?s new|changelog|revision|readme|user guide|sources and methods|"
    r"glossary|explanatory note|faq|about the data|about this data",
    re.IGNORECASE,
)
MAX_LINKED = 10


@dataclass
class Doc:
    url: str
    role: str  # url_main | url_download | license | metadata | code | linked-from-main
    kind: str = "doc"  # doc | data
    status: str = "pending"  # ok | thin | unreachable | data (not fetched as a document)
    via: str = ""  # direct | wayback | cache
    fetched_at: str = ""
    chars: int = 0
    sha256: str = ""
    file: str = ""
    title: str = ""
    note: str = ""
    links: list = field(default_factory=list, repr=False)


def extract_links(text: str) -> list[str]:
    """Every http(s) link in a text, in order, without trailing punctuation, Jinja fragments or our own hosts."""
    found: list[str] = []
    for url in URL_RE.findall(text):
        # Sentence punctuation after a link, and the closing parenthesis of a markdown `[text](url)`.
        while url and (url[-1] in ".,;:!?*_\\" or (url[-1] == ")" and url.count("(") < url.count(")"))):
            url = url[:-1]
        if "{" in url or "}" in url or "<" in url or len(url) < 12:
            continue
        if any(host in url for host in SKIP_HOSTS):
            continue
        if url not in found:
            found.append(url)
    return found


def is_data_url(url: str) -> bool:
    return urlsplit(url).path.lower().endswith(DATA_SUFFIXES)


def looks_like_endpoint(url: str) -> bool:
    """Whether a link found in step code is something the code calls (an API, a query) and not a page to read."""
    parts = urlsplit(url)
    return bool(parts.query) or bool(re.search(r"(^|[./-])api([./-]|$)", f"{parts.netloc}{parts.path}".lower()))


def file_name(url: str, number: int) -> str:
    """A readable, unique file name for a saved document."""
    parts = urlsplit(url)
    stem = re.sub(r"[^A-Za-z0-9]+", "-", f"{parts.netloc}{parts.path}").strip("-")[:70]
    return f"{number:02d}-{stem}.txt"


def to_text(content: bytes, content_type: str, url: str) -> tuple[str, str, list[tuple[str, str]]]:
    """(text, title, links) of a fetched document. Links are (anchor text, absolute address), for HTML only."""
    if content[:5] == b"%PDF-" or "pdf" in content_type or urlsplit(url).path.lower().endswith(".pdf"):
        return _pdf_text(content), "", []
    if urlsplit(url).path.lower().endswith(".docx") or "wordprocessingml" in content_type:
        import docx

        document = docx.Document(io.BytesIO(content))
        return "\n".join(p.text for p in document.paragraphs), "", []
    decoded = content.decode("utf-8", errors="replace")
    if "html" in content_type or decoded.lstrip()[:200].lower().startswith(("<!doctype html", "<html")):
        return _html_text(decoded, url)
    return decoded, "", []


def _pdf_text(content: bytes) -> str:
    """Text of a PDF with a `[page N]` marker before each page, so a quote can cite its page."""
    pages: list[str] = []
    try:
        from pypdf import PdfReader

        reader = PdfReader(io.BytesIO(content))
        pages = [page.extract_text() or "" for page in reader.pages]
    except Exception:
        pages = []
    if sum(len(p) for p in pages) < THIN_CHARS:
        # A scanned or oddly encoded PDF: pdfplumber reads some that pypdf cannot.
        try:
            import pdfplumber

            with pdfplumber.open(io.BytesIO(content)) as pdf:
                pages = [page.extract_text() or "" for page in pdf.pages]
        except Exception:
            pass
    return "\n\n".join(f"[page {i}]\n{text}" for i, text in enumerate(pages, start=1))


def _html_text(html: str, url: str) -> tuple[str, str, list[tuple[str, str]]]:
    import html2text
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "lxml")
    title = (soup.title.string or "").strip() if soup.title else ""
    links = [
        (a.get_text(" ", strip=True), urljoin(url, a["href"]))
        for a in soup.find_all("a", href=True)
        if not a["href"].startswith(("#", "mailto:", "javascript:"))
    ]
    for tag in soup(["script", "style", "noscript", "svg", "nav", "footer"]):
        tag.decompose()
    converter = html2text.HTML2Text()
    converter.body_width = 0
    converter.ignore_images = True
    converter.ignore_links = True
    return converter.handle(str(soup)), title, links


def _get(url: str) -> tuple[bytes, str] | None:
    """Content and content type of a link, or None when it cannot be read."""
    try:
        with requests.get(url, headers=HEADERS, timeout=TIMEOUT, stream=True, allow_redirects=True) as response:
            if response.status_code != 200:
                return None
            chunks, size = [], 0
            for chunk in response.iter_content(1 << 16):
                chunks.append(chunk)
                size += len(chunk)
                if size > MAX_DOC_BYTES:
                    break
            return b"".join(chunks), response.headers.get("Content-Type", "").lower()
    except requests.RequestException:
        return None


def _wayback(url: str) -> tuple[bytes, str, str] | None:
    """The Wayback Machine's closest capture of a link: (content, content type, capture date)."""
    try:
        reply = requests.get("https://archive.org/wayback/available", params={"url": url}, timeout=TIMEOUT).json()
    except (requests.RequestException, ValueError):
        return None
    closest = (reply.get("archived_snapshots") or {}).get("closest") or {}
    if not closest.get("available") or not closest.get("timestamp"):
        return None
    # `id_` asks for the capture as it was served, without the Wayback toolbar.
    got = _get(f"https://web.archive.org/web/{closest['timestamp']}id_/{url}")
    if got is None:
        return None
    return got[0], got[1], closest["timestamp"][:8]


def fetch_one(doc: Doc, docs_dir: Path, number: int, now: dt.datetime) -> Doc:
    """Fetch one link, directly and then through the Wayback Machine, and save its text."""
    if doc.kind == "data":
        doc.status, doc.note = "data", "a data file: see the freshness check, not read as documentation"
        return doc
    if doc.role == "code" and looks_like_endpoint(doc.url):
        doc.status, doc.note = "not-fetched", "an endpoint in the step code, not documentation"
        return doc
    if doc.role == "not-fetched":
        doc.status, doc.note = "not-fetched", "past the cap on links read from metadata: fetch it if a claim cites it"
        return doc
    got, via, note = _get(doc.url), "direct", ""
    text, title, links = ("", "", [])
    if got is not None:
        text, title, links = to_text(got[0], got[1], doc.url)
    if got is None or len(text.strip()) < THIN_CHARS:
        archived = _wayback(doc.url)
        if archived is not None:
            archived_text, archived_title, archived_links = to_text(archived[0], archived[1], doc.url)
            if len(archived_text.strip()) > len(text.strip()):
                text, title, links = archived_text, archived_title, archived_links
                via, note = "wayback", f"Wayback Machine capture of {archived[2]}: may be older than the live page"
    doc.links = links
    doc.title = title
    doc.chars = len(text.strip())
    if doc.chars == 0:
        doc.status = "unreachable"
        doc.note = "not readable by script (blocked, missing or timed out): WebFetch it, then offer the user's Chrome"
        return doc
    doc.status = "ok" if doc.chars >= THIN_CHARS else "thin"
    if doc.status == "thin":
        note = "almost no text (a JavaScript page or a block page): WebFetch it, then offer the user's Chrome"
    doc.via, doc.note, doc.fetched_at = via, note, now.strftime("%Y-%m-%d %H:%M")
    doc.sha256 = hashlib.sha256(text.encode()).hexdigest()[:16]
    doc.file = file_name(doc.url, number)
    header = f"URL: {doc.url}\nFetched: {doc.fetched_at} ({via})\nTitle: {title}\n{'-' * 80}\n"
    (docs_dir / doc.file).write_text(header + text)
    return doc


def is_fresh(entry: dict, docs_dir: Path, now: dt.datetime, days: int = CACHE_DAYS) -> bool:
    """Whether a saved document can be reused: read successfully, still on disk, and younger than `days`."""
    if entry.get("status") != "ok" or not entry.get("file") or not (docs_dir / entry["file"]).exists():
        return False
    try:
        fetched = dt.datetime.strptime(entry["fetched_at"], "%Y-%m-%d %H:%M")
    except (KeyError, ValueError):
        return False
    return now - fetched < dt.timedelta(days=days)


def linked_documents(doc: Doc, known: set[str]) -> list[Doc]:
    """Documentation one hop from the dataset page: links whose text or address looks like documentation."""
    out: list[Doc] = []
    for text, url in doc.links:
        url = url.split("#")[0]
        if url in known or not url.startswith("http") or any(host in url for host in SKIP_HOSTS):
            continue
        if is_data_url(url) or not DOC_HINTS.search(f"{text} {urlsplit(url).path}"):
            continue
        known.add(url)
        out.append(Doc(url=url, role="linked-from-main", note=f"linked from the dataset page as “{text[:60]}”"))
        if len(out) == MAX_LINKED:
            break
    return out


def fetch_all(
    links: list[tuple[str, str]], dossier: Path, refresh: bool = False, now: dt.datetime | None = None
) -> list[Doc]:
    """Fetch every (url, role) in parallel, reusing saved documents, and write `docs/index.json`.

    Documentation linked from the dataset page (`url_main`) is fetched in a second parallel round.
    """
    now = now or dt.datetime.now()
    docs_dir = dossier / "docs"
    docs_dir.mkdir(parents=True, exist_ok=True)
    index_path = docs_dir / "index.json"
    saved = {}
    if index_path.exists() and not refresh:
        saved = {entry["url"]: entry for entry in json.loads(index_path.read_text())}

    docs: list[Doc] = []
    numbers = iter(range(1, 10_000))

    def run(batch: list[Doc]) -> None:
        todo = []
        for doc in batch:
            entry = saved.get(doc.url)
            if entry and is_fresh(entry, docs_dir, now):
                cached = Doc(**{k: v for k, v in entry.items() if k in Doc.__dataclass_fields__})
                cached.role, cached.via = doc.role, "cache"
                docs.append(cached)
                next(numbers)
            else:
                todo.append((doc, next(numbers)))
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            docs.extend(pool.map(lambda item: fetch_one(item[0], docs_dir, item[1], now), todo))

    first = [
        Doc(url=url, role=role, kind="data" if is_data_url(url) or role == "url_download" else "doc")
        for url, role in links
    ]
    run(first)
    known = {doc.url for doc in docs}
    second: list[Doc] = []
    for doc in docs:
        if doc.role == "url_main" and doc.via != "cache":
            second += linked_documents(doc, known)
    # A dataset page served from the cache has no parsed links: reuse the documents it led to last time.
    second += [
        Doc(url=url, role="linked-from-main", note=entry.get("note", ""))
        for url, entry in saved.items()
        if entry.get("role") == "linked-from-main" and url not in known
    ]
    run(second)

    index_path.write_text(json.dumps([{k: v for k, v in asdict(d).items() if k != "links"} for d in docs], indent=1))
    return docs


def check_freshness(url: str, md5: str, size: int | None, max_mb: int = 150) -> dict:
    """Download a snapshot's source file again and compare its bytes with the snapshot (never cached).

    A producer can replace a file in place without changing its name or version label, so the bytes are the
    evidence: `same`, `changed`, `too-large` (not downloaded; compare by hand) or `unreachable`.
    """
    result = {"url": url, "status": "unreachable", "detail": ""}
    try:
        with requests.get(url, headers=HEADERS, timeout=TIMEOUT, stream=True, allow_redirects=True) as response:
            if response.status_code != 200:
                result["detail"] = f"HTTP {response.status_code}"
                return result
            declared = int(response.headers.get("Content-Length") or 0)
            encoded = response.headers.get("Content-Encoding")
            if declared and size and declared != size and not encoded:
                result["status"] = "changed"
                result["detail"] = (
                    f"{declared:,} bytes at the source, {size:,} in the snapshot: replaced in place since "
                    "`date_accessed`. Look for a newer release before relying on the snapshot"
                )
                return result
            if declared > max_mb * 1024 * 1024:
                result["status"] = "too-large"
                result["detail"] = f"same size as the snapshot ({declared / 1e6:.0f} MB), too large to hash here"
                return result
            digest, got = hashlib.md5(), 0
            for chunk in response.iter_content(1 << 20):
                digest.update(chunk)
                got += len(chunk)
                if got > max_mb * 1024 * 1024:
                    result["status"], result["detail"] = "too-large", f"over {max_mb} MB: not hashed"
                    return result
            modified = response.headers.get("Last-Modified", "")
    except requests.RequestException as error:
        result["detail"] = type(error).__name__
        return result
    if digest.hexdigest() == md5:
        result["status"], result["detail"] = "same", "bytes identical to the snapshot"
    else:
        result["status"] = "changed"
        result["detail"] = (
            f"bytes differ from the snapshot ({got:,} bytes now, {size or 0:,} in the snapshot"
            + (f"; Last-Modified {modified}" if modified else "")
            + "): replaced in place, or generated per request. Diff the content before relying on the snapshot"
        )
    return result
