"""Sitemap helpers for catalog dataset landing pages."""

from __future__ import annotations

import html
import re
from dataclasses import dataclass

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass(frozen=True)
class SitemapEntry:
    url: str
    lastmod: str | None = None


def sitemap_xml(entries: list[SitemapEntry]) -> str:
    """Build a sitemap XML document with one ``<url>`` per entry, sorted by URL.

    ``lastmod`` should be the dataset's real version date (``YYYY-MM-DD``) where known —
    it's the main recrawl signal now that versions bump invisibly under a stable URL.
    Entries without a valid date-shaped ``lastmod`` omit the tag rather than defaulting
    to "today".
    """
    blocks = []
    for entry in sorted(entries, key=lambda e: e.url):
        loc = f"    <loc>{html.escape(entry.url, quote=True)}</loc>"
        if entry.lastmod and _DATE_RE.match(entry.lastmod):
            blocks.append(f"  <url>\n{loc}\n    <lastmod>{entry.lastmod}</lastmod>\n  </url>")
        else:
            blocks.append(f"  <url>\n{loc}\n  </url>")
    body = "\n".join(blocks)
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        f"{body}\n"
        "</urlset>\n"
    )
