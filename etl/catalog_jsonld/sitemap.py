"""Sitemap helpers for catalog dataset landing pages."""

from __future__ import annotations

import html


def sitemap_xml(urls: list[str]) -> str:
    entries = "\n".join(f"  <url><loc>{html.escape(url, quote=True)}</loc></url>" for url in sorted(urls))
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        f"{entries}\n"
        "</urlset>\n"
    )
