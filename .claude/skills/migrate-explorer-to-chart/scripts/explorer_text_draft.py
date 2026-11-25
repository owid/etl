"""Draft a multidim's `<short_name>.explorer.md` from the text an explorer shows below it.

The text below an explorer is a WordPress block (`wpBlockId` in the explorer's TSV), stored as
HTML in `posts.wpApiSnapshot`. This prints it as markdown that `SimpleMarkdownText` renders
(paragraphs, headings as `###`, links, lists, images), plus a download line when the
explorer has a `downloadDataLink`. It is a draft: read it, cut what the multidim page already
covers (e.g. "Explore more of our work" link lists, which "Research & Writing" replaces), then save
it next to the step and call `add_explorer_text` (`etl/viz/chart/explorer_text.py`).

Usage (production, read-only):
    ENV_FILE=<prod creds> DATA_API_ENV=production .venv/bin/python \
        .claude/skills/migrate-explorer-to-chart/scripts/explorer_text_draft.py <slug> [--out FILE]
"""

import argparse
import json
import re
from pathlib import Path

from bs4 import BeautifulSoup, NavigableString, Tag

from etl.config import OWID_ENV

BLOCK_TAGS = {"p", "h1", "h2", "h3", "h4", "h5", "h6", "ul", "ol", "blockquote", "figure", "img", "block", "div"}


def tsv_field(tsv: str, keyword: str) -> str | None:
    for line in tsv.splitlines():
        cells = line.split("\t")
        if cells[0] == keyword and len(cells) > 1 and cells[1].strip():
            return cells[1].strip()
    return None


def inline(node) -> str:
    """Markdown for inline content: text, links, emphasis, sub/superscripts kept as plain text."""
    if isinstance(node, NavigableString):
        return re.sub(r"\s+", " ", str(node))
    if not isinstance(node, Tag):
        return ""
    inner = "".join(inline(c) for c in node.children)
    if node.name == "a" and node.get("href"):
        href = str(node["href"]).replace("https://owid.cloud/", "https://ourworldindata.org/")
        return f"[{inner.strip()}]({href})" if inner.strip() else href
    if node.name in ("strong", "b") and inner.strip():
        return f"**{inner.strip()}**"
    if node.name in ("em", "i") and inner.strip():
        return f"_{inner.strip()}_"
    if node.name == "br":
        return "\n"
    return inner


def blocks(node: Tag) -> list[str]:
    """Markdown paragraphs for block content, in order."""
    out: list[str] = []
    for child in node.children:
        if isinstance(child, NavigableString):
            if child.strip():
                out.append(inline(child).strip())
            continue
        if not isinstance(child, Tag):
            continue
        name = child.name
        if name and re.fullmatch(r"h[1-6]", name):
            text = inline(child).strip()
            if text:
                out.append(f"### {text.strip('*')}")
        elif name == "p":
            text = inline(child).strip()
            if text:
                out.append(text)
        elif name in ("ul", "ol"):
            items = [inline(li).strip() for li in child.find_all("li", recursive=False)]
            marker = (lambda i: f"{i}.") if name == "ol" else (lambda i: "-")
            out.append("\n".join(f"{marker(i)} {t}" for i, t in enumerate(items, 1) if t))
        elif name == "blockquote":
            out.extend(f"> {b}" for b in blocks(child))
        elif name == "img":
            out.append(f"![{child.get('alt', '')}]({child.get('src', '')})")
        elif name == "block" and child.get("type") == "prominent-link":
            # WordPress "prominent link" cards: keep the link, with its title when it has one.
            url = child.find("link-url")
            title = child.find("title")
            href = url.get_text(strip=True) if url else ""
            label = inline(title).strip() if title else ""
            if href:
                out.append(f"- [{label or re.sub(r'^https?://', '', href)}]({href})")
        elif "wp-block-owid-prominent-link" in (child.get("class") or []):
            a = child.find("a")
            if a and a.get("href"):
                out.append(f"- [{child.get('data-title') or a['href']}]({a['href']})")
        else:
            out.extend(blocks(child))
    return out


def to_markdown(html: str) -> str:
    paragraphs = [p for p in blocks(BeautifulSoup(html, "html.parser")) if p.strip()]
    # Consecutive single-line list items (e.g. prominent-link cards) form one list.
    merged: list[str] = []
    for p in paragraphs:
        if merged and p.startswith("- ") and merged[-1].startswith("- "):
            merged[-1] += "\n" + p
        else:
            merged.append(p)
    return "\n\n".join(merged) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("slug")
    parser.add_argument("--out", type=Path, help="Write the draft here instead of printing it.")
    args = parser.parse_args()

    df = OWID_ENV.read_sql("SELECT tsv FROM explorers WHERE slug = %(slug)s", params={"slug": args.slug})
    if df.empty:
        raise SystemExit(f"No explorer with slug {args.slug!r}.")
    tsv = df["tsv"].iloc[0]

    parts = []
    block_id = tsv_field(tsv, "wpBlockId")
    if block_id:
        posts = OWID_ENV.read_sql("SELECT wpApiSnapshot FROM posts WHERE id = %(id)s", params={"id": int(block_id)})
        snapshot = (
            json.loads(posts["wpApiSnapshot"].iloc[0]) if not posts.empty and posts["wpApiSnapshot"].iloc[0] else {}
        )
        html = ((snapshot.get("data") or {}).get("wpBlock") or {}).get("content")
        if html:
            parts.append(to_markdown(html).strip())
        else:
            print(f"# wpBlockId {block_id} has no content in posts.wpApiSnapshot.")
    download = tsv_field(tsv, "downloadDataLink")
    if download:
        parts.append(f"**Download the data**: [download the complete dataset]({download}).")
    if not parts:
        raise SystemExit(f"{args.slug!r} has no text below it and no download link: no .explorer.md needed.")

    text = "\n\n".join(parts) + "\n"
    if args.out:
        args.out.write_text(text)
        print(f"Wrote {args.out} ({len(text):,} characters).")
    else:
        print(text)


if __name__ == "__main__":
    main()
