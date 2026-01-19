"""
Find all posts citing a chart and generate scroll-to-text fragment URLs.

This module provides functionality to discover where charts are referenced
in OWID articles (both as embedded charts and hyperlinks) and generate
URLs that scroll directly to those citations.
"""

import json
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import streamlit as st
from sqlalchemy import text
from sqlalchemy.orm import Session

from etl.config import OWIDEnv


@dataclass
class Citation:
    """A single citation of a chart in an article."""

    type: str  # "embedded" or "hyperlink"
    link_text: str | None
    surrounding_text: str | None
    chart_url: str
    fragment_url: str


@dataclass
class ArticleCitations:
    """Citations of a chart within a single article."""

    slug: str
    post_type: str
    base_url: str
    citations: list[Citation]

    @property
    def url(self) -> str:
        return f"{self.base_url}/{self.slug}"


def extract_text_from_value(value_list: list) -> str:
    """Extract plain text from a text block value list.

    Note: Subscripts/superscripts are kept as plain text (e.g., CO2 not CO₂)
    because browser text fragment matching works on DOM text content.
    """
    text_parts = []
    for item in value_list:
        if isinstance(item, dict):
            span_type = item.get("spanType")
            if span_type == "span-simple-text":
                text_parts.append(item.get("text", ""))
            elif span_type == "span-link":
                for child in item.get("children", []):
                    if isinstance(child, dict) and child.get("spanType") == "span-simple-text":
                        text_parts.append(child.get("text", ""))
            elif span_type in ("span-subscript", "span-superscript"):
                # Keep as plain text for text fragment matching
                for child in item.get("children", []):
                    if isinstance(child, dict):
                        text_parts.append(child.get("text", ""))
            elif "text" in item:
                text_parts.append(item["text"])
    return "".join(text_parts)


def truncate_to_sentence_or_word(text: str, max_length: int) -> str:
    """Truncate text to max_length, preferring sentence or word boundaries."""
    if len(text) <= max_length:
        return text

    truncated = text[:max_length]

    # Try to end at a sentence boundary (. ! ?)
    for punct in ".!?":
        last_punct = truncated.rfind(punct)
        if last_punct > max_length // 2:  # Don't truncate too much
            return truncated[: last_punct + 1]

    # Fall back to word boundary
    last_space = truncated.rfind(" ")
    if last_space > max_length // 2:
        return truncated[:last_space]

    return truncated


def extract_context_around_link(full_text: str, link_text: str, max_length: int = 200) -> str:
    """Extract text centered around the link text."""
    pos = full_text.find(link_text)
    if pos == -1:
        # Fallback to beginning if link text not found
        return truncate_to_sentence_or_word(full_text, max_length)

    # Calculate window around link
    link_end = pos + len(link_text)
    half_context = (max_length - len(link_text)) // 2

    start = max(0, pos - half_context)
    end = min(len(full_text), link_end + half_context)

    # Adjust if we hit boundaries
    if start == 0:
        end = min(len(full_text), max_length)
    elif end == len(full_text):
        start = max(0, len(full_text) - max_length)

    context = full_text[start:end]

    # Trim to word boundaries at edges
    if start > 0:
        # Trim partial word at start
        first_space = context.find(" ")
        if first_space > 0 and first_space < 20:
            context = context[first_space + 1 :]

    if end < len(full_text):
        # Trim partial word/sentence at end
        # Find last sentence boundary
        last_period = context.rfind(".")
        if last_period > len(context) // 2:
            context = context[: last_period + 1]
        else:
            # Fall back to word boundary
            last_space = context.rfind(" ")
            if last_space > len(context) // 2:
                context = context[:last_space]

    return context


def create_text_fragment_url(base_url: str, full_text: str, link_text: str | None = None, max_length: int = 200) -> str:
    """Create a scroll-to-text fragment URL, centered around link_text if provided."""
    if link_text:
        text_to_highlight = extract_context_around_link(full_text, link_text, max_length)
    else:
        text_to_highlight = truncate_to_sentence_or_word(full_text, max_length)

    # Keep parentheses unencoded (browsers need them literal for text fragments)
    # but encode everything else including hyphens
    encoded = quote(text_to_highlight, safe="()")
    # Hyphens must be encoded as %2D for text fragments to work in browsers
    encoded = encoded.replace("-", "%2D")
    return f"{base_url}#:~:text={encoded}"


def extract_heading_text(heading_obj: dict) -> str:
    """Extract text from a heading object."""
    text_content = heading_obj.get("text", [])
    if isinstance(text_content, list):
        return extract_text_from_value(text_content)
    return str(text_content) if text_content else ""


def heading_to_slug(heading_text: str) -> str:
    """Convert heading text to URL slug (matching OWID's slugification)."""
    slug = heading_text.lower()
    # Replace spaces and special chars with hyphens
    slug = re.sub(r"[^\w\s-]", "", slug)
    slug = re.sub(r"[\s_]+", "-", slug)
    slug = re.sub(r"-+", "-", slug)
    slug = slug.strip("-")
    return slug


def find_context_before_chart(body: list, chart_index: int) -> tuple[str | None, str | None]:
    """Find heading or text content before a chart in the body.

    Returns (context_text, heading_slug) - heading_slug is set if a heading was found.
    Prefers headings over text blocks for more reliable anchoring.
    """
    # First, look for a heading (search further back)
    for i in range(chart_index - 1, max(0, chart_index - 10) - 1, -1):
        item = body[i]
        if isinstance(item, dict) and item.get("type") == "heading":
            text = extract_heading_text(item)
            slug = heading_to_slug(text)
            return text, slug

    # Fall back to nearest text block
    for i in range(chart_index - 1, max(0, chart_index - 5) - 1, -1):
        item = body[i]
        if isinstance(item, dict) and item.get("type") == "text":
            value = item.get("value", [])
            if isinstance(value, list):
                text = extract_text_from_value(value)
                if text:
                    return text, None

    return None, None


def find_chart_citations_in_content(content: str, slug: str, chart_slug: str, base_site_url: str) -> list[Citation]:
    """Find all citations of a chart in post content JSON.

    Args:
        content: JSON content of the post
        slug: Post slug
        chart_slug: Chart slug to search for
        base_site_url: Base URL for the site (e.g., https://ourworldindata.org)
    """
    citations = []
    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        return citations

    base_url = f"{base_site_url}/{slug}"
    body = data.get("body", [])
    seen_urls: set[str] = set()  # Track URLs to avoid duplicates

    # First pass: find embedded charts in body with their context
    for i, item in enumerate(body):
        if isinstance(item, dict) and item.get("type") == "chart":
            url = item.get("url", "")
            if f"/grapher/{chart_slug}" in url:
                # Find heading or text before chart
                context, heading_slug = find_context_before_chart(body, i)
                if heading_slug:
                    # Use anchor fragment if we found a heading
                    fragment_url = f"{base_url}#{heading_slug}"
                elif context:
                    # Fall back to text fragment
                    fragment_url = create_text_fragment_url(base_url, context, context)
                else:
                    fragment_url = base_url
                citations.append(
                    Citation(
                        type="embedded",
                        link_text=None,
                        surrounding_text=context,
                        chart_url=url,
                        fragment_url=fragment_url,
                    )
                )

    # Second pass: find hyperlinks in all text blocks
    def search_hyperlinks(obj: Any, parent_text: str | None = None) -> None:
        if isinstance(obj, dict):
            # Check if this is a text block - capture its text for context
            if obj.get("type") == "text" and "value" in obj:
                value = obj["value"]
                if isinstance(value, list):
                    full_text = extract_text_from_value(value)
                    for item in value:
                        if isinstance(item, dict) and item.get("spanType") == "span-link":
                            url = item.get("url", "")
                            if f"/grapher/{chart_slug}" in url:
                                link_text = extract_text_from_value(item.get("children", []))
                                if url not in seen_urls:
                                    seen_urls.add(url)
                                    citations.append(
                                        Citation(
                                            type="hyperlink",
                                            link_text=link_text,
                                            surrounding_text=full_text,
                                            chart_url=url,
                                            fragment_url=create_text_fragment_url(base_url, full_text, link_text),
                                        )
                                    )
            # Check for standalone span-links (e.g., in additional-charts)
            elif obj.get("spanType") == "span-link":
                url = obj.get("url", "")
                if f"/grapher/{chart_slug}" in url and url not in seen_urls:
                    seen_urls.add(url)
                    link_text = extract_text_from_value(obj.get("children", []))
                    # Use link text itself as context if no parent text
                    context = parent_text or link_text
                    citations.append(
                        Citation(
                            type="hyperlink",
                            link_text=link_text,
                            surrounding_text=context,
                            chart_url=url,
                            fragment_url=create_text_fragment_url(base_url, context, link_text),
                        )
                    )
            for v in obj.values():
                search_hyperlinks(v, parent_text)
        elif isinstance(obj, list):
            for item in obj:
                search_hyperlinks(item, parent_text)

    search_hyperlinks(data)

    return citations


@st.cache_data(show_spinner=False)
def find_all_citations(_session: Session, chart_slug: str, base_site_url: str) -> list[ArticleCitations]:
    """Find all posts citing the given chart.

    Args:
        _session: Database session (underscore prefix for Streamlit cache)
        chart_slug: The chart's URL slug
        base_site_url: Base URL for the site (e.g., https://ourworldindata.org)

    Returns:
        List of ArticleCitations, one per article that cites the chart
    """
    # Find all posts that reference this chart via posts_gdocs_links
    query = text("""
        SELECT DISTINCT p.id, p.slug, p.type, p.content
        FROM posts_gdocs_links l
        JOIN posts_gdocs p ON l.sourceId = p.id
        WHERE l.linkType = 'grapher' AND l.target = :chart_slug
        AND p.published = 1
    """)

    results = []
    rows = _session.execute(query, {"chart_slug": chart_slug}).fetchall()

    for row in rows:
        _post_id, slug, post_type, content = row
        citations = find_chart_citations_in_content(content, slug, chart_slug, base_site_url)
        if citations:
            results.append(
                ArticleCitations(
                    slug=slug,
                    post_type=post_type,
                    base_url=base_site_url,
                    citations=citations,
                )
            )

    return results


def _truncate_context_around_link(context: str, link_text: str | None, max_len: int = 100) -> str:
    """Truncate context text, centering around and highlighting the link text.

    Args:
        context: Full context text
        link_text: The hyperlink text to highlight (or None for embedded charts)
        max_len: Maximum length of the output (excluding markdown bold markers)

    Returns:
        Truncated context with link text in bold, e.g.:
        "...small population meaning their total **annual emissions** are low."
    """
    if not link_text or link_text not in context:
        # No link text, just truncate from start
        if len(context) > max_len:
            return context[:max_len] + "..."
        return context

    link_pos = context.find(link_text)
    link_end = link_pos + len(link_text)

    # Calculate how much context to show before and after
    available = max_len - len(link_text)
    before = min(link_pos, available // 2)
    after = min(len(context) - link_end, available - before)

    # Adjust if we have room on one side
    if before < available // 2:
        after = min(len(context) - link_end, available - before)
    if after < available // 2:
        before = min(link_pos, available - after)

    start = link_pos - before
    end = link_end + after

    # Build the truncated context with highlighted link
    prefix = "..." if start > 0 else ""
    suffix = "..." if end < len(context) else ""
    snippet = context[start:link_pos] + f"**{link_text}**" + context[link_end:end]
    return prefix + snippet + suffix


@dataclass
class MergedCitation:
    """A citation merged from both environments for unified display."""

    article_slug: str
    post_type: str
    citation_type: str  # "embedded" or "hyperlink"
    context: str | None
    link_text: str | None  # The hyperlink text (for hyperlink citations)
    chart_url: str  # The grapher URL with query params (e.g., /grapher/chart?country=USA)
    prod_url: str | None  # Fragment URL for production article
    staging_url: str | None  # Fragment URL for staging article


def _chart_url_to_thumbnail(chart_url: str, base_site_url: str) -> str:
    """Convert a chart URL to a thumbnail SVG URL.

    Args:
        chart_url: Chart URL which may be absolute or relative, like:
            - https://ourworldindata.org/grapher/chart?country=USA
            - /grapher/chart?country=USA
        base_site_url: Base site URL like https://ourworldindata.org

    Returns:
        Full thumbnail URL like https://ourworldindata.org/grapher/chart.svg?country=USA
    """
    # Extract the path portion (everything after /grapher/)
    if "/grapher/" in chart_url:
        # Get everything from /grapher/ onwards
        grapher_idx = chart_url.index("/grapher/")
        path_and_query = chart_url[grapher_idx:]

        # Insert .svg before query params
        if "?" in path_and_query:
            path, query = path_and_query.split("?", 1)
            return f"{base_site_url}{path}.svg?{query}"
        else:
            return f"{base_site_url}{path_and_query}.svg"

    # Fallback: just append .svg
    return f"{chart_url}.svg"


def _merge_citations(
    target_citations: list[ArticleCitations],
    source_citations: list[ArticleCitations],
) -> list[MergedCitation]:
    """Merge citations from both environments into a unified list.

    Context is assumed to be identical between environments since it comes
    from the same article content.
    """
    merged: list[MergedCitation] = []

    # Build lookup for staging citations by (article_slug, index)
    source_by_article: dict[str, list[Citation]] = {}
    for article in source_citations:
        source_by_article[article.slug] = article.citations

    # Use production as primary, add staging URLs where available
    seen_articles: set[str] = set()
    for article in target_citations:
        seen_articles.add(article.slug)
        source_list = source_by_article.get(article.slug, [])

        for i, citation in enumerate(article.citations):
            # Try to match with staging citation at same index
            staging_url = source_list[i].fragment_url if i < len(source_list) else None

            merged.append(
                MergedCitation(
                    article_slug=article.slug,
                    post_type=article.post_type,
                    citation_type=citation.type,
                    context=citation.surrounding_text,
                    link_text=citation.link_text,
                    chart_url=citation.chart_url,
                    prod_url=citation.fragment_url,
                    staging_url=staging_url,
                )
            )

    # Add any staging-only articles
    for article in source_citations:
        if article.slug not in seen_articles:
            for citation in article.citations:
                merged.append(
                    MergedCitation(
                        article_slug=article.slug,
                        post_type=article.post_type,
                        citation_type=citation.type,
                        context=citation.surrounding_text,
                        link_text=citation.link_text,
                        chart_url=citation.chart_url,
                        prod_url=None,
                        staging_url=citation.fragment_url,
                    )
                )

    return merged


def st_show_citations(
    chart_slug: str,
    source_session: Session,
    target_session: Session,
    source_env: OWIDEnv,
    target_env: OWIDEnv,
) -> None:
    """Display citations for a chart in Streamlit UI as a unified table.

    Args:
        chart_slug: The chart's URL slug
        source_session: Database session for staging
        target_session: Database session for production
        source_env: Staging environment config
        target_env: Production environment config
    """
    if not chart_slug:
        return

    with st.spinner("Finding article citations..."):
        # Get citations from both environments
        source_citations = find_all_citations(source_session, chart_slug, source_env.site)
        target_citations = find_all_citations(target_session, chart_slug, target_env.site)

    # Only show section if there are citations in either environment
    if not source_citations and not target_citations:
        return

    st.markdown("##### :material/format_quote: Article Citations")

    # Merge citations from both environments
    merged = _merge_citations(target_citations, source_citations)

    # Build markdown table with thumbnails
    rows = []
    rows.append("| Article | Type | Context | Production | Staging |")
    rows.append("|---------|------|---------|------------|---------|")

    for citation in merged:
        # Article column
        article = f"**{citation.article_slug}** ({citation.post_type})"

        # Type badge
        if citation.citation_type == "embedded":
            type_badge = "Embedded"
        else:
            type_badge = "Hyperlink"

        # Context (truncated), with link text highlighted in bold
        if citation.context:
            context = _truncate_context_around_link(citation.context, citation.link_text)
            # Escape pipe characters and newlines for markdown table
            context = context.replace("|", "\\|").replace("\n", " ")
        else:
            context = "-"

        # Thumbnails and links for each environment
        prod_thumb = _chart_url_to_thumbnail(citation.chart_url, target_env.site)
        staging_thumb = _chart_url_to_thumbnail(citation.chart_url, source_env.site)

        if citation.prod_url:
            prod_cell = f"![thumb]({prod_thumb}) [View]({citation.prod_url})"
        else:
            prod_cell = "-"

        if citation.staging_url:
            staging_cell = f"![thumb]({staging_thumb}) [View]({citation.staging_url})"
        else:
            staging_cell = "-"

        rows.append(f"| {article} | {type_badge} | {context} | {prod_cell} | {staging_cell} |")

    st.markdown("\n".join(rows))
