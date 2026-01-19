"""
Find all posts citing a chart and generate scroll-to-text fragment URLs.

This module provides functionality to discover where charts are referenced
in OWID articles (both as embedded charts and hyperlinks) and generate
URLs that scroll directly to those citations.
"""

import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import streamlit as st
from sqlalchemy import text
from sqlalchemy.orm import Session


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
    citations: list[Citation]

    @property
    def url(self) -> str:
        return f"https://ourworldindata.org/{self.slug}"


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
    import re

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


def find_chart_citations_in_content(content: str, slug: str, chart_slug: str) -> list[Citation]:
    """Find all citations of a chart in post content JSON."""
    citations = []
    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        return citations

    base_url = f"https://ourworldindata.org/{slug}"
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
def find_all_citations(_session: Session, chart_slug: str) -> list[ArticleCitations]:
    """Find all posts citing the given chart.

    Args:
        _session: Database session (underscore prefix for Streamlit cache)
        chart_slug: The chart's URL slug

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
        citations = find_chart_citations_in_content(content, slug, chart_slug)
        if citations:
            results.append(
                ArticleCitations(
                    slug=slug,
                    post_type=post_type,
                    citations=citations,
                )
            )

    return results


def st_show_citations(chart_slug: str, session: Session) -> None:
    """Display citations for a chart in Streamlit UI.

    Args:
        chart_slug: The chart's URL slug
        session: Database session for queries
    """
    if not chart_slug:
        return

    with st.spinner("Finding article citations..."):
        article_citations = find_all_citations(session, chart_slug)

    if not article_citations:
        return

    st.markdown("##### :material/format_quote: Article Citations")

    for article in article_citations:
        with st.expander(f"**{article.slug}** ({article.post_type})", expanded=False):
            st.caption(article.url)

            for i, citation in enumerate(article.citations, 1):
                with st.container(border=True):
                    # Citation type badge
                    if citation.type == "embedded":
                        st.markdown(f"**[{i}]** :blue-badge[EMBEDDED]")
                    else:
                        st.markdown(f"**[{i}]** :orange-badge[HYPERLINK]")

                    # Link text (for hyperlinks)
                    if citation.link_text:
                        st.markdown(f'Link text: "{citation.link_text}"')

                    # Context preview
                    if citation.surrounding_text:
                        context_preview = citation.surrounding_text[:150]
                        if len(citation.surrounding_text) > 150:
                            context_preview += "..."
                        st.caption(f'Context: "{context_preview}"')

                    # Fragment URL
                    st.markdown(f"[View in article]({citation.fragment_url})")
