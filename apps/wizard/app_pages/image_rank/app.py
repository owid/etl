"""Image Rank - Rank images by pageviews from posts that use them."""

import json

import streamlit as st
from structlog import get_logger

from apps.wizard.app_pages.chart_diff.citations import (
    _build_post_url,
    create_text_fragment_url,
    extract_heading_text,
    heading_to_slug,
)
from apps.wizard.utils.components import Pagination
from etl.db import read_sql

log = get_logger()

# Page config
st.set_page_config(
    page_title="Wizard: Image Rank",
    page_icon="🪄",
    layout="wide",
)

# Cloudflare images base URL
CLOUDFLARE_IMAGES_URL = "https://imagedelivery.net/qLq-8BTgXU8yG0N6HnOy8g"
BASE_SITE_URL = "https://ourworldindata.org"


def find_image_in_content(content: str, filename: str, slug: str, post_type: str) -> str | None:
    """Find the image in post content and return a fragment URL to its location.

    Args:
        content: JSON content of the post
        filename: Image filename to search for
        slug: Post slug
        post_type: Post type (e.g., 'article', 'data-insight')

    Returns:
        Fragment URL pointing to the image location, or None if not found
    """
    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        return None

    base_url = _build_post_url(BASE_SITE_URL, slug, post_type)

    # Check if it's the featured image (appears at top of page)
    if data.get("featured-image") == filename:
        return base_url

    body = data.get("body", [])

    # Find the image in body
    for i, item in enumerate(body):
        if isinstance(item, dict) and item.get("type") == "image":
            # Check both filename and smallFilename
            if item.get("filename") == filename or item.get("smallFilename") == filename:
                # Find heading before image (reuse logic from citations.py)
                context, heading_slug = _find_context_before_item(body, i)
                if heading_slug:
                    return f"{base_url}#{heading_slug}"
                elif context:
                    return create_text_fragment_url(base_url, context, context)
                return base_url

    return None


def _find_context_before_item(body: list, item_index: int) -> tuple[str | None, str | None]:
    """Find heading or text content before an item in the body.

    Returns (context_text, heading_slug) - heading_slug is set if a heading was found.
    Reuses the same logic as find_context_before_chart from citations.py.
    """
    # First, look for a heading (search further back)
    for i in range(item_index - 1, max(0, item_index - 10) - 1, -1):
        item = body[i]
        if isinstance(item, dict) and item.get("type") == "heading":
            text = extract_heading_text(item)
            slug = heading_to_slug(text)
            return text, slug

    # Fall back to nearest text block
    for i in range(item_index - 1, max(0, item_index - 5) - 1, -1):
        item = body[i]
        if isinstance(item, dict) and item.get("type") == "text":
            from apps.wizard.app_pages.chart_diff.citations import extract_text_from_value

            value = item.get("value", [])
            if isinstance(value, list):
                text = extract_text_from_value(value)
                if text:
                    return text, None

    return None, None


@st.cache_data(show_spinner=False, ttl=3600)
def get_ranked_images(sort_by: str = "views_365d", image_type_filter: str = "all") -> list[dict]:
    """Get all images ranked by pageviews."""
    # Build WHERE clause based on image type filter
    # Logic matches getImageType() from ImagesIndexPage.tsx
    type_filter = ""
    if image_type_filter == "content":
        # If an image is used in body content, it's in content category
        type_filter = "AND i.isBodyContent = 1"
    elif image_type_filter == "featured-thumbnail-rw":
        # Featured, thumbnails, or R&W (only if NOT in body content)
        type_filter = """
        AND i.isBodyContent = 0
        AND (i.isFeaturedImage = 1 OR LOWER(i.filename) LIKE '%thumbnail%' OR i.isInResearchAndWriting = 1)
        """
    elif image_type_filter == "other":
        # Everything else (fallback): not in body content and not featured/thumbnail/rw
        type_filter = """
        AND i.isBodyContent = 0
        AND i.isFeaturedImage = 0
        AND LOWER(i.filename) NOT LIKE '%thumbnail%'
        AND (i.isInResearchAndWriting = 0 OR i.isInResearchAndWriting IS NULL)
        """

    query = """
    SELECT
        i.id,
        i.filename,
        i.defaultAlt,
        i.cloudflareId,
        i.isFeaturedImage,
        i.isBodyContent,
        i.isInResearchAndWriting,
        COUNT(DISTINCT pg.id) AS post_count,
        COALESCE(SUM(pv.views_7d), 0) AS views_7d,
        COALESCE(SUM(pv.views_365d), 0) AS views_365d
    FROM images i
    LEFT JOIN posts_gdocs_x_images pxi ON i.id = pxi.imageId
    LEFT JOIN posts_gdocs pg ON pxi.gdocId = pg.id AND pg.published = 1
    LEFT JOIN analytics_pageviews pv ON pv.url = CONCAT('https://ourworldindata.org/', pg.slug)
    WHERE i.cloudflareId IS NOT NULL
    AND i.replacedBy IS NULL
    {}
    GROUP BY i.id
    ORDER BY {} DESC
    """.format(type_filter, sort_by)
    df = read_sql(query)
    return df.to_dict("records")


@st.cache_data(show_spinner=False, ttl=3600)
def get_posts_for_image(image_id: int) -> list[dict]:
    """Get all posts that use a specific image, including content for fragment URLs."""
    query = """
    SELECT
        pg.id,
        pg.slug,
        pg.type,
        pg.content->>'$.title' as title,
        pg.content as content,
        COALESCE(pv.views_7d, 0) as views_7d,
        COALESCE(pv.views_365d, 0) as views_365d
    FROM posts_gdocs_x_images pxi
    JOIN posts_gdocs pg ON pxi.gdocId = pg.id
    LEFT JOIN analytics_pageviews pv ON pv.url = CONCAT('https://ourworldindata.org/', pg.slug)
    WHERE pxi.imageId = %s
    AND pg.published = 1
    ORDER BY pv.views_365d DESC
    """
    df = read_sql(query, params=(image_id,))
    return df.to_dict("records")


def display_image_row(rank: int, image: dict) -> None:
    """Display a single image row with expandable posts."""
    thumbnail_url = f"{CLOUDFLARE_IMAGES_URL}/{image['cloudflareId']}/w=400"
    filename = image["filename"]

    # Determine image type badges
    badges = []
    if image.get("isBodyContent") == 1:
        badges.append("📝 Content")
    if image.get("isFeaturedImage") == 1:
        badges.append("⭐ Featured")
    if image.get("isInResearchAndWriting") == 1:
        badges.append("📚 R&W")
    if "thumbnail" in filename.lower():
        badges.append("🖼️ Thumbnail")
    badge_str = " ".join(badges) if badges else "❓ Other"

    with st.expander(f"**#{rank}** - {filename} | {badge_str}", expanded=True):
        col1, col2 = st.columns([1, 2])

        with col1:
            st.image(thumbnail_url, width="stretch")

        with col2:
            st.markdown(f"**Alt text:** {image['defaultAlt'] or 'N/A'}")
            views_per_day = image["views_365d"] / 365
            st.markdown(
                f"**Views (7d):** {int(image['views_7d']):,} | "
                f"**Views (365d):** {int(image['views_365d']):,} | "
                f"**Views per day:** {views_per_day:,.1f}"
            )

            # Show posts using this image
            if image["post_count"] > 0:
                st.markdown(f"**Posts using this image:** ({image['post_count']})")

                posts = get_posts_for_image(image["id"])
                for post in posts:
                    # Try to get a fragment URL pointing to the image location
                    fragment_url = None
                    if post.get("content"):
                        fragment_url = find_image_in_content(post["content"], filename, post["slug"], post["type"])
                    # Fall back to base post URL
                    if not fragment_url:
                        fragment_url = _build_post_url(BASE_SITE_URL, post["slug"], post["type"])

                    title = post["title"] or post["slug"]
                    st.markdown(
                        f"- [{title}]({fragment_url}) ({post['type']}) - "
                        f"7d: {int(post['views_7d']):,} | 365d: {int(post['views_365d']):,}"
                    )


# Main UI
st.title("Image Rank")
st.markdown("Images ranked by total pageviews from posts that use them.")

# Filters
col1, col2 = st.columns(2)

with col1:
    # Sort options
    sort_by = st.radio(
        "Sort by",
        options=["views_365d", "views_7d"],
        format_func=lambda x: "Views (365 days)" if x == "views_365d" else "Views (7 days)",
        horizontal=True,
    )

with col2:
    # Image type filter
    image_type_filter = st.radio(
        "Image type",
        options=["all", "content", "featured-thumbnail-rw", "other"],
        format_func=lambda x: {
            "all": "All images",
            "content": "Content",
            "featured-thumbnail-rw": "Thumbnails or featured images",
            "other": "Other",
        }[x],
        horizontal=True,
        index=1,  # Default to "content"
    )

# Load data
with st.spinner("Loading images..."):
    images = get_ranked_images(sort_by, image_type_filter)

st.markdown(f"**Total images:** {len(images):,}")

# Pagination
pagination = Pagination(images, items_per_page=20, pagination_key="image_pagination")
pagination.show_controls(mode="bar")

# Display images
start_rank = (pagination.page - 1) * pagination.items_per_page + 1
for i, image in enumerate(pagination.get_page_items()):
    rank = start_rank + i
    display_image_row(rank, image)
