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


def classify_image_type(filename: str, posts_content: list[str]) -> str:
    """Classify image type based on how it's used in posts.

    Logic matches getImageType() from ImagesIndexPage.tsx:
    - content: appears in body content
    - featured-thumbnail-rw: featured/thumbnail/R&W (but not in body)
    - other: everything else
    """
    is_body_content = False
    is_featured = False
    is_thumbnail = "thumbnail" in filename.lower()
    is_in_rw = False

    # Check all posts that use this image
    for content_json in posts_content:
        try:
            data = json.loads(content_json)
        except (json.JSONDecodeError, TypeError):
            continue

        # Check if it's a featured image
        if data.get("featured-image") == filename:
            is_featured = True

        # Check if it appears in body content
        body = data.get("body", [])
        for item in body:
            if isinstance(item, dict) and item.get("type") == "image":
                if item.get("filename") == filename or item.get("smallFilename") == filename:
                    is_body_content = True
                    break

        # Check if post type is research-and-writing
        # (simplified - could check post type from separate field if needed)
        if data.get("type") == "research-and-writing":
            is_in_rw = True

    # Apply classification logic (body content takes priority)
    if is_body_content:
        return "content"

    if is_featured or is_thumbnail or is_in_rw:
        return "featured-thumbnail-rw"

    return "other"


@st.cache_data(show_spinner=False, ttl=3600)
def get_ranked_images(sort_by: str = "views_365d", image_type_filter: str = "all") -> list[dict]:
    """Get all images ranked by pageviews."""
    # First, get all images with their aggregate stats
    images_query = """
    SELECT
        i.id,
        i.filename,
        i.defaultAlt,
        i.cloudflareId,
        COUNT(DISTINCT pg.id) AS post_count,
        COALESCE(SUM(pv.views_7d), 0) AS views_7d,
        COALESCE(SUM(pv.views_365d), 0) AS views_365d
    FROM images i
    LEFT JOIN posts_gdocs_x_images pxi ON i.id = pxi.imageId
    LEFT JOIN posts_gdocs pg ON pxi.gdocId = pg.id AND pg.published = 1
    LEFT JOIN analytics_pageviews pv ON pv.url = CONCAT('https://ourworldindata.org/', pg.slug)
    WHERE i.cloudflareId IS NOT NULL
    AND i.replacedBy IS NULL
    GROUP BY i.id
    ORDER BY {} DESC
    """.format(sort_by)

    images_df = read_sql(images_query)
    records = images_df.to_dict("records")

    # Get all image-post relationships with content in one query
    posts_query = """
    SELECT
        pxi.imageId,
        CAST(pg.content AS CHAR(100000)) as content
    FROM posts_gdocs_x_images pxi
    JOIN posts_gdocs pg ON pxi.gdocId = pg.id
    WHERE pg.published = 1
    """
    posts_df = read_sql(posts_query)

    # Group posts by image ID
    posts_by_image = {}
    for _, row in posts_df.iterrows():
        image_id = row["imageId"]
        if image_id not in posts_by_image:
            posts_by_image[image_id] = []
        posts_by_image[image_id].append(row["content"])

    # Classify each image and filter
    filtered_records = []
    for record in records:
        # Get posts content for this image
        posts_content = posts_by_image.get(record["id"], [])

        # Classify the image
        image_type = classify_image_type(record["filename"], posts_content)
        record["image_type"] = image_type

        # Filter based on selected type
        if image_type_filter == "all" or image_type == image_type_filter:
            filtered_records.append(record)

    return filtered_records


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

    # Map image type to display badge
    type_badges = {"content": "📝 Content", "featured-thumbnail-rw": "⭐ Featured/Thumbnail/R&W", "other": "❓ Other"}
    badge_str = type_badges.get(image.get("image_type"), "❓ Unknown")

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
