"""Image Rank - Rank images by pageviews from posts that use them."""

import streamlit as st
from structlog import get_logger

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


def build_post_url(slug: str, post_type: str) -> str:
    """Build the full URL for a post based on its type."""
    if post_type == "data-insight":
        return f"https://ourworldindata.org/data-insights/{slug}"
    return f"https://ourworldindata.org/{slug}"


@st.cache_data(show_spinner=False, ttl=3600)
def get_ranked_images(sort_by: str = "views_365d") -> list[dict]:
    """Get all images ranked by pageviews."""
    query = """
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
    """.format(
        sort_by
    )
    df = read_sql(query)
    return df.to_dict("records")


@st.cache_data(show_spinner=False, ttl=3600)
def get_posts_for_image(image_id: int) -> list[dict]:
    """Get all posts that use a specific image."""
    query = """
    SELECT
        pg.id,
        pg.slug,
        pg.type,
        pg.content->>'$.title' as title,
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
    thumbnail_url = f"{CLOUDFLARE_IMAGES_URL}/{image['cloudflareId']}/w=200"

    with st.expander(f"**#{rank}** - {image['filename']}", expanded=False):
        col1, col2 = st.columns([1, 3])

        with col1:
            st.image(thumbnail_url, width=200)

        with col2:
            st.markdown(f"**Alt text:** {image['defaultAlt'] or 'N/A'}")
            st.markdown(f"**Views (7d):** {image['views_7d']:,}")
            st.markdown(f"**Views (365d):** {image['views_365d']:,}")
            st.markdown(f"**Posts:** {image['post_count']}")

        # Show posts using this image
        if image["post_count"] > 0:
            st.markdown("---")
            st.markdown("**Posts using this image:**")

            posts = get_posts_for_image(image["id"])
            for post in posts:
                url = build_post_url(post["slug"], post["type"])
                title = post["title"] or post["slug"]
                st.markdown(
                    f"- [{title}]({url}) ({post['type']}) - "
                    f"7d: {post['views_7d']:,} | 365d: {post['views_365d']:,}"
                )


# Main UI
st.title("Image Rank")
st.markdown("Images ranked by total pageviews from posts that use them.")

# Sort options
sort_by = st.radio(
    "Sort by",
    options=["views_365d", "views_7d"],
    format_func=lambda x: "Views (365 days)" if x == "views_365d" else "Views (7 days)",
    horizontal=True,
)

# Load data
with st.spinner("Loading images..."):
    images = get_ranked_images(sort_by)

st.markdown(f"**Total images:** {len(images):,}")

# Pagination
pagination = Pagination(images, items_per_page=20, pagination_key="image_pagination")
pagination.show_controls(mode="bar")

# Display images
start_rank = (pagination.page - 1) * pagination.items_per_page + 1
for i, image in enumerate(pagination.get_page_items()):
    rank = start_rank + i
    display_image_row(rank, image)
