"""
OWID Posts MCP Server Module
----------------------------
Provides post markdown content retrieval functionality for Our World in Data posts.
"""

from typing import Any, Dict, Optional

import structlog
from fastmcp import FastMCP

from owid_mcp.data_utils import run_sql

log = structlog.get_logger()

INSTRUCTIONS = (
    "Fetch markdown content for Our World in Data posts.\n\n"
    "AVAILABLE TOOLS:\n"
    "• `fetch_post` - Fetch markdown content for a post by slug or Google Doc ID\n"
    "• `search_posts` - Search for posts by title or content\n\n"
    "USAGE:\n"
    "• Use the post slug (e.g., 'poverty', 'climate-change') or Google Doc ID to fetch content\n"
    "• Returns post data including slug, title, and full markdown content\n"
    "• Content is fetched from the public OWID database via Datasette\n\n"
    "EXAMPLES:\n"
    "• fetch_post('poverty') - Fetch post by slug\n"
    "• fetch_post('1BxGqJY9sHdW8s4K2lL3N4s7g5F6H') - Fetch by Google Doc ID\n"
    "• search_posts('climate change') - Search for posts about climate change"
)

mcp = FastMCP()


async def _fetch_post_by_identifier(identifier: str) -> Optional[Dict[str, Any]]:
    """
    Fetch post data by slug or Google Doc ID using the public Datasette API.

    Args:
        identifier: The post slug or Google Doc ID to fetch

    Returns:
        Dict with 'slug', 'title', 'markdown' keys if found, None otherwise
    """
    # Try to fetch by ID first (assuming identifier is a Google Doc ID if it looks like one)
    if len(identifier) > 20 and not identifier.count("-") > 3:  # Likely a Google Doc ID
        query = f"SELECT slug, content -> '$.title' as title, markdown FROM posts_gdocs WHERE id = '{identifier}'"
        result = await run_sql(query, max_rows=1)
    else:
        # Try by slug first for shorter identifiers
        query = f"SELECT slug, content -> '$.title' as title, markdown FROM posts_gdocs WHERE slug = '{identifier}'"
        result = await run_sql(query, max_rows=1)

    # Check if we got results
    if result["rows"]:
        row = result["rows"][0]
        return {
            "slug": row[0] if row[0] else "",
            "title": row[1] if row[1] else "",
            "markdown": row[2] if row[2] else "",
        }

    # If no results with first attempt, try the other approach
    if len(identifier) > 20 and not identifier.count("-") > 3:
        # Was trying by ID, now try by slug
        query = f"SELECT slug, content -> '$.title' as title, markdown FROM posts_gdocs WHERE slug = '{identifier}'"
        result = await run_sql(query, max_rows=1)
    else:
        # Was trying by slug, now try by ID
        query = f"SELECT slug, content -> '$.title' as title, markdown FROM posts_gdocs WHERE id = '{identifier}'"
        result = await run_sql(query, max_rows=1)

    if result["rows"]:
        row = result["rows"][0]
        return {
            "slug": row[0] if row[0] else "",
            "title": row[1] if row[1] else "",
            "markdown": row[2] if row[2] else "",
        }

    return None


@mcp.tool
async def fetch_post(identifier: str, include_metadata: bool = False) -> Dict[str, Any]:
    """
    Fetch markdown content for a post by slug or Google Doc ID from the OWID database.

    Args:
        identifier: The post slug (e.g., "poverty") or Google Doc ID to fetch
        include_metadata: Whether to include title and slug metadata (default: False)

    Returns:
        Dict containing the post content in markdown format:
        - {"content": "markdown_text", "metadata": {"slug": "...", "title": "...", "length": ...}}
    """
    log.info("fetch_post", identifier=identifier)

    try:
        post_data = await _fetch_post_by_identifier(identifier)

        if post_data is None:
            return {"error": f"No post found with identifier: {identifier}", "content": ""}

        # Prepare content in markdown format
        if include_metadata:
            content = f"# {post_data['title']}\n\nSlug: {post_data['slug']}\n\n{post_data['markdown']}"
            return {"content": content, "metadata": post_data}
        else:
            # Default markdown format
            return {
                "content": post_data["markdown"],
                "metadata": {
                    "slug": post_data["slug"],
                    "title": post_data["title"],
                    "length": len(post_data["markdown"]),
                },
            }

    except Exception as e:
        log.error("fetch_post.error", identifier=identifier, error=str(e))
        return {"error": f"Error fetching post: {e}", "content": ""}


@mcp.tool
async def search_posts(query: str, limit: int = 10) -> Dict[str, Any]:
    """
    Search for posts by title or content.

    Args:
        query: Search term to look for in post titles or content
        limit: Maximum number of results to return (default: 10)

    Returns:
        Dict with search results containing slug, title, and excerpt
    """
    log.info("search_posts", query=query, limit=limit)

    # Search in titles and content using LIKE
    sql_query = f"""
    SELECT
        slug,
        content -> '$.title' as title,
        SUBSTR(markdown, 1, 200) as excerpt
    FROM posts_gdocs
    WHERE
        (content -> '$.title' LIKE '%{query}%' OR markdown LIKE '%{query}%')
        AND slug IS NOT NULL
        AND markdown IS NOT NULL
    ORDER BY
        CASE WHEN content -> '$.title' LIKE '%{query}%' THEN 1 ELSE 2 END,
        slug
    LIMIT {limit}
    """

    result = await run_sql(sql_query, max_rows=limit)

    posts = []
    for row in result["rows"]:
        slug, title, excerpt = row
        posts.append(
            {
                "slug": slug or "",
                "title": title or "",
                "excerpt": excerpt or "",
                "url": f"https://ourworldindata.org/{slug}" if slug else "",
            }
        )

    return {"query": query, "results": posts, "count": len(posts)}
