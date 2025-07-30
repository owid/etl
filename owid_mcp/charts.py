import io
import urllib.parse
from typing import Any, Dict, List, Optional

import httpx
import pandas as pd
import structlog
from fastmcp import FastMCP
from fastmcp.utilities.types import Image
from pydantic import BaseModel

from mcp.types import ImageContent
from owid_mcp.config import HTTP_TIMEOUT
from owid_mcp.data_utils import make_algolia_request

log = structlog.get_logger()


class ChartDataResult(BaseModel):
    """Chart data fetch result."""

    id: str  # unique ID for the document or search result item
    title: str  # string title for the search result item
    text: str  # full text of the document or item
    url: str  # URL to the document or search result item
    metadata: Optional[Dict[str, Any]] = None  # optional key/value pairing of data


class ChartSearchResult(BaseModel):
    """Chart search result item."""

    id: str  # unique ID for the document or search result item
    title: str  # string title for the search result item
    text: str  # relevant snippet of text for the search terms
    url: str  # URL to the document or search result item


INSTRUCTIONS = (
    "CHART TOOLS (alternative to indicator tools):\n"
    "• `search_chart` - Find grapher charts via Algolia search\n"
    "• `fetch_chart_data` - Download processed CSV data with optional time filtering\n"
    "• `fetch_chart_image` - Get PNG images of charts\n\n"
    "INTERACTIVE CHARTS:\n"
    "• ALWAYS show users the interactive chart links from search results\n"
    "• Remove '.csv' from URLs to get interactive version\n"
    "• Example: https://ourworldindata.org/grapher/population-density.csv → https://ourworldindata.org/grapher/population-density\n"
    "• Tell users they can explore data interactively, change selections, and view different chart types\n\n"
    "USAGE:\n"
    "• Include country names in search: 'population France', 'emissions China'\n"
    "• Use time filtering in fetch (e.g., '1990..2010', 'earliest..2010', '1990..latest')\n"
    "• Returns CSV with Entity column removed if Code column has no empty values (Code, Year, metrics)"
)

mcp = FastMCP()


@mcp.tool
async def fetch_chart_data(id: str, time: Optional[str] = None) -> ChartDataResult:
    """Download a grapher CSV and return the processed data.

    The returned CSV includes:
    - Entity: Country/region names (removed if Code column has no empty values)
    - Code: Country/region ISO codes
    - Year: Time period
    - [Metric columns]: The actual data values

    IMPORTANT: Only provide data that exists in the fetched CSV. Do not hallucinate
    or interpolate missing values.

    Args:
        id: The chart slug (e.g., 'population-density') returned by `search_chart`.
        time: Optional time range filter (e.g., '1990..2010', 'earliest..2010', '1990..latest').

    Returns:
        ChartDataResult with `text` containing processed CSV data and metadata about
        the dataset structure.
    """
    log.info("chart.fetch_data", slug=id, time=time)

    # Construct CSV URL from slug
    csv_url = f"https://ourworldindata.org/grapher/{id}.csv"

    # Build query parameters
    query_params = {"tab": "line", "csvType": "filtered"}

    # Add time parameter if provided
    if time:
        query_params["time"] = time

    query_string = urllib.parse.urlencode(query_params)
    fetch_url = f"{csv_url}?{query_string}"

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        resp = await client.get(fetch_url)
        resp.raise_for_status()
        csv_content = resp.text

    # Process CSV - remove Entity column if Code column has no empty values
    df = pd.read_csv(io.StringIO(csv_content))

    # Check if Code column exists and has no empty/null values
    if "Code" in df.columns and "Entity" in df.columns:
        # Check for empty, null, or whitespace-only values in Code column
        code_has_empty = df["Code"].isna().any() or (df["Code"].astype(str).str.strip() == "").any()
        if not code_has_empty:
            # Remove Entity column as it's redundant when Code is complete
            df = df.drop(columns=["Entity"])

    # Convert back to CSV string
    processed_csv: str = df.to_csv(index=False)

    title = id.replace("-", " ").title()

    return ChartDataResult(
        id=id,
        title=title,
        text=processed_csv,
        url=fetch_url,
        metadata={
            "mime": "text/csv",
            "encoding": "utf-8",
            "size_bytes": len(processed_csv.encode("utf-8")),
            "rows": len(df),
            "columns": list(df.columns),
            "time_filter": time,
        },
    )


@mcp.tool
async def fetch_chart_image(id: str, time: Optional[str] = None) -> ImageContent:
    """Download a grapher PNG image by converting chart slug to PNG URL.

    Takes a chart slug from search results and constructs a PNG URL to download
    the chart image as base64-encoded content.

    Args:
        id: The chart slug (e.g., 'population-density') returned by `search_chart`.
        time: Optional time range filter (e.g., '1990..2010', 'earliest..2010', '1990..latest').

    Returns:
        ImageContent with base64-encoded PNG data.
    """
    log.info("chart.fetch_image", slug=id, time=time)

    # Construct PNG URL from slug
    png_url = f"https://ourworldindata.org/grapher/{id}.png"

    # Build query parameters for PNG
    png_params = {"tab": "chart"}

    # Add time parameter if provided
    if time:
        png_params["time"] = time

    # Construct final URL with parameters
    query_string = urllib.parse.urlencode(png_params)
    png_url = f"{png_url}?{query_string}"

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            resp = await client.get(png_url)
            resp.raise_for_status()
            png_bytes = resp.content

        # Use FastMCP Image utility to create proper ImageContent
        img_obj = Image(data=png_bytes, format="png")
        return img_obj.to_image_content()

    except Exception as exc:
        log.warning("chart.fetch_image.error", url=png_url, error=str(exc))
        raise ValueError(f"Failed to download PNG: {exc}")


@mcp.tool
async def search_chart(query: str) -> List[ChartSearchResult]:
    """Search OWID using Algolia and return grapher CSV URLs.

    IMPORTANT: Include country names in your query for country-specific data.
    Examples: "population density france", "co2 emissions china", "gdp germany"

    Args:
        query: Free‑text query. Always include country names when seeking country data.

    Returns:
        List of ChartSearchResult objects with CSV URLs. URLs are automatically filtered
        for countries mentioned in the query.
    """
    log.debug("search_chart.start", query=query)

    hits = await make_algolia_request(query, 10)

    results: List[ChartSearchResult] = []
    for hit in hits:
        slug = hit["slug"]
        title = hit.get("title") or slug.replace("-", " ").title()
        subtitle = (
            hit.get("_snippetResult", {})
            .get("subtitle", {})
            .get("value", "")
            .replace("<mark>", "")
            .replace("</mark>", "")
        )

        # Use chart slug as id with simple URL format
        chart_url = f"https://ourworldindata.org/grapher/{slug}"

        results.append(
            ChartSearchResult(
                id=slug,  # Use slug as id instead of full URL
                title=title,
                text=subtitle or title,
                url=chart_url,  # Interactive chart URL
            )
        )

    log.debug("search_chart.done", returned=len(results))
    return results
