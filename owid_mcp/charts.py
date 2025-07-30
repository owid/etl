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
from owid_mcp.data_utils import country_name_to_iso3, make_algolia_request

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


async def _fetch_chart_data_internal(id: str, time: Optional[str] = None) -> ChartDataResult:
    """Download a grapher CSV and return the processed data.

    The returned CSV includes:
    - Entity: Country/region names (removed if Code column has no empty values)
    - Code: Country/region ISO codes
    - Year: Time period
    - [Metric columns]: The actual data values

    IMPORTANT: Only provide data that exists in the fetched CSV. Do not hallucinate
    or interpolate missing values.

    Args:
        id: The full grapher CSV URL returned by `search`.
        time: Optional time range filter (e.g., '1990..2010', 'earliest..2010', '1990..latest').

    Returns:
        ChartDataResult with `text` containing processed CSV data and metadata about
        the dataset structure.
    """
    log.info("chart.fetch_data", url=id, time=time)

    if not id.startswith("http"):
        return ChartDataResult(
            id=id,
            title="Invalid ID (expected URL)",
            text="",
            url=id,
            metadata={"error": "ID must be a grapher CSV URL"},
        )

    # Parse URL and add required parameters
    parsed = urllib.parse.urlparse(id)
    query_params = urllib.parse.parse_qs(parsed.query)
    
    # Add tab=line&csvType=filtered if not already present
    if "tab" not in query_params:
        query_params["tab"] = ["line"]
    if "csvType" not in query_params:
        query_params["csvType"] = ["filtered"]
    
    # Add time parameter if provided
    if time:
        query_params["time"] = [time]
        
    new_query = urllib.parse.urlencode(query_params, doseq=True)
    fetch_url = urllib.parse.urlunparse(
        (parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment)
    )

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            resp = await client.get(fetch_url)
            resp.raise_for_status()
            csv_content = resp.text

        # Process CSV - remove Entity column if Code column has no empty values
        df = pd.read_csv(io.StringIO(csv_content))
        
        # Check if Code column exists and has no empty/null values
        if 'Code' in df.columns and 'Entity' in df.columns:
            # Check for empty, null, or whitespace-only values in Code column
            code_has_empty = df['Code'].isna().any() or (df['Code'].astype(str).str.strip() == '').any()
            if not code_has_empty:
                # Remove Entity column as it's redundant when Code is complete
                df = df.drop(columns=['Entity'])

        # Convert back to CSV string
        processed_csv: str = df.to_csv(index=False)

        slug = urllib.parse.urlparse(id).path.rsplit("/", 1)[-1].split(".")[0]
        title = slug.replace("-", " ").title() if slug else "OWID Dataset"

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

    except Exception as exc:  # noqa: BLE001
        log.warning("chart.fetch_data.error", url=fetch_url, error=str(exc))
        return ChartDataResult(
            id=id,
            title="Error fetching CSV",
            text=f"Failed to download CSV: {exc}",
            url=fetch_url,
            metadata={"error": str(exc)},
        )


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
        id: The full grapher CSV URL returned by `search`.
        time: Optional time range filter (e.g., '1990..2010', 'earliest..2010', '1990..latest').

    Returns:
        ChartDataResult with `text` containing processed CSV data and metadata about
        the dataset structure.
    """
    return await _fetch_chart_data_internal(id, time)


@mcp.tool
async def fetch_chart_image(id: str, time: Optional[str] = None) -> ImageContent:
    """Download a grapher PNG image by converting CSV URL to PNG URL.

    Takes a CSV URL from search results and converts it to PNG format by replacing
    the .csv extension with .png, then returns the image as base64-encoded content.

    Args:
        id: The full grapher CSV URL returned by `search`.
        time: Optional time range filter (e.g., '1990..2010', 'earliest..2010', '1990..latest').

    Returns:
        ImageContent with base64-encoded PNG data.
    """
    log.info("chart.fetch_image", url=id, time=time)

    if not id.startswith("http"):
        # Return a text response for invalid URLs - FastMCP will handle the conversion
        raise ValueError("Invalid ID (expected URL)")

    # Convert CSV URL to PNG URL by replacing .csv with .png
    png_url = id.replace(".csv", ".png")

    # Remove CSV-specific query parameters that don't make sense for PNG
    parsed = urllib.parse.urlparse(png_url)
    query_params = urllib.parse.parse_qs(parsed.query)

    # Keep only PNG-relevant parameters
    png_params = {}
    if "country" in query_params:
        png_params["country"] = query_params["country"]

    # Use time parameter from function argument or existing URL
    if time:
        png_params["time"] = [time]
    elif "time" in query_params:
        png_params["time"] = query_params["time"]

    # Add chart tab parameter for PNG
    png_params["tab"] = ["chart"]

    # Reconstruct URL with PNG-appropriate parameters
    new_query = urllib.parse.urlencode(png_params, doseq=True)
    png_url = urllib.parse.urlunparse(
        (parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment)
    )

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


async def _search_chart_internal(query: str) -> List[ChartSearchResult]:
    """Search OWID using Algolia and return grapher CSV URLs.

    IMPORTANT: Include country names in your query for country-specific data.
    Examples: "population density france", "co2 emissions china", "gdp germany"

    Args:
        query: Free‑text query. Always include country names when seeking country data.

    Returns:
        List of ChartSearchResult objects with CSV URLs. URLs are automatically filtered
        for countries mentioned in the query.
    """
    limit = 10  # Fixed limit for deep research compatibility
    log.debug("search_chart.start", query=query, limit=limit)

    try:
        hits = await make_algolia_request(query, limit)

        results: List[ChartSearchResult] = []
        for hit in hits:
            # Not needed
            hit.pop("availableEntities", [])

            slug = hit["slug"]
            title = hit.get("title") or slug.replace("-", " ").title()
            subtitle = (
                hit.get("_snippetResult", {})
                .get("subtitle", {})
                .get("value", "")
                .replace("<mark>", "")
                .replace("</mark>", "")
            )

            # Attempt to deduce countries from highlight results for tighter charts
            country_codes: List[str] = []
            highlight_entities = hit.get("_highlightResult", {}).get("availableEntities", [])

            for ent in highlight_entities:
                if ent.get("matchedWords"):
                    country_name = ent["value"].replace("<mark>", "").replace("</mark>", "")
                    country_iso3 = country_name_to_iso3(country_name)
                    if country_iso3:
                        country_codes.append(country_iso3)

            # &time=earliest..latest probably wrong...
            grapher_url = f"https://ourworldindata.org/grapher/{slug}.csv?tab=line&csvType=filtered"

            # Perhaps we should use this?
            # https://ourworldindata.org/grapher/population-density.csv?v=1&csvType=filtered&useColumnShortNames=true&tab=line&country=FRA~DEU

            if country_codes:
                # Format multiple countries as country=FRA~DEU~...
                country_param = "~".join(country_codes)
                grapher_url += f"&country={country_param}"

            # NOTE: we could get metadata from https://ourworldindata.org/grapher/urban-area-long-term.metadata.json?v=1&csvType=filtered&useColumnShortNames=true
            # TODO: we return grapher_url as id only because of deep research compatibility. Once it starts supporting regular MPC, we should
            #   stop adding `tab=line&csvType=filtered`
            results.append(
                ChartSearchResult(
                    id=grapher_url,
                    title=title,
                    text=subtitle or title,
                    url=grapher_url,
                )
            )

        log.debug("search_chart.done", returned=len(results))
        return results

    except Exception as exc:
        log.error("search_chart.error", query=query, error=str(exc))
        return []


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
    return await _search_chart_internal(query)
