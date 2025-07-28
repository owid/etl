import asyncio
import base64
import io
import logging
import os
import re
import urllib.parse
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
import pandas as pd
import structlog
import yaml
from fastmcp import FastMCP
from fastmcp.server.middleware import Middleware, MiddlewareContext
from fastmcp.utilities.types import Image
from pydantic import BaseModel
from sentry_sdk import capture_exception
from sentry_sdk import logger as sentry_logger

from etl.config import enable_sentry
from mcp.types import ImageContent
from owid_mcp.config import COMMON_ENTITIES, DATASETTE_BASE, HTTP_TIMEOUT, OWID_API_BASE

log = structlog.get_logger()


INSTRUCTIONS = (
    "CHART SEARCH (search/fetch/fetch_chart):\n"
    "• Use `search` to find relevant grapher datasets, then `fetch` to get CSV data\n"
    "• IMPORTANT: Always include country names in your search query when looking for country-specific data (e.g., 'population France' not just 'population')\n"
    "• The fetch tool returns CSV data with Entity column removed - only Code, Year, and metric columns remain\n"
    "• INTERACTIVE CHARTS: Users can view interactive charts by removing '.csv' from search result URLs\n"
    "  - Always inform users they can open interactive charts using the provided links\n"
    "  - Example: https://ourworldindata.org/grapher/population-density becomes interactive chart\n"
    "• ALWAYS be specific with countries and time ranges to minimize data size:\n"
    "  - Use specific country names in search queries to get filtered results\n"
    "  - Use time parameter in fetch/fetch_chart (e.g., '1990..2010', 'earliest..2010', '1990..latest')\n"
    "  - Prefer narrow time ranges over full historical data when possible\n\n"
)

mcp = FastMCP()


@mcp.tool
async def fetch_chart(id: str, time: Optional[str] = None) -> ImageContent:
    """Download a grapher PNG image by converting CSV URL to PNG URL.

    Takes a CSV URL from search results and converts it to PNG format by replacing
    the .csv extension with .png, then returns the image as base64-encoded content.

    Args:
        id: The full grapher CSV URL returned by `search`.
        time: Optional time range filter (e.g., '1990..2010', 'earliest..2010', '1990..latest').

    Returns:
        ImageContent with base64-encoded PNG data.
    """
    log.info("deep‑research.fetch_image", url=id, time=time)

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
        log.warning("deep‑research.fetch_image.error", url=png_url, error=str(exc))
        raise ValueError(f"Failed to download PNG: {exc}")
