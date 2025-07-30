"""
OWID Data Utilities
-------------------
Shared utilities for data processing and API interactions across MCP modules.
"""

import asyncio
import math
import re
import urllib.parse
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import structlog
import yaml

from owid_mcp.config import CATALOG_BASE, DATASETTE_BASE, HTTP_TIMEOUT, MAX_ROWS_DEFAULT, MAX_ROWS_HARD, OWID_API_BASE

log = structlog.get_logger()

# Algolia configuration
ALGOLIA_APP_ID = "ASCB5XMYF2"
ALGOLIA_API_KEY = "bafe9c4659e5657bf750a38fbee5c269"
ALGOLIA_URL = f"https://{ALGOLIA_APP_ID.lower()}-dsn.algolia.net/1/indexes/*/queries"

# Global mapping cache
_NAME_TO_CODE_MAPPING: Optional[Dict[str, str]] = None

# SQL validation pattern
SQL_SELECT_RE = re.compile(r"^\s*select\b", re.IGNORECASE | re.DOTALL)


def _load_regions_mapping() -> Dict[str, str]:
    """Load OWID regions mapping from YAML file."""
    global _NAME_TO_CODE_MAPPING

    if _NAME_TO_CODE_MAPPING is not None:
        return _NAME_TO_CODE_MAPPING

    # Path to the regions file
    regions_file = (
        Path(__file__).parent.parent / "etl" / "steps" / "data" / "garden" / "regions" / "2023-01-01" / "regions.yml"
    )

    mapping: Dict[str, str] = {}

    with open(regions_file, "r", encoding="utf-8") as f:
        regions = yaml.safe_load(f)

    for region in regions:
        if not isinstance(region, dict):
            continue

        code = region.get("code")
        name = region.get("name")

        if code and name:
            # Map name to code (case-insensitive)
            mapping[name.lower()] = code

            # Map aliases to code
            aliases = region.get("aliases", [])
            if isinstance(aliases, list):
                for alias in aliases:
                    if isinstance(alias, str):
                        mapping[alias.lower()] = code

    log.info("regions.mapping.loaded", count=len(mapping))

    _NAME_TO_CODE_MAPPING = mapping
    return mapping


def country_name_to_iso3(name: Optional[str]) -> Optional[str]:
    """Convert country name to ISO-3 code using OWID regions mapping."""
    if not name:
        return None

    mapping = _load_regions_mapping()
    return mapping.get(name.lower())


async def _make_algolia_request_base(request_config: Dict[str, Any], log_prefix: str) -> List[Dict[str, Any]]:
    """Base function for making Algolia API requests.

    Args:
        request_config: The request configuration for the Algolia index
        log_prefix: Prefix for logging messages

    Returns:
        List of search hits from Algolia
    """
    payload = {"requests": [request_config]}

    headers = {
        "x-algolia-api-key": ALGOLIA_API_KEY,
        "x-algolia-application-id": ALGOLIA_APP_ID,
        "x-algolia-agent": "OWID-MCP (python)",
    }

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        resp = await client.post(ALGOLIA_URL, json=payload, headers=headers)
        resp.raise_for_status()
        response_data = resp.json()
        hits = response_data["results"][0].get("hits", [])
        log.debug(f"{log_prefix}.response", hits_count=len(hits))
        return hits


async def make_algolia_request(query: str, limit: int = 10) -> List[Dict[str, Any]]:
    """Make a request to Algolia search API and return the hits.

    Args:
        query: Search query string
        limit: Maximum number of results to return

    Returns:
        List of search hits from Algolia
    """
    log.debug("algolia.request", query=query, limit=limit)

    request_config = {
        "indexName": "explorer-views-and-charts",
        "attributesToRetrieve": [
            "title",
            "slug",
            "availableEntities",
            "variantName",
            "type",
        ],
        "query": query,
        "facetFilters": [[], "isIncomeGroupSpecificFM:false"],
        "highlightPreTag": "<mark>",
        "highlightPostTag": "</mark>",
        "facets": ["tags"],
        "hitsPerPage": limit,
        "page": 0,
    }

    hits = await _make_algolia_request_base(request_config, "algolia")

    # Exclude explorerView since we don't have persistent CSV URLs for them
    hits = [hit for hit in hits if hit.get("type") != "explorerView"]

    return hits


async def make_algolia_pages_request(
    query: str,
    hits_per_page: int = 10,
    distinct: bool = True,
) -> List[Dict[str, Any]]:
    """Make a request to Algolia pages index and return the hits.

    Args:
        query: Search query string
        hits_per_page: Maximum number of results per page to return
        distinct: Whether to enable distinct results

    Returns:
        List of search hits from Algolia pages index
    """
    log.debug("algolia.pages.request", query=query, hits_per_page=hits_per_page)

    request_config = {
        "indexName": "pages",
        "query": query,
        "hitsPerPage": hits_per_page,
        "distinct": distinct,
    }

    return await _make_algolia_request_base(request_config, "algolia.pages")


async def run_sql(query: str, max_rows: int = MAX_ROWS_DEFAULT) -> Dict[str, Any]:
    """Execute a **read‑only** SQL SELECT via the OWID public Datasette.

    Parameters
    ----------
    query : str
        A SQL statement starting with `SELECT`. Anything else is rejected.
    max_rows : int
        Safety cap (1‑5000). The query is rewritten with `LIMIT` if absent.

    Returns
    -------
    dict
        {"columns": [...], "rows": [[...], ...], "source": datasette_url}
    """
    if not SQL_SELECT_RE.match(query):
        raise ValueError("Only SELECT statements are allowed.")
    if max_rows < 1 or max_rows > MAX_ROWS_HARD:
        raise ValueError(f"max_rows must be 1‑{MAX_ROWS_HARD}.")

    # Append/override LIMIT to enforce row cap
    if re.search(r"\blimit\b", query, re.IGNORECASE):
        query = re.sub(r"limit\s+\d+", f"LIMIT {max_rows}", query, flags=re.IGNORECASE)
    else:
        query = f"{query} LIMIT {max_rows}"

    qs = urllib.parse.urlencode({"sql": query, "_size": "max"})
    # Remove the .json extension from DATASETTE_BASE since it's already included in config
    datasette_base = DATASETTE_BASE.replace(".json", "")
    datasette_url = f"{datasette_base}.json?{qs}"

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        resp = await client.get(datasette_url)
        resp.raise_for_status()
        js = resp.json()

    return {
        "columns": js.get("columns", []),
        "rows": js.get("rows", []),
        "source": datasette_url,
    }


def smart_round(value: float | None) -> float | None:
    """Apply smart rounding to reduce context waste while preserving meaningful precision.

    Args:
        value: The numeric value to round, or None

    Returns:
        Rounded value according to smart rounding rules, or None if input is None

    Rounding rules:
    - None values: returned as-is
    - Integers: preserved as integers
    - Very small numbers (< 0.001): rounded to 4 significant digits
    - Small numbers (0.001 - 1): rounded to 3 decimal places
    - Medium numbers (1 - 1000): rounded to 2 decimal places
    - Large numbers (1000 - 10000): rounded to 1 decimal place
    - Very large numbers (>10000): rounded to integers
    """

    if value is None:
        return None

    # Check if it's already an integer (no fractional part)
    if value == int(value):
        return int(value)

    abs_val = abs(value)

    # Very small numbers (< 0.001): round to 4 significant digits
    if abs_val < 0.001:
        if abs_val == 0:
            return 0
        # Find the order of magnitude
        order = math.floor(math.log10(abs_val))
        precision = 3 - order  # 4 significant digits
        rounded = round(value, min(precision, 15))  # Cap at 15 decimal places
        # If rounding results in 0, return the original value with scientific notation
        if rounded == 0:
            return value
        return rounded

    # Small numbers (0.001 - 1): round to 3 decimal places
    elif abs_val < 1:
        return round(value, 3)

    # Medium numbers (1 - 1000): round to 2 decimal places
    elif abs_val < 1000:
        return round(value, 2)

    # Large numbers (1000+): round to 1 decimal place
    elif abs_val < 10000:
        return round(value, 1)

    # Very large numbers: round to nearest integer
    else:
        return round(value)


def build_rows(data_json: Dict[str, Any], entities_meta: Dict[int, Dict[str, str]]) -> List[Dict[str, Any]]:
    """Convert the compact OWID arrays into a list[{entity, year, value}]."""

    values = data_json["values"]
    years = data_json["years"]
    entity_ids = data_json["entities"]

    # Guard against length mismatch
    if not (len(values) == len(years) == len(entity_ids)):
        raise ValueError("Mismatched lengths in OWID data arrays")

    rows: List[Dict[str, Any]] = []
    append = rows.append

    for v, y, eid in zip(values, years, entity_ids):
        meta = entities_meta.get(eid)
        if meta is None:
            # Skip unknown entity id (should not normally happen)
            continue
        append(
            {
                "entity": meta["name"],
                "year": y,
                "value": smart_round(v),
            }
        )

    return rows


async def fetch_json(url: str) -> Dict[str, Any]:
    """Fetch JSON data from a URL."""
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.json()


async def fetch_indicator_data(indicator_id: int, entity: str | None = None) -> Dict[str, Any]:
    """Fetch indicator data and metadata for a single indicator ID.

    Args:
        indicator_id: Numeric OWID indicator id
        entity: Optional entity name or ISO-3 code for filtering

    Returns:
        Dictionary with metadata and data keys
    """
    # Fetch OWID raw data + metadata concurrently
    data_url = f"{OWID_API_BASE}/{indicator_id}.data.json"
    meta_url = f"{OWID_API_BASE}/{indicator_id}.metadata.json"
    data_json, metadata = await asyncio.gather(fetch_json(data_url), fetch_json(meta_url))

    # Build mapping from numeric id -> {name, code}
    entities_meta = {
        ent["id"]: {"name": ent["name"], "code": ent["code"]} for ent in metadata["dimensions"]["entities"]["values"]
    }

    rows = build_rows(data_json, entities_meta)

    # Optional server-side filter for a single entity
    if entity is not None:
        ent_lower = entity.lower()
        filtered_rows = []
        for r in rows:
            if r["entity"] and r["entity"].lower() == ent_lower:
                filtered_rows.append(r)
            else:
                # Check if entity matches any code in entities_meta
                for ent_meta in entities_meta.values():
                    if ent_meta["code"] and ent_meta["code"].lower() == ent_lower and ent_meta["name"] == r["entity"]:
                        filtered_rows.append(r)
                        break
        rows = filtered_rows

    return {"metadata": metadata, "data": rows}


def rows_to_csv(rows: List[Dict[str, Any]]) -> str:
    """Convert data rows to CSV format."""
    if not rows:
        return "entity,year,value\n"

    # CSV header
    csv_lines = ["entity,year,value"]

    # CSV data rows
    for row in rows:
        entity = str(row.get("entity", "")).replace('"', '""')  # Escape quotes
        year = str(row.get("year", ""))
        value = str(row.get("value", ""))
        csv_lines.append(f'"{entity}",{year},{value}')

    return "\n".join(csv_lines)


def build_catalog_info(catalog_path: str) -> Dict[str, str]:
    """Build Parquet URL & example SQL template from catalogPath.

    Args:
        catalog_path: Path like 'grapher/biodiversity/2025-04-07/cherry_blossom/cherry_blossom#average_20_years'
    """
    # Split on '#' to separate path from column
    path, column = catalog_path.split("#")

    # Parse the path: channel/namespace/version/dataset_slug/dataset_slug
    parts = path.split("/")
    channel, namespace, version, dataset_slug, table_name = parts[0], parts[1], parts[2], parts[3], parts[4]

    parquet_url = f"{CATALOG_BASE}/{channel}/{namespace}/{version}/{dataset_slug}/{table_name}.parquet"
    sql_tpl = "SELECT country, year, {col} FROM '{url}' " "WHERE country = '??' LIMIT 100".format(
        col=column, url=parquet_url
    )
    return {
        "parquet_url": parquet_url,
        "sql_template": sql_tpl,
        "column": column,
    }
