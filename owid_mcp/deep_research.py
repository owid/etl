"""
OWID Deep Research MCP Module
-----------------------------
Provides **search** and **fetch** tools compatible with OpenAI's Deep Research specification.

Usage
-----
* Add an **optional** `country:<ISO_CODE>` token to the search query to
  restrict results to a single country (e.g. `gdp per capita country:US`).
* The server encodes that slice in the returned ID as `<indicator>|<ISO>`.
  The Deep‑Research client then calls `fetch` with that exact ID to obtain
  only the requested country‑level series, keeping the context window small.
"""

import asyncio
import re
import urllib.parse
from typing import Any, Dict, List, Tuple

import httpx
import structlog
from fastmcp import FastMCP

from owid_mcp.config import DATASETTE_BASE, HTTP_TIMEOUT, OWID_API_BASE
from owid_mcp.data_utils import build_rows, fetch_json, rows_to_csv

log = structlog.get_logger()

# ---------------------------------------------------------------------------
# Helper for extracting an optional country filter from the search query
# ---------------------------------------------------------------------------
COUNTRY_RE = re.compile(r"\bcountry:(\w{2,3})\b", re.IGNORECASE)


def _extract_country(query: str) -> Tuple[str, str | None]:
    """Return *(cleaned_query, ISO_code_or_None)*.

    Removes a single `country:XX` token (case‑insensitive) from *query* and
    upper‑cases the ISO code. If no token is present the second element is
    *None*.
    """
    m = COUNTRY_RE.search(query)
    if not m:
        return query.strip(), None

    country = m.group(1).upper()
    cleaned = COUNTRY_RE.sub("", query).strip()
    return cleaned, country


# ---------------------------------------------------------------------------
# Create the MCP server
# ---------------------------------------------------------------------------

mcp = FastMCP(
    name="OWID Deep Research",
    instructions=(
        "Search and fetch OWID indicators for deep research workflows. "
        "Add `country:<ISO_CODE>` to the search query to obtain country‑level "
        "slices (IDs returned as `<indicator>|<ISO>`). Use `search` first, "
        "then `fetch` the composite ID(s)."
    ),
)

# ---------------------------------------------------------------------------
# SEARCH TOOL
# ---------------------------------------------------------------------------


@mcp.tool
async def search(query: str, limit: int = 10) -> List[Dict[str, str]]:
    """Search OWID indicators.

    Supports an optional *country filter* via a `country:XX` token.
    Returns IDs as either `<indicator>` (global) or `<indicator>|<ISO>`.
    """
    # 1. Parse optional country filter
    cleaned_query, country = _extract_country(query)
    log.info("Deep research search", query=cleaned_query, country=country, limit=limit)

    # 2. Run SQL search against Datasette
    sql = """
    SELECT
        v.id,
        v.name,
        v.description,
        COALESCE(cd.chart_count, 0) AS chart_count
    FROM variables v
    LEFT JOIN (
        SELECT variableId, COUNT(DISTINCT chartId) AS chart_count
        FROM chart_dimensions
        GROUP BY variableId
    ) cd ON cd.variableId = v.id
    WHERE v.catalogPath IS NOT NULL AND (
            v.name LIKE :q COLLATE NOCASE
            OR v.description LIKE :q COLLATE NOCASE
    )
    ORDER BY chart_count DESC, v.name
    LIMIT :limit
    """
    params = {"q": f"%{cleaned_query}%", "limit": limit}
    qs = urllib.parse.urlencode({"sql": sql, **params})

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        resp = await client.get(f"{DATASETTE_BASE}?{qs}")
        resp.raise_for_status()
        rows = resp.json()["rows"]

    results: List[Dict[str, str]] = []
    for row in rows:
        var_id, title, description, chart_count = row
        text_snippet = (description or title)[:200] + ("..." if len(description or title or "") > 200 else "")
        chart_url = f"https://ourworldindata.org/charts?variable={var_id}"

        composite_id = f"{var_id}|{country}" if country else str(var_id)
        results.append(
            {
                "id": composite_id,
                "title": title,
                "text": text_snippet,
                "url": chart_url,
                # isn't accepted by deep research
                # "metadata": {
                #     "country": country,
                #     "chart_count": chart_count,
                # },
            }
        )

    log.info("Deep research search completed", found=len(results))
    return results


# ---------------------------------------------------------------------------
# FETCH TOOL
# ---------------------------------------------------------------------------


@mcp.tool
async def fetch(id: str) -> Dict[str, Any]:
    """Fetch an indicator (optionally sliced by country).

    *ID format*
    -----------
    * Global series:            `<indicator>`  →  e.g. `12345`
    * Country‑level slice:      `<indicator>|<ISO>`  →  e.g. `12345|US`
    The second form returns only the rows whose `entity_code` equals *ISO*.
    """
    log.info("Fetching indicator", composite_id=id)

    try:
        # 1. Split ID into indicator + optional country
        parts = id.split("|", 1)
        indicator_id = int(parts[0])
        country = parts[1].upper() if len(parts) > 1 else None

        # 2. Fetch raw data + metadata concurrently
        data_url = f"{OWID_API_BASE}/{indicator_id}.data.json"
        meta_url = f"{OWID_API_BASE}/{indicator_id}.metadata.json"
        data_json, metadata = await asyncio.gather(fetch_json(data_url), fetch_json(meta_url))

        # 3. Build tidy rows
        entities_meta = {
            ent["id"]: {"name": ent["name"], "code": ent["code"]}
            for ent in metadata["dimensions"]["entities"]["values"]
        }
        rows = build_rows(data_json, entities_meta)

        # 4. Optional country filter
        if country:
            rows = [r for r in rows if r.get("entity_code") == country]

        # 5. Convert to CSV
        csv_text = rows_to_csv(rows)

        # 6. Compose result
        title = metadata.get("name", f"Indicator {indicator_id}")
        chart_url = f"https://ourworldindata.org/charts?variable={indicator_id}"

        return {
            "id": id,
            "title": title,
            "text": csv_text,
            "url": chart_url,
            "metadata": {
                "description": metadata.get("description", ""),
                "unit": metadata.get("unit", ""),
                "short_unit": metadata.get("shortUnit", ""),
                "display": metadata.get("display", {}),
                "row_count": len(rows),
                "country": country,
            },
        }

    except Exception as e:
        log.warning("Failed to fetch indicator", composite_id=id, error=str(e))
        return {
            "id": id,
            "title": f"Error fetching indicator {id}",
            "text": f"Failed to fetch indicator data: {str(e)}",
            "url": f"https://ourworldindata.org/charts?variable={id}",
            "metadata": {"error": str(e)},
        }
