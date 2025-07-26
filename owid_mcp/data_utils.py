"""
OWID Data Utilities
-------------------
Shared utilities for data processing and API interactions across MCP modules.
"""

import asyncio
from typing import Any, Dict, List

import httpx
import structlog

from owid_mcp.config import HTTP_TIMEOUT, OWID_API_BASE
from owid_mcp.utils import smart_round

log = structlog.get_logger()


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
        ent["id"]: {"name": ent["name"], "code": ent["code"]} 
        for ent in metadata["dimensions"]["entities"]["values"]
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
                    if (ent_meta["code"] and ent_meta["code"].lower() == ent_lower 
                        and ent_meta["name"] == r["entity"]):
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
    from owid_mcp.config import CATALOG_BASE
    
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