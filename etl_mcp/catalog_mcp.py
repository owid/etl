from dataclasses import dataclass
from typing import List, Optional

import numpy as np
from fastmcp import FastMCP
from owid import catalog


@dataclass
class TableMetadata:
    """Metadata for a table in the data catalog."""

    table: Optional[str]
    namespace: Optional[str]
    dataset: Optional[str]
    version: Optional[str]
    channel: Optional[str]
    path: Optional[str]
    download_url: Optional[str]
    dimensions: List[str]
    description: str


# Create ETL Catalog MCP server
etl_catalog_mcp = FastMCP("ETL Catalog")


@etl_catalog_mcp.tool
def find_table(
    table: Optional[str] = None,
    namespace: Optional[str] = None,
    dataset: Optional[str] = None,
    version: Optional[str] = None,
    channel: Optional[str] = "garden",
) -> List[TableMetadata]:
    """Find tables in the data catalog using various filters.

    Args:
        table: Table name (supports regex patterns)
        namespace: Namespace to filter by
        dataset: Dataset name to filter by
        version: Version to filter by
        channel: Channel to search in (default: "garden")

    Returns:
        List of TableMetadata objects with table information
    """
    # Use catalog.find to search for tables
    results = catalog.find(
        table=table,
        namespace=namespace,
        dataset=dataset,
        version=version,
        channels=[channel] if channel else ["garden"],
    )

    # Convert results to a list of TableMetadata objects
    tables = []
    for _, row in results.iterrows():
        # Generate download URL from path
        path = row.get("path")
        download_url = None
        if path:
            download_url = f"https://catalog.ourworldindata.org/{path}.feather"

        # Safe conversion function to handle numpy arrays and other types
        def safe_convert(value, default=""):
            if value is None:
                return default
            if isinstance(value, np.ndarray):
                return value.tolist()
            if hasattr(value, "tolist"):
                return value.tolist()
            return str(value)

        # Convert dimensions to list if it's a numpy array
        dimensions = row.get("dimensions", [])
        if dimensions is None:
            dimensions = []
        elif isinstance(dimensions, np.ndarray):
            dimensions = dimensions.tolist()
        elif hasattr(dimensions, "tolist"):
            dimensions = dimensions.tolist()
        else:
            # Force convert to list of strings to avoid serialization issues
            dimensions = [str(d) for d in dimensions] if dimensions else []

        tables.append(
            TableMetadata(
                table=safe_convert(row.get("table")),
                namespace=safe_convert(row.get("namespace")),
                dataset=safe_convert(row.get("dataset")),
                version=safe_convert(row.get("version")),
                channel=safe_convert(row.get("channel")),
                path=safe_convert(path) if path else None,
                download_url=download_url,
                dimensions=dimensions,
                description=safe_convert(row.get("description")),
            )
        )

    return tables
