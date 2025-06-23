from dataclasses import dataclass
from typing import List, Optional

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


mcp = FastMCP("Data Catalog 🚀")


@mcp.tool
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

        tables.append(
            TableMetadata(
                table=row.get("table"),
                namespace=row.get("namespace"),
                dataset=row.get("dataset"),
                version=row.get("version"),
                channel=row.get("channel"),
                path=path,
                download_url=download_url,
                dimensions=row.get("dimensions", []),
                description=row.get("description", ""),
            )
        )

    return tables


if __name__ == "__main__":
    mcp.run()
