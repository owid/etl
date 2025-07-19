from fastmcp import FastMCP

from etl_mcp.catalog_mcp import etl_catalog_mcp
from etl_mcp.etl_tools_mcp import etl_tools_mcp

# Create the main MCP server instance
mcp = FastMCP("OWID Data Catalog & ETL 🚀")

# Mount the specialized servers
mcp.mount(etl_catalog_mcp, prefix="catalog")
mcp.mount(etl_tools_mcp, prefix="etl")

if __name__ == "__main__":
    mcp.run()
