from fastmcp import FastMCP

from owid_mcp.charts_mcp import charts_mcp
from owid_mcp.etl_catalog import etl_catalog_mcp
from owid_mcp.etl_tools import etl_tools_mcp

# Create the main MCP server instance
mcp = FastMCP("OWID Data Catalog & Charts 🚀")

# Mount the specialized servers
mcp.mount(etl_catalog_mcp, prefix="catalog")
mcp.mount(etl_tools_mcp, prefix="tools")
mcp.mount(charts_mcp)

if __name__ == "__main__":
    mcp.run()
