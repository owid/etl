from fastmcp import FastMCP

# Create the main MCP server instance
mcp = FastMCP("OWID Data Catalog & Charts 🚀")


# Simple test resource
@mcp.resource("test://hello")
async def test_resource() -> str:
    return "Hello, World!"


if __name__ == "__main__":
    mcp.run()
