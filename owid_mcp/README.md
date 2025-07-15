# OWID MCP Server

A FastMCP server providing access to OWID's data catalog and chart visualization tools.

## Architecture

This server uses FastMCP's composition pattern with separate modules:

- **`server.py`** - Main server that mounts specialized sub-servers
- **`etl.py`** - ETL catalog tools (find tables, create PRs, update steps)  
- **`charts.py`** - Chart search and visualization tools

## Available Tools

### ETL Tools (mounted at `catalog/`)

| Tool | Description |
|------|-------------|
| `find_table()` | Search data catalog tables by name, namespace, dataset, version |
| `create_pr()` | Create pull requests using existing PR CLI |
| `update_step()` | Update ETL steps to new versions |

### Chart Tools (mounted at `charts/`)

| Tool | Description |
|------|-------------|
| `search_chart(query)` | Find chart slugs by title/note (searches Datasette `charts` table) |

### Resources

| Resource | Description |
|----------|-------------|
| `chart://{slug}` | Streams latest Grapher SVG with optional query parameters |

## Usage Examples

### Running the Server

```bash
# Run the main server
python -m mcp.server

# Or run individual modules for testing
python -m mcp.etl
python -m mcp.charts
```

### Testing Chart Search

```python
# Test directly in Python
import asyncio
from mcp.charts import search_chart

async def test_chart_search():
    results = await search_chart("population density")
    for result in results:
        print(f"Title: {result.title}")
        print(f"URI: {result.resource_uri}")
        print(f"Snippet: {result.snippet}")
        print("---")

asyncio.run(test_chart_search())
```

```bash
# Or run the charts server directly
python -c "
import asyncio
from mcp.charts import charts_mcp

async def test():
    # This would typically be called by MCP client
    pass

# Run the server
charts_mcp.run()
"
```

### Testing ETL Tools

```python
# Test directly in Python
from mcp.etl import find_table, create_pr, update_step

# Find tables in the biodiversity namespace
tables = find_table(namespace="biodiversity", channel="garden")
for table in tables:
    print(f"Table: {table.table}")
    print(f"Dataset: {table.dataset}")
    print(f"Version: {table.version}")
    print(f"Download URL: {table.download_url}")
    print("---")

# Create a new PR (dry run - check what would happen)
# pr_result = create_pr(title="Update biodiversity dataset", category="data")
# print(f"PR would be created: {pr_result.message}")

# Update ETL steps (dry run)
# update_result = update_step(
#     steps=["data://garden/biodiversity/2025-04-07/cherry_blossom"], 
#     dry_run=True
# )
# print(f"Update result: {update_result.message}")
```

### Testing with MCP Client

If you have an MCP client configured, you can test with:

```bash
# Example with hypothetical MCP client
# mcp-client call search_chart '{"query": "population density"}'
# mcp-client call find_table '{"namespace": "biodiversity"}'
```

## Development

The server follows FastMCP best practices:
- No nested function definitions
- Clean module separation using `mount()`
- Direct tool registration at module level
- Proper type hints and documentation
