# OWID MCP Server

This is Our World in Data's MCP (Model Context Protocol) server providing access to charts, indicators, and data from Our World in Data. It's built using fastMCP and provides both Deep Research functionality and direct indicator access.

## Features

### Deep Research Module (`deep_research_algolia.py`)

**Tools:**
- `search(query)` - Search OWID charts via Algolia, returns CSV URLs filtered by country mentions
- `fetch(id, time?)` - Download and process CSV data from chart URLs with optional time filtering
- `fetch_image(id)` - Download PNG chart images by converting CSV URLs to PNG format

### Indicators Module (`indicators.py`)

**Tools:**
- `search_indicator(query, limit?)` - Search for OWID indicators by name or description
- `run_sql(query, max_rows?)` - Execute read-only SQL queries against the OWID public Datasette

**Resources:**
- `ind://{indicator_id}` - Get indicator data and metadata for all entities
- `ind://{indicator_id}/{entity}` - Get indicator data filtered for specific country/entity

### Posts Module (`posts.py`)

**Tools:**
- `fetch_post(identifier, include_metadata?)` - Fetch markdown content for posts by slug or Google Doc ID
- `search_posts(query, limit?)` - Search for posts by title or content

### Shared Utilities (`data_utils.py`)

**Functions:**
- `make_algolia_request(query, limit)` - Make requests to Algolia search API
- `country_name_to_iso3(name)` - Convert country names to ISO-3 codes using OWID regions mapping
- `run_sql(query, max_rows)` - Execute read-only SQL queries against the OWID public Datasette
- `smart_round(value)` - Apply smart rounding to reduce context waste while preserving precision
- Various data processing utilities for CSV conversion and metadata handling

## Running the Server

```bash
# Activate virtual environment
source .venv/bin/activate

# Run the OWID MCP server
fastmcp run owid_mcp/server.py

# Or run with development options
python -m mcp.server.stdio owid_mcp.server:mcp
```

## Usage Examples

### Chart Search and Data Retrieval
```python
# Search for population density charts
search_results = await search("population density france")

# Fetch CSV data from the first result
csv_data = await fetch(search_results[0].id)

# Get chart image
chart_image = await fetch_image(search_results[0].id)
```

### Indicator Search and Access
```python
# Search for GDP indicators
indicators = await search_indicator("GDP per capita", limit=5)

# Access indicator data via resource
indicator_data = await client.read_resource("ind://2118")  # GDP per capita

# Get data for specific country
usa_data = await client.read_resource("ind://2118/USA")
```

### SQL Queries
```python
# Query the OWID database directly
results = await run_sql("SELECT id, name FROM variables WHERE name LIKE '%population%' LIMIT 10")
```

### Post Content Retrieval
```python
# Fetch post by slug
post_content = await fetch_post_markdown("poverty")

# Search for posts about climate change
search_results = await search_posts("climate change", limit=5)
```

## Architecture

The server uses a modular architecture:

- **`server.py`** - Main FastMCP server that imports and combines modules
- **`deep_research_algolia.py`** - Algolia-based chart search and CSV/image fetching
- **`indicators.py`** - Direct indicator search and data access
- **`posts.py`** - Post markdown content retrieval and search
- **`data_utils.py`** - Shared utilities for data processing and API requests
- **`config.py`** - Configuration constants and settings

## Search Optimization Tips

### For Charts (Deep Research)
- Include country names in queries: "population density france", "co2 emissions china"
- Use simple, generic terms: "coal production", "gdp per capita"
- Avoid OWID-specific terms or overly complex queries

### For Indicators
- Search by concept only: "population density" (not "population density USA")
- Use `country:` filter for specific countries: "population density country:US"
- Entity names must match exactly as they appear in OWID

## Configuration

Key environment variables and settings are defined in `config.py`:
- API endpoints and timeouts
- Database connection settings
- Common entity mappings
- Rate limiting and caching parameters
