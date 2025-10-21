# OWID ChatGPT App MCP Server

This is a specialized MCP server for ChatGPT Apps that provides access to Our World in Data charts with widget support. It uses the OpenAI Apps SDK MCP protocol to display interactive chart visualizations directly in ChatGPT.

Inspired by https://github.com/openai/openai-apps-sdk-examples

## Features

- **Chart Search**: Search for OWID charts by keywords (e.g., "population density", "CO2 emissions")
- **Interactive Widgets**: Display charts as embedded iframes with the `text/html+skybridge` MIME type
- **Real OWID Data**: Uses the existing `owid_mcp.charts` module to search Algolia for actual OWID charts
- **Stateless HTTP**: Fully compatible with ChatGPT App's stateless JSON-RPC protocol

## Differences from Regular MCP Server

The ChatGPT App server differs from the regular MCP server (`owid_mcp/server.py`) in:

1. **Widget Support**: Returns `text/html+skybridge` content for ChatGPT App widget rendering
2. **Low-level Protocol**: Uses direct MCP protocol handlers instead of FastMCP decorators
3. **Structured Content**: Returns `structuredContent` with chart metadata for widget hydration
4. **Stateless HTTP**: Designed specifically for ChatGPT App's stateless session model

## Running the Server

### Local Development

```bash
# From the ETL repo root
.venv/bin/python -m chatgpt_app.server
```

The server will run on `http://localhost:8001`.

### Production Deployment

The server should be deployed at: `https://chatgpt-app.owid.io/mcp`

## API Endpoints

### List Tools
```bash
curl -X POST https://chatgpt-app.owid.io/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}'
```

### Search Charts
```bash
curl -X POST https://chatgpt-app.owid.io/mcp \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 2,
    "method": "tools/call",
    "params": {
      "name": "search-charts",
      "arguments": {"query": "CO2 emissions"}
    }
  }'
```

### List Resources
```bash
curl -X POST https://chatgpt-app.owid.io/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc": "2.0", "id": 3, "method": "resources/list", "params": {}}'
```

## ChatGPT App Integration

Configure your ChatGPT App to use this MCP server:

```json
{
  "mcpServers": {
    "owid-charts": {
      "url": "https://chatgpt-app.owid.io/mcp"
    }
  }
}
```

Then use the `search-charts` tool with natural language queries to display OWID charts.

## Widget Protocol

The server implements the ChatGPT App widget protocol:

1. **Tool Metadata**: Tools include `_meta` with OpenAI-specific annotations:
   - `openai/outputTemplate`: URI of the widget template
   - `openai/widgetAccessible`: Indicates widget support
   - `openai/resultCanProduceWidget`: Indicates this tool can return widgets

2. **Embedded Resources**: Tool results include an `EmbeddedResource` in `_meta["openai.com/widget"]` containing:
   - HTML content with MIME type `text/html+skybridge`
   - Chart iframe and styling

3. **Structured Content**: Results include `structuredContent` with chart metadata for JavaScript hydration

## Example Queries

- "Show me population density"
- "CO2 emissions by country"
- "GDP per capita trends"
- "Life expectancy data"
- "Renewable energy share"

## Architecture

```
chatgpt_app/
├── __init__.py
└── server.py
├── FastMCP server (stateless HTTP mode)
├── Low-level MCP protocol handlers
│   ├── _list_tools() → search-charts tool definition
│   ├── _list_resources() → widget template resource
│   ├── _list_resource_templates() → widget URI template
│   ├── _call_tool_request() → chart search and widget generation
│   └── _handle_read_resource() → default widget HTML
└── Chart search integration with owid_mcp.charts
```

## Testing

Test the server locally:

```bash
# Start the server
.venv/bin/python -m chatgpt_app.server

# In another terminal, test the endpoints
curl -X POST http://localhost:8001/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}'
```

## References

- [OpenAI Apps SDK - MCP Server Guide](https://developers.openai.com/apps-sdk/build/mcp-server)
- [MCP Protocol Specification](https://spec.modelcontextprotocol.io/)
- [OWID Grapher Documentation](https://docs.owid.io/projects/etl/)
