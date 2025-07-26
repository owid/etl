# Deep Research Test Request

This directory contains a test request for OpenAI's Deep Research feature using the OWID MCP server.

## File: `test_deep_research_request.sh`

A shell script that sends a test request to OpenAI's Deep Research API to compare coal production per capita between France and Germany over history.

### Prerequisites

1. **OpenAI API Key**: You need an OpenAI API key with access to the `o4-mini-deep-research` model
2. **Environment Setup**: Set your API key as an environment variable

### Setup

```bash
# Set your OpenAI API key
export OPENAI_API_KEY="your_api_key_here"
```

### Running the Test

```bash
# Make the script executable
chmod +x test_deep_research_request.sh

# Run the test
./test_deep_research_request.sh
```

### What This Tests

- **Deep Research Model**: Uses OpenAI's `o4-mini-deep-research` model
- **MCP Integration**: Tests integration with the OWID MCP server at `https://mcp.owid.io/mcp/`
- **Search Optimization**: Tests the new search optimization guidelines we added to avoid failed searches
- **Data Query**: Specifically tests coal production per capita data for France and Germany

### Expected Behavior

With the new search optimization guidelines in place, Deep Research should:

1. Use simpler search queries like `"coal production per capita"` instead of overly complex ones
2. Avoid including "OWID" or "dataset" terms that cause 0 hits
3. Successfully find and fetch coal production data for both countries
4. Provide a meaningful comparison over time

### Monitoring

You can monitor the MCP server logs to see the actual search queries being made and verify that the optimization guidelines are being followed.

### Alternative Test Queries

You can modify the script to test other queries:

```bash
# Population density comparison
"text": "Compare population density between India and China over time"

# GDP per capita analysis  
"text": "Show me GDP per capita trends for Nordic countries"

# Energy consumption
"text": "How has renewable energy consumption changed in European countries?"
```

### Troubleshooting

1. **API Key Issues**: Ensure your OPENAI_API_KEY is correctly set and has Deep Research access
2. **Model Access**: Verify you have access to the `o4-mini-deep-research` model
3. **Network Issues**: Check if you can reach both the OpenAI API and the OWID MCP server
4. **Server Logs**: Monitor MCP server logs to see actual search queries and responses