# Test MCP Server

This is a mock MCP server for testing purposes, demonstrating both resources and tools functionality using fastMCP.

## Features

### Resources
- `dataset://{dataset_id}` - Get information about specific datasets
- `test://data/all` - Get all test data
- `test://data/{category}` - Get test data filtered by category

### Tools
- `list_datasets()` - List all available datasets
- `search_data(query, category, min_value, max_value)` - Search test data with filters
- `calculate_stats(category)` - Calculate statistics for test data
- `create_sample_data(name, value, category)` - Create new sample data entry

## Running the Server

```bash
# Activate virtual environment
source .venv/bin/activate

# Run the test MCP server
python -m owid_test_mcp.server
```

## Testing with LLM

Once the server is running, you can test it by asking an LLM to interact with it. Here are some example prompts:

### Testing Resources
1. "Can you show me the available MCP resources?"
2. "Read the resource `dataset://dataset1`"
3. "Get all test data from `test://data/all`"
4. "Show me climate data from `test://data/climate`"

### Testing Tools
1. "Use the MCP tool to list all datasets"
2. "Search for data containing 'temperature' in the climate category"
3. "Calculate statistics for the climate category"
4. "Create a new sample data entry with name 'Wind Speed', value 15.5, category 'climate'"

### Combined Testing
"List all available datasets, then read the details of dataset1, and finally calculate statistics for climate data"

## Sample Data

The server includes sample datasets and test data for demonstration:

**Datasets:**
- dataset1: Global Temperature Data (1000 records)
- dataset2: Population Statistics (195 records)
- dataset3: Economic Indicators (500 records)

**Test Data:**
- Climate data: Temperature, Humidity, Pressure
- Demographics: Population
- Economics: GDP

## Configuration

The server runs on the default MCP port and can be configured through environment variables or command-line arguments as supported by fastMCP.