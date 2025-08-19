#!/bin/bash

# Test request for OpenAI Deep Research using OWID MCP server
# This tests the coal production per capita comparison between France and Germany
# Usage: ./test_deep_research_request.sh

# Check if OPENAI_API_KEY is set
if [ -z "$OPENAI_API_KEY" ]; then
    echo "Error: OPENAI_API_KEY environment variable is not set"
    echo "Please set it with: export OPENAI_API_KEY=your_api_key_here"
    exit 1
fi

echo "Testing Deep Research with OWID MCP server..."
echo "Request: Coal production per capita comparison between France and Germany"
echo "Using server: https://mcp.owid.io/mcp/"
echo ""

curl https://api.openai.com/v1/responses \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $OPENAI_API_KEY" \
  -d '{
  "model": "o4-mini-deep-research",
  "input": [
    {
      "role": "user",
      "content": [
        {
          "type": "input_text",
          "text": "Can you compare coal production per capita of France and Germany over the history?"
        }
      ]
    }
  ],
  "reasoning": {
    "summary": "auto"
  },
  "tools": [
    {
      "type": "mcp",
      "server_label": "owid",
      "server_url": "https://mcp.owid.io/mcp/",
      "allowed_tools": [
        "search",
        "fetch"
      ],
      "require_approval": "never"
    }
  ]
}'