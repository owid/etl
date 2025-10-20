"""Chart search MCP server for ChatGPT App.

This server exposes a tool to search for charts based on keywords and returns
them as interactive iframes. Uses OWID's real chart data via the existing
owid_mcp charts module. Uses low-level MCP protocol for ChatGPT App widget support.
"""
# pyright: reportMissingImports=false

from __future__ import annotations

from functools import lru_cache
from typing import Any, Dict, List

import mcp.types as types
import uvicorn
from mcp.server.fastmcp import FastMCP
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from starlette.middleware.cors import CORSMiddleware

from owid_mcp.data_utils import make_algolia_request

MIME_TYPE = "text/html+skybridge"


class SearchChartsInput(BaseModel):
    """Schema for chart search tool."""

    query: str = Field(
        ...,
        description="Search query with keywords to find relevant charts (e.g., 'population density', 'CO2 emissions')",
    )

    model_config = ConfigDict(extra="forbid")


mcp = FastMCP(
    name="owid-chart-search",
    stateless_http=True,
)


WIDGET_TEMPLATE_URI = "ui://widget/chart-viewer.html"
WIDGET_TITLE = "OWID Chart Viewer"


TOOL_INPUT_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "query": {
            "type": "string",
            "description": "Search query with keywords to find relevant charts (e.g., 'population density', 'CO2 emissions')",
        }
    },
    "required": ["query"],
    "additionalProperties": False,
}


def _tool_meta() -> Dict[str, Any]:
    return {
        "openai/outputTemplate": WIDGET_TEMPLATE_URI,
        "openai/toolInvocation/invoking": "Searching OWID charts",
        "openai/toolInvocation/invoked": "Found charts",
        "openai/widgetAccessible": True,
        "openai/resultCanProduceWidget": True,
    }


@lru_cache(maxsize=128)
def _generate_chart_widget_html(chart_slug: str, chart_title: str, chart_url: str) -> str:
    """Generate HTML widget for displaying a chart iframe."""
    return f"""<!doctype html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <style>
    * {{
      margin: 0;
      padding: 0;
      box-sizing: border-box;
    }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      background: white;
      padding: 16px;
    }}
    .chart-container {{
      max-width: 100%;
      margin: 0 auto;
    }}
    .chart-header {{
      margin-bottom: 12px;
    }}
    .chart-title {{
      font-size: 18px;
      font-weight: 600;
      color: #1a1a1a;
      margin-bottom: 8px;
    }}
    .chart-link {{
      font-size: 14px;
      color: #0066cc;
      text-decoration: none;
    }}
    .chart-link:hover {{
      text-decoration: underline;
    }}
    .chart-iframe {{
      width: 100%;
      height: 600px;
      border: 1px solid #e0e0e0;
      border-radius: 8px;
    }}
  </style>
</head>
<body>
  <div class="chart-container">
    <div class="chart-header">
      <div class="chart-title" id="chart-title">{chart_title}</div>
      <a href="{chart_url}" target="_blank" class="chart-link" id="chart-link">View full chart on OWID →</a>
    </div>
    <iframe
      id="chart-iframe"
      src="{chart_url}"
      loading="lazy"
      allow="web-share; clipboard-write"
      class="chart-iframe">
    </iframe>
  </div>
  <script>
    // Listen for structured content from MCP
    window.addEventListener('message', (event) => {{
      if (event.data.type === 'structuredContent') {{
        const data = event.data.content;
        if (data.title) {{
          document.getElementById('chart-title').textContent = data.title;
        }}
        if (data.chartUrl) {{
          const iframe = document.getElementById('chart-iframe');
          iframe.src = data.chartUrl;
          const link = document.getElementById('chart-link');
          link.href = data.chartUrl;
        }}
      }}
    }});

    // Alternative: Direct hydration from structured content
    if (window.structuredContent) {{
      const data = window.structuredContent;
      if (data.title) {{
        document.getElementById('chart-title').textContent = data.title;
      }}
      if (data.chartUrl) {{
        document.getElementById('chart-iframe').src = data.chartUrl;
        document.getElementById('chart-link').href = data.chartUrl;
      }}
    }}
  </script>
</body>
</html>"""


def _embedded_widget_resource(chart_slug: str, chart_title: str, chart_url: str) -> types.EmbeddedResource:
    return types.EmbeddedResource(
        type="resource",
        resource=types.TextResourceContents(
            uri=WIDGET_TEMPLATE_URI,
            mimeType=MIME_TYPE,
            text=_generate_chart_widget_html(chart_slug, chart_title, chart_url),
            title=WIDGET_TITLE,
        ),
    )


@mcp._mcp_server.list_tools()
async def _list_tools() -> List[types.Tool]:
    return [
        types.Tool(
            name="search-charts",
            title="Search OWID Charts",
            description="Search Our World in Data charts based on keywords and return them as interactive iframes",
            inputSchema=TOOL_INPUT_SCHEMA,
            _meta=_tool_meta(),
            annotations={
                "destructiveHint": False,
                "openWorldHint": False,
                "readOnlyHint": True,
            },
        )
    ]


@mcp._mcp_server.list_resources()
async def _list_resources() -> List[types.Resource]:
    return [
        types.Resource(
            name=WIDGET_TITLE,
            title=WIDGET_TITLE,
            uri=WIDGET_TEMPLATE_URI,
            description="OWID chart viewer widget for displaying data visualizations",
            mimeType=MIME_TYPE,
            _meta=_tool_meta(),
        )
    ]


@mcp._mcp_server.list_resource_templates()
async def _list_resource_templates() -> List[types.ResourceTemplate]:
    return [
        types.ResourceTemplate(
            name=WIDGET_TITLE,
            title=WIDGET_TITLE,
            uriTemplate=WIDGET_TEMPLATE_URI,
            description="OWID chart viewer widget template",
            mimeType=MIME_TYPE,
            _meta=_tool_meta(),
        )
    ]


async def _handle_read_resource(req: types.ReadResourceRequest) -> types.ServerResult:
    if str(req.params.uri) != WIDGET_TEMPLATE_URI:
        return types.ServerResult(
            types.ReadResourceResult(
                contents=[],
                _meta={"error": f"Unknown resource: {req.params.uri}"},
            )
        )

    # Return a default chart widget HTML (GDP per capita as a reasonable default)
    default_url = "https://ourworldindata.org/grapher/gdp-per-capita-worldbank"
    default_title = "GDP per capita"
    contents = [
        types.TextResourceContents(
            uri=WIDGET_TEMPLATE_URI,
            mimeType=MIME_TYPE,
            text=_generate_chart_widget_html("gdp-per-capita-worldbank", default_title, default_url),
            _meta=_tool_meta(),
        )
    ]

    return types.ServerResult(types.ReadResourceResult(contents=contents))


async def _call_tool_request(req: types.CallToolRequest) -> types.ServerResult:
    if req.params.name != "search-charts":
        return types.ServerResult(
            types.CallToolResult(
                content=[
                    types.TextContent(
                        type="text",
                        text=f"Unknown tool: {req.params.name}",
                    )
                ],
                isError=True,
            )
        )

    arguments = req.params.arguments or {}
    try:
        payload = SearchChartsInput.model_validate(arguments)
    except ValidationError as exc:
        return types.ServerResult(
            types.CallToolResult(
                content=[
                    types.TextContent(
                        type="text",
                        text=f"Input validation error: {exc.errors()}",
                    )
                ],
                isError=True,
            )
        )

    # Search for charts using OWID's Algolia API
    hits = await make_algolia_request(payload.query, limit=10)

    if not hits:
        return types.ServerResult(
            types.CallToolResult(
                content=[
                    types.TextContent(
                        type="text",
                        text=f"No charts found for query: {payload.query}",
                    )
                ],
                isError=False,
            )
        )

    # Get the first result and extract chart info
    first_hit = hits[0]
    chart_slug = first_hit["slug"]
    chart_title = first_hit.get("title") or chart_slug.replace("-", " ").title()
    chart_url = f"https://ourworldindata.org/grapher/{chart_slug}"

    # Remove .csv extension to get interactive chart URL
    if chart_url.endswith(".csv"):
        chart_url = chart_url[:-4]

    widget_resource = _embedded_widget_resource(chart_slug, chart_title, chart_url)

    # Build response text with all results
    response_text = f"Found {len(hits)} chart(s) from Our World in Data:\n\n"
    response_text += f"**{chart_title}**\n"
    response_text += f"[View interactive chart]({chart_url})\n\n"

    if len(hits) > 1:
        response_text += "Other relevant charts:\n"
        for other_hit in hits[1:6]:  # Show up to 5 more
            other_slug = other_hit["slug"]
            other_title = other_hit.get("title") or other_slug.replace("-", " ").title()
            other_url = f"https://ourworldindata.org/grapher/{other_slug}"
            response_text += f"- [{other_title}]({other_url})\n"

    meta: Dict[str, Any] = {
        "openai.com/widget": widget_resource.model_dump(mode="json"),
        "openai/outputTemplate": WIDGET_TEMPLATE_URI,
        "openai/toolInvocation/invoking": "Searching OWID charts",
        "openai/toolInvocation/invoked": "Found charts",
        "openai/widgetAccessible": True,
        "openai/resultCanProduceWidget": True,
    }

    return types.ServerResult(
        types.CallToolResult(
            content=[
                types.TextContent(
                    type="text",
                    text=response_text,
                )
            ],
            structuredContent={
                "title": chart_title,
                "chartUrl": chart_url,
                "slug": chart_slug,
            },
            _meta=meta,
        )
    )


mcp._mcp_server.request_handlers[types.CallToolRequest] = _call_tool_request
mcp._mcp_server.request_handlers[types.ReadResourceRequest] = _handle_read_resource


app = mcp.streamable_http_app()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=False,
)


if __name__ == "__main__":
    uvicorn.run("chatgpt_app.server:app", host="0.0.0.0", port=8001)
