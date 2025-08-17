import urllib.parse
from pathlib import Path
from typing import AsyncGenerator, Literal

import pandas as pd
import requests
import streamlit as st
import yaml
from pydantic_ai import Agent
from pydantic_ai.agent import CallToolsNode
from pydantic_ai.messages import (
    PartDeltaEvent,
    PartStartEvent,
    TextPart,
    TextPartDelta,
)

from apps.wizard.app_pages.expert_agent.utils import CURRENT_DIR
from etl.analytics import ANALYTICS_URL, clean_sql_query, read_datasette
from etl.docs import (
    render_collection,
    render_dataset,
    render_grapher_config,
    render_indicator,
    render_origin,
    render_table,
)
from etl.files import ruamel_dump, ruamel_load
from etl.paths import BASE_DIR, DOCS_DIR

#######################################################
# LOAD KNOWLEDGE BASE
#######################################################
# General system prompt and context
with open(CURRENT_DIR / "context.yml", "r") as f:
    CONTEXT = yaml.safe_load(f)
SYSTEM_PROMPT = CONTEXT["system_prompt"]


# Analytics
@st.cache_data(show_spinner="Loading analytics documentation...", show_time=True)
def cached_analytics_docs():
    """Cache the analytics documentation (5 minutes)."""
    from apps.wizard.app_pages.expert_agent.read_analytics import get_analytics_db_docs

    docs = get_analytics_db_docs(max_workers=10)

    summary = {}
    tables_summary = {}
    for doc in docs:
        # Main summary
        summary[doc["name"]] = doc["description"]
        # Per-table summary
        table_summary = {col["name"]: col["description"] for col in doc["columns"]}
        tables_summary[doc["name"]] = ruamel_dump(table_summary)
    summary = ruamel_dump(summary)

    return summary, tables_summary


ANALYTICS_DB_OVERVIEW, ANALYTICS_DB_TABLE_DETAILS = cached_analytics_docs()

# ETL docs
with open(BASE_DIR / "mkdocs.yml", "r") as f:
    DOCS_INDEX = ruamel_load(f)
DOCS_INDEX = dict(DOCS_INDEX)

#######################################################
# AGENT
#######################################################

# MCP
# server = MCPServerStdio(
#     command=".venv/bin/fastmcp",
#     args=[
#         "run",
#         "owid_mcp/server.py",
#     ],
# )

# Pydantic AI Agent
agent = Agent(
    system_prompt=SYSTEM_PROMPT,
    retries=2,
    # mcp_servers=[server],
)

#######################################################
# STREAMING
#######################################################


async def agent_stream(prompt: str, model_name: str) -> AsyncGenerator[str, None]:
    """Stream agent response using run_stream.

    Args:
        prompt: The user prompt to process
        model_name: The model to use.

    Yields:
        str: Text chunks from the agent response
    """
    async with agent.run_stream(
        prompt,
        model=model_name,  # type: ignore
    ) as result:
        # Yield each message from the stream
        async for message in result.stream_text(delta=True):
            if message is not None:
                yield message

    # At the very end, after the streaming is complete
    # Capture the usage information in session state
    if hasattr(result, "usage"):
        st.session_state["last_usage"] = result.usage()


async def agent_stream2(prompt: str, model_name: str) -> AsyncGenerator[str, None]:
    """Stream agent response using iter method with detailed event handling.

    It uses a more sophisticated approach, to support streaming with models other than OpenAI. Reference: https://github.com/pydantic/pydantic-ai/issues/1007#issuecomment-2963469441

    Args:
        prompt: The user prompt to process
        model_name: The model to use.
    Yields:
        str: Text chunks from the agent response
    """
    # Use provided model_name or fall back to session state
    async with agent.iter(
        prompt,
        model=model_name,
    ) as run:
        nodes = []
        # Yield each message from the stream
        async for node in run:
            nodes.append(node)
            if Agent.is_model_request_node(node):
                is_final_synthesis_node = any(
                    isinstance(prev_node, CallToolsNode) for prev_node in nodes
                )  # Heuristic: check if tools were called before
                print(f"--- ModelRequestNode (Is Final Synthesis? {is_final_synthesis_node}) ---")
                async with node.stream(run.ctx) as request_stream:
                    async for event in request_stream:
                        print(f"Request Event: Data: {event!r}")
                        # Specifically track TextPartDelta for the final node
                        if (
                            # is_final_synthesis_node and
                            isinstance(event, PartDeltaEvent) and isinstance(event.delta, TextPartDelta)
                        ):
                            yield event.delta.content_delta
                        elif (
                            # is_final_synthesis_node and
                            isinstance(event, PartStartEvent) and isinstance(event.part, TextPart)
                        ):
                            yield event.part.content

            elif Agent.is_call_tools_node(node):
                print("--- CallToolsNode ---")
                async with node.stream(run.ctx) as handle_stream:
                    async for event in handle_stream:
                        print(f"Call Event Data: {event!r}")

    # At the very end, after the streaming is complete
    # Capture the usage information using callback or session state
    if hasattr(run, "usage"):
        st.session_state["last_usage"] = run.usage()


#######################################################
# TOOLS
#######################################################


# General context
@agent.tool_plain(docstring_format="google")
async def get_context(category_name: Literal["analytics", "metadata", "docs"]) -> str:
    """Get the context for the agent.

    Args:
        category_name: The name of the category to get context for. "analytics" for analytics questions, "metadata" for metadata/api reference, and "docs" for general documentation.

    Returns:
        str: The context for the specified category.
    """
    st.toast("**Tool use**: `get_context`", icon=":material/robot:")
    return CONTEXT["context"][category_name]


# Documentation
@agent.tool_plain(docstring_format="google")
async def get_docs_index() -> str:
    """Get the documentation index, which summarizes the available documentation files.

    The format is like

    ```
    - Getting started:
      - "getting-started/index.md"
      - Installation: "getting-started/working-environment.md"
      - First steps: "getting-started/building-datasets.md"
      - Contributing: "contributing.md"
    ```

    where values before ":" are the section names, and values after ":", are the file paths. E.g. "Installation" is a section under "Getting started", and its file path is "getting-started/working-environment.md". Or "getting-started/index.md" is the file path for the "Getting started" section.

    Returns:
        str: The documentation index. List of available section and pages.
    """
    st.toast("**Tool use**: `get_docs_index`", icon=":material/robot:")
    docs = ruamel_dump(DOCS_INDEX["nav"])
    return docs


def read_page_md(page_path: str | Path) -> str:
    """Read text from MD page, add header with page path."""
    with open(page_path, "r") as f:
        text = f.read()
    text = f"_page: {page_path}_\n\n" + text
    return text


@agent.tool_plain(docstring_format="google")
async def get_docs_page(file_path: str) -> str:
    """Get the documentation from a specific file_path.

    Args:
        file_path: The path to the file containing the documentation we are interested in.

    Returns:
        str: The documentation for the specified file_path.
    """
    st.toast(f"**Tool use**: `get_docs_page`, `file_path='{file_path}'`", icon=":material/robot:")
    if (DOCS_DIR / file_path).exists():
        docs = read_page_md(DOCS_DIR / file_path)
    else:
        return f"File not found: {file_path}"
    return docs


# Analytics
@agent.tool_plain(docstring_format="google")
async def get_db_tables() -> str:
    """Get an overview of the database tables. Retrieve the names of the tables in the database, along with their short descriptions.

    Returns:
        str: Table short descriptions in format "table1: ...\ntable2: ...".
    """
    st.toast("**Tool use**: `get_db_tables`", icon=":material/robot:")
    return ANALYTICS_DB_OVERVIEW


@agent.tool_plain(docstring_format="google")
async def get_db_table_fields(tb_name: str) -> str:
    """Retrieve the documentation of the columns of database table "tb_name".

    Args:
        tb_name: Name of the table

    Returns:
        str: Table documentation as string, mapping column names to their descriptions. E.g. "column1: description1\ncolumn2: description2".
    """
    st.toast(f"**Tool use**: `get_db_table_fields`, `table='{tb_name}'`", icon=":material/robot:")
    if tb_name not in ANALYTICS_DB_TABLE_DETAILS:
        print("Table not found:", tb_name)
        print("Available tables:", sorted(ANALYTICS_DB_TABLE_DETAILS.keys()))
        return "Table not found: " + tb_name
    return ANALYTICS_DB_TABLE_DETAILS[tb_name]


# API reference for Metadata
@agent.tool_plain(docstring_format="google")
async def get_api_reference_metadata(
    object_name: Literal["dataset", "table", "indicator", "origin", "collection", "grapher_config"],
) -> str:
    """Get the API reference documentation for a certain object's metadata.

    Args:
        object_name: The type of object to get metadata for.

    Returns:
        str: Metadata for the specified object type.
    """
    st.toast(f"**Tool use**: `get_db_table_fields`, `object_name='{object_name}'`", icon=":material/robot:")
    match object_name:
        case "dataset":
            return render_dataset()
        case "table":
            return render_table()
        case "indicator":
            return render_indicator()
        case "collection":
            return render_collection()
        case "grapher_config":
            return render_grapher_config()
        case "origin":
            return f"""# Origins:
{render_origin()}

------
#### `variable.presentation.grapher_config`

{render_grapher_config()}
"""
        case _:
            return "Invalid object name: " + object_name


@agent.tool_plain(docstring_format="google")
async def generate_url_to_datasette(query: str) -> str:
    """Generate a URL to the Datasette instance with the given query.

    Args:
        query: Query to Datasette instance.
    Returns:
        str: URL to the Datasette instance with the query. The URL links to a datasette preview with the SQL query and its results.
    """
    st.toast("**Tool use**: `generate_url_to_datasette`", icon=":material/robot:")
    return _generate_url_to_datasette(query)


def _generate_url_to_datasette(query: str) -> str:
    query = clean_sql_query(query)
    return f"{ANALYTICS_URL}?" + urllib.parse.urlencode({"sql": query, "_size": "max"})


@agent.tool_plain(docstring_format="google")
async def validate_datasette_query(query: str) -> str:
    """Validate an SQL query.

    Args:
        query: Query to Datasette instance.
    Returns:
        str: Validation result message. If the query is not valid, it will return an error message. Use it to improve the query!
    """
    st.toast("**Tool use**: `validate_datasette_query`", icon=":material/robot:")
    url = _generate_url_to_datasette(f"{query} LIMIT 1")
    url = url.replace(ANALYTICS_URL, ANALYTICS_URL + ".json")
    response = requests.get(url).json()
    if response["ok"]:
        if ("rows" not in response) or not isinstance(response["rows"], list):
            text = "Query is invalid! Check for correctness, it must be DuckDB compatible! Seems like no rows were returned."
        if len(response["rows"]) == 0:
            text = "Query is valid, but no results found. It might be intended, or an unexpected error. Check the query for correctness. It must be DuckDB compatible!"
        else:
            text = "Query is valid!"
    else:
        text = f"Query is invalid! Check for correctness, it must be DuckDB compatible! `\nError: {response['error']}"
    return text


@agent.tool_plain(docstring_format="google")
async def get_data_from_datasette(query: str) -> str:
    """Execute a query in the semantic layer in Datasette and get the actual data results.

    Args:
        query: Query to Datasette instance.
    Returns:
        pd.DataFrame: DataFrame with the results of the query.
    """
    st.toast("**Tool use**: `get_data_from_datasette`", icon=":material/robot:")
    df = read_datasette(query, use_https=False)
    if df.empty:
        return ""

    result = df.head().to_markdown(index=False)
    if result is None:
        return ""
    return result
