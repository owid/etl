from pathlib import Path
from typing import Literal

import streamlit as st
import yaml
from pydantic_ai import Agent
from pydantic_ai.mcp import MCPServerStdio

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

CURRENT_DIR = Path(__file__).parent

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
    from apps.wizard.app_pages.expert.read_analytics import get_analytics_db_docs

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
