from pathlib import Path
from typing import Any, AsyncGenerator, List, Literal

import logfire
import streamlit as st
import yaml
from pydantic import BaseModel
from pydantic_ai import Agent

# from pydantic_ai.agent import CallToolsNode
from pydantic_ai.mcp import CallToolFunc, MCPServerStreamableHTTP, ToolResult
from pydantic_ai.messages import (
    PartDeltaEvent,
    PartStartEvent,
    TextPart,
    TextPartDelta,
)
from pydantic_ai.models.openai import OpenAIResponsesModelSettings
from pydantic_ai.tools import RunContext

from apps.wizard.app_pages.expert_agent.utils import CURRENT_DIR, DataFrameModel, QueryResult, log, serialize_df
from etl.analytics.datasette import (
    DatasetteSQLError,
    _generate_url_to_datasette,
    read_datasette,
)
from etl.analytics.metabase import _generate_question_url, create_question
from etl.config import GOOGLE_API_KEY, LOGFIRE_TOKEN_EXPERT, OWID_MCP_SERVER_URL
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

logfire.configure(token=LOGFIRE_TOKEN_EXPERT)
logfire.instrument_pydantic_ai()

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
    from apps.wizard.app_pages.expert_agent.read_analytics import get_metabase_db_docs

    docs = get_metabase_db_docs()

    summary = {}
    tables_summary = {}
    for doc in docs:
        # Main summary
        summary[doc["table"]] = doc["description"]
        # Per-table summary
        table_summary = {col["name"]: col["description"] for col in doc["columns"]}
        tables_summary[doc["table"]] = ruamel_dump(table_summary)
    summary = ruamel_dump(summary)

    return summary, tables_summary


ANALYTICS_DB_OVERVIEW, ANALYTICS_DB_TABLE_DETAILS = cached_analytics_docs()

# ETL docs
with open(BASE_DIR / "mkdocs.yml", "r") as f:
    DOCS_INDEX = ruamel_load(f)
DOCS_INDEX = dict(DOCS_INDEX)

#######################################################
# MCPs
#######################################################


async def process_tool_call(
    ctx: RunContext[int],
    call_tool: CallToolFunc,
    name: str,
    tool_args: dict[str, Any],
) -> ToolResult:
    """A tool call processor that passes along the deps."""
    st.markdown(
        f"**:material/compare_arrows: MCP**: Querying OWID MCP, method `{name}`"
    )  # , icon=":material/compare_arrows:")
    return await call_tool(name, tool_args, {"deps": ctx.deps})


## Trying to tweak the settings for OpenAI responses
settings = OpenAIResponsesModelSettings(
    # openai_reasoning_effort="low",
    # openai_reasoning_summary="detailed",
    openai_truncation="auto",
)


## Use MCPs or not based on user input
def get_toolsets():
    if ("expert_use_mcp" in st.session_state) and st.session_state["expert_use_mcp"]:
        # Create MCP server instance inside function to avoid event loop binding issues
        mcp_server_prod = MCPServerStreamableHTTP(
            url=OWID_MCP_SERVER_URL,
            process_tool_call=process_tool_call,
            tool_prefix="owid_data_",
        )
        return [mcp_server_prod]
    return []


#######################################################
# AGENTS
#######################################################

# Main Agent
agent = Agent(
    instructions=SYSTEM_PROMPT,
    retries=2,
    model_settings=settings,
)

# Agent for recommending follow-up questions
if GOOGLE_API_KEY:
    MODEL_SUGGESTIONS = "google-gla:gemini-2.5-flash"
else:
    MODEL_SUGGESTIONS = "openai:gpt-5-mini"
recommender_agent = Agent(
    model=MODEL_SUGGESTIONS,
    instructions="""You will get a conversation history. Based on it, recommend 3 follow-up questions that the user could ask next. The questions should be short, concise, to the point, and should be framed as if the user was asking them. Example: 'How many articles did we publish in 2025?'. Two questions should be related to the conversation, and on should be more tangential to explore a different topic, but still concrete.""",
    output_type=list[str],
    retries=2,
)

# Agent for summarizing the conversation. Currently not in use.
summarize_agent = Agent(
    # "openai:gpt-5-nano",
    instructions="""You will get a chat history between a user and an LLM. Summarize the content shared by the two parties.""",
    output_type=str,
    retries=2,
)

#######################################################
# STREAMING
#######################################################


def _get_model_from_name(model_name: str):
    if model_name == "llama3.2":
        from pydantic_ai.models.openai import OpenAIChatModel
        from pydantic_ai.providers.ollama import OllamaProvider

        model = OpenAIChatModel(
            model_name="llama3.2",
            provider=OllamaProvider(base_url="http://localhost:11434/v1"),
        )

    else:
        model = model_name
    return model


async def agent_stream(prompt: str, model_name: str, message_history) -> AsyncGenerator[str, None]:
    """Stream agent response using run_stream.

    Args:
        prompt: The user prompt to process
        model_name: The model to use.

    Yields:
        str: Text chunks from the agent response
    """
    model = _get_model_from_name(model_name)
    async with agent.run_stream(
        prompt,
        model=model,  # type: ignore
        message_history=message_history,
        toolsets=get_toolsets(),  # type: ignore
    ) as result:
        # Yield each message from the stream
        async for message in result.stream_text(delta=True):
            if message is not None:
                yield message

    # At the very end, after the streaming is complete
    # Capture the usage information in session state
    if hasattr(result, "usage"):
        st.session_state["last_usage"] = result.usage()


async def _collect_agent_stream2(prompt: str, model_name: str, message_history) -> List[str]:
    """Collect all chunks from agent_stream2 in one async context to avoid task switching issues."""
    toolsets = get_toolsets()
    print("===========================================")
    print("Toolsets available")
    print(toolsets)
    print("===========================================")

    chunks = []
    model = _get_model_from_name(model_name)

    async with agent.iter(
        prompt,
        model=model,
        message_history=message_history,
        toolsets=toolsets,  # type: ignore
    ) as run:
        nodes = []
        async for node in run:
            # print(f"--------------------------")
            # print(node)
            nodes.append(node)
            if Agent.is_model_request_node(node):
                # is_final_synthesis_node = any(isinstance(prev_node, CallToolsNode) for prev_node in nodes)
                # print(f"--- ModelRequestNode (Is Final Synthesis? {is_final_synthesis_node}) ---")
                async with node.stream(run.ctx) as request_stream:
                    async for event in request_stream:
                        # print(f"Request Event: Data: {event!r}")
                        if isinstance(event, PartDeltaEvent) and isinstance(event.delta, TextPartDelta):
                            chunks.append(event.delta.content_delta)
                        elif isinstance(event, PartStartEvent) and isinstance(event.part, TextPart):
                            chunks.append(event.part.content)

            elif Agent.is_call_tools_node(node):
                # print("--- CallToolsNode ---")
                async with node.stream(run.ctx) as handle_stream:
                    async for event in handle_stream:
                        pass
                        # print(f"Call Event Data: {event!r}")

        # Capture usage and result info
        if hasattr(run, "usage"):
            st.session_state["last_usage"] = run.usage()
        if hasattr(run, "result"):
            st.session_state["agent_result"] = run.result

    return chunks


async def agent_stream2(prompt: str, model_name: str, message_history) -> AsyncGenerator[str, None]:
    """Stream agent response using iter method with Streamlit-compatible wrapper.

    This version collects all chunks in one async context first, then yields them
    to avoid async context manager issues with Streamlit's task switching.

    Args:
        prompt: The user prompt to process
        model_name: The model to use.
    Yields:
        str: Text chunks from the agent response
    """
    # Collect all chunks first to avoid async context issues with Streamlit
    # with st.spinner("Asking LLM...", show_time=True):
    with st.status("Talking with the expert...", expanded=False) as status:
        st.markdown(
            f"**:material/smart_toy: Agent working**: `{st.session_state['expert_config']['model_name']}`",
        )
        chunks = await _collect_agent_stream2(
            prompt,
            model_name,
            message_history,
        )
        status.update(label="Got the answer!", state="complete", expanded=False)

    # Yield chunks one by one
    for chunk in chunks:
        if chunk:  # Only yield non-empty chunks
            yield chunk


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
    st.markdown(
        f"**:material/construction: Tool use**: Getting context on category '{category_name}', using `get_context`"
    )  # , icon=":material/calculate:")
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
    st.markdown(
        "**:material/construction: Tool use**: Getting the table of contents of ETL documentation, via `get_docs_index`"
    )  # , icon=":material/calculate:")
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
    st.markdown(
        f"**:material/construction: Tool use**: Getting ETL docs page '{file_path}', via `get_docs_page`"
    )  # , icon=":material/calculate:")
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
    st.markdown(
        "**:material/construction: Tool use**: Getting the database details of our semantic layer, via `get_db_tables`"
    )  # , icon=":material/calculate:")
    return ANALYTICS_DB_OVERVIEW


@agent.tool_plain(docstring_format="google")
async def get_db_table_fields(tb_name: str) -> str:
    """Retrieve the documentation of the columns of database table "tb_name".


    Args:
        tb_name: Name of the table

    Returns:
        str: Table documentation as string, mapping column names to their descriptions. E.g. "column1: description1\ncolumn2: description2".
    """
    st.markdown(
        f"**:material/construction: Tool use**: Getting documentation of table `{tb_name}` from the semantic layer, via `get_db_table_fields`"
    )  # , icon=":material/calculate:")
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
    st.markdown(
        f"**:material/construction: Tool use**: Getting metadata documentation for object '{object_name}', via `get_api_reference_metadata')`"
    )  # , icon=":material/calculate:")
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
async def execute_query(query: str, title: str, description: str, num_rows: int = 10) -> QueryResult:
    """Execute a query in the semantic layer and get the data results.

    The query is ran on Datasette, and if valid, it obtains the results from Datasette and creates a "question" in Metabase with the same query. Note that Metabase and Datasette use the same underlying database, so the query should work in both places.

    If the query is invalid, it returns an error message that can be used to improve the query until valid.

    Args:
        query: Query to Datasette instance.
        title: Title that describes what the query does. Should be short and concise.
        description: Description of what the query does. Should be 1-3 sentence long.
        num_rows: Number of rows to return. Defaults to 10. Too many rows may increase the tokens. Be mindful when increasing this number.
    Returns:
        QueryResult: Serializable object with the following fields:
            message (str): Message about the query execution. "SUCCCESS" if the query was valid, or an error message if not.
            valid (bool): Whether the query was valid or not.
            result (str): The data results in markdown format (first `num_rows` rows).
            url_metabase (str): Link to the created question in Metabase, if the query was valid and the question was created successfully.
            url_datasette (str): Link to the query in Datasette.
    """
    st.markdown(
        f"**:material/construction: Tool use**: Getting data ({num_rows} rows) from Datasette, via `execute_query`"
    )  # , icon=":material/calculate:")

    try:
        df = read_datasette(query, use_https=False)
        url_datasette = _generate_url_to_datasette(query)

        if df.empty:
            return QueryResult(
                message="Query returned no results. Try it on Datasette",
                valid=False,
                url_datasette=url_datasette,
            )

        # Serialize dataframe
        data = serialize_df(df, num_rows=num_rows)

        if data.data != []:
            return QueryResult(
                message="Query returned no results. Try it on Datasette",
                valid=False,
                url_datasette=url_datasette,
            )

        try:
            url_metabase = create_question_in_metabase(query=query, title=title, description=description)
        except Exception as _:
            url_datasette = url_datasette
            url_metabase = None
        return QueryResult(
            message="SUCCESS",
            valid=True,
            result=data,
            url_metabase=url_metabase,
            url_datasette=url_datasette,
        )
    except (DatasetteSQLError,) as e:
        # Handle specific Datasette-related errors
        return QueryResult(
            message=f"ERROR. Query is invalid! Check for correctness, it must be DuckDB compatible!\nError: {e}",
            valid=False,
        )


# Create question in Metabase
def create_question_in_metabase(query: str, title: str, description: str) -> str:
    """Create a question in Metabase with the given SQL query and title.

    This tool should be used once we are sure that the query is valid in Datasette.

    Args:
        query: Query user for Datasette/Metabase.
        title: Title that describes what the query does. Should be short, but concise.
    Returns:
        str: Link to the created question in Metabase. This link can be shared with others to access the question directly.
    """
    log.info(f"Creating Metabase question for query '{title}'")
    st.markdown("**:material/add_circle: Creating metabase question**")  # , icon=":material/calculate:")

    # Create question in Metabase
    question = create_question(
        query=query,
        title=title,
        description=description,
    )
    # Obtain URL to the question
    url = _generate_question_url(question)

    return url


@agent.tool_plain(docstring_format="google")
async def list_available_questions_metabase() -> List[dict]:
    """List available questions in Metabase in the "Expert" collection.

    Returns a list of questions with their metadata. Use the question names (and descriptions if available) to decide which question to use.

    Returns:
        List[dict]: List of questions with their metadata. Each item has the following fields:
            - name (str): Name of the question.
            - id (int): ID of the question.
            - description (str, optional): Description of the question, if available.
    """
    from etl.analytics.metabase import list_questions

    st.markdown(
        "**:material/construction: Tool use**: Listing available questions in Metabase, via `list_available_questions_metabase`"
    )

    # Get questions
    questions = list_questions()

    # Create question summary
    summary = []
    for q in questions:
        summary_ = {
            "name": q["name"],
            "id": q["id"],
        }
        if (q["description"] is not None) and (q["description"].strip() != ""):
            summary_["description"] = q["description"]
        summary.append(summary_)
    return summary


@agent.tool_plain(docstring_format="google")
async def get_question_data(card_id: int, num_rows: int = 20) -> QueryResult:
    """Get the data from a Metabase question by its card ID.

    After choosing a question from the list of available questions, use this tool to get the data from that question.

    Args:
        card_id: The ID of the question card in Metabase.
        num_rows: Number of rows to return. Defaults to 20.
    Returns:
        DataFrameModel: Serializable object with the following fields:
            columns (list[str]): List of column names in the dataframe.
            dtypes (dict[str, str]): Dictionary mapping column names to their data types.
            data (list[list]): Small slice of the data (first `num_rows` rows).
            total_rows (int): Total number of rows in the dataframe.
    """
    from etl.analytics.metabase import get_question_data, get_question_info

    # Getting data
    data = get_question_data(card_id)

    # Get question
    question = get_question_info(card_id)

    q_name = question.get("name", "Unknown")
    st.markdown(
        f"**:material/construction: Tool use**: Getting data from a Metabase question, via `get_question_data`, using id `{card_id}` for questio nnamed '{q_name}'"
    )

    # GEnerate URL
    url = _generate_question_url(question)

    # Serialize
    data = serialize_df(data, num_rows=num_rows)

    # Build result
    result = QueryResult(
        message="SUCCESS",
        valid=True,
        result=data,
        url_metabase=url,
    )
    return result
