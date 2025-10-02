import uuid
from pathlib import Path
from typing import Any, Dict, List, Literal

import logfire
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import yaml
from pydantic_ai import Agent

# from pydantic_ai.agent import CallToolsNode
from pydantic_ai.mcp import CallToolFunc, MCPServerStreamableHTTP, ToolResult
from pydantic_ai.models.openai import OpenAIResponsesModelSettings
from pydantic_ai.tools import RunContext

# from RestrictedPython import compile_restricted, safe_globals
from apps.wizard.app_pages.expert_agent.media import save_code_file, save_plot_file
from apps.wizard.app_pages.expert_agent.utils import CURRENT_DIR, MODEL_DEFAULT, QueryResult, log, serialize_df
from etl.analytics.datasette import (
    DatasetteSQLError,
    _generate_url_to_datasette,
    read_datasette,
)
from etl.analytics.metabase import _generate_question_url, create_question, get_question_info
from etl.analytics.metabase import get_question_data as _get_question_data
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
    ctx: RunContext[dict],
    call_tool: CallToolFunc,
    name: str,
    tool_args: dict[str, Any],
) -> ToolResult:
    """A tool call processor that passes along the deps."""
    # tool_ui_message(
    #     message_type="markdown",
    #     text=f"**:material/compare_arrows: MCP**: Querying OWID MCP, method `{name}`",
    # )
    return await call_tool(name, tool_args, {"deps": ctx.deps})


## Trying to tweak the settings for OpenAI responses
settings = OpenAIResponsesModelSettings(
    # openai_reasoning_effort="low",
    # openai_reasoning_summary="detailed",
    openai_truncation="auto",
)


## Use MCPs or not based on user input
def get_toolsets():
    # if ("expert_use_mcp" in st.session_state) and st.session_state["expert_use_mcp"]:
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
    toolsets=get_toolsets(),  # type: ignore
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

# Agent for generating plotting code
plotting_agent = Agent(
    model=MODEL_DEFAULT,  # Use the same default model as other agents
    instructions="""You are a data visualization expert specializing in plotly. You receive information about a pandas DataFrame and user instructions for creating a plot.

Your task is to generate clean, executable Python code using plotly that creates the requested visualization.

IMPORTANT RULES:
1. Use only these imported libraries: pandas (as pd), plotly.express (as px), plotly.graph_objects (as go)
2. The DataFrame is already loaded as variable 'df'
3. Create a plot and assign the result to variable 'fig'
4. Do NOT call fig.show(), fig.write_image(), or any file operations
5. Do NOT import any libraries - they are already available
6. Return ONLY the Python code, no explanations or markdown formatting
7. Make the plots visually appealing with proper titles, axis labels, and formatting
8. Handle edge cases like missing data gracefully

Common plot types and their plotly.express functions:
- Line chart: px.line()
- Bar chart: px.bar()
- Scatter plot: px.scatter()
- Histogram: px.histogram()
- Box plot: px.box()
- Heatmap: px.imshow() or go.Heatmap()
- Pie chart: px.pie()

Example output for "create a line chart showing values over time":
```python
fig = px.line(df, x='date', y='value', title='Values Over Time')
fig.update_layout(height=600, showlegend=True)
fig.update_xaxes(title='Date')
fig.update_yaxes(title='Value')
```

Always include proper titles and axis labels to make the plot self-explanatory.""",
    output_type=str,
    retries=2,
)


#######################################################
# STREAMING
#######################################################
def run_agent_stream(prompt: str, structured: bool = False, question_id: str | None = None):
    print("============================================")
    print("01---------------------------")
    print(st.session_state)
    print("---------------------------")
    tools = agent._get_toolset()
    print(tools)
    if structured:
        from apps.wizard.app_pages.expert_agent.stream import agent_stream_sync_structured

        stream_func = agent_stream_sync_structured

    else:
        from apps.wizard.app_pages.expert_agent.stream import agent_stream_sync

        stream_func = agent_stream_sync

    def handle_session_updates(updates):
        for key, value in updates.items():
            st.session_state[key] = value

    # Agent to work, and stream its output
    model_name = st.session_state["expert_config"].get("model_name", MODEL_DEFAULT)
    stream = stream_func(
        agent=agent,
        prompt=prompt,
        model_name=model_name,
        message_history=st.session_state["agent_messages"],
        session_updates_callback=handle_session_updates,
        question_id=question_id,
    )

    return stream


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
    # tool_ui_message(
    #     message_type="markdown",
    #     text=f"**:material/construction: Tool use**: Getting context on category '{category_name}', using `get_context`",
    # )
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
    # tool_ui_message(
    #     message_type="markdown",
    #     text="**:material/construction: Tool use**: Getting the table of contents of ETL documentation, via `get_docs_index`",
    # )
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
    # tool_ui_message(
    #     message_type="markdown",
    #     text=f"**:material/construction: Tool use**: Getting ETL docs page '{file_path}', via `get_docs_page`",
    # )
    if (DOCS_DIR / file_path).exists():
        docs = read_page_md(DOCS_DIR / file_path)
    else:
        return f"File not found: {file_path}"
    return docs


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
    # tool_ui_message(
    #     message_type="markdown",
    #     text=f"**:material/construction: Tool use**: Getting metadata documentation for object '{object_name}', via `get_api_reference_metadata')`",
    # )
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


# Analytics
@agent.tool_plain(docstring_format="google")
async def get_db_tables() -> str:
    """Get an overview of the database tables. Retrieve the names of the tables in the database, along with their short descriptions.

    Returns:
        str: Table short descriptions in format "table1: ...\ntable2: ...".
    """
    # tool_ui_message(
    #     message_type="markdown",
    #     text="**:material/construction: Tool use**: Getting the database details of our semantic layer, via `get_db_tables`",
    # )
    return ANALYTICS_DB_OVERVIEW


@agent.tool_plain(docstring_format="google")
async def get_db_table_fields(tb_names: List[str]) -> Dict[str, Any]:
    """Retrieve the documentation of the columns of a subset of tables in the database table.


    Args:
        tb_names: Names of the tables of interest

    Returns:
        Dict: Documentation of the tables of interest. Each key in the dictionary corresponds to a table. The values is the table documentation as string, mapping column names to their descriptions. E.g. "column1: description1\ncolumn2: description2".
    """
    # tool_ui_message(
    #     message_type="markdown",
    #     text=f"**:material/construction: Tool use**: Getting documentation of table `{tb_name}` from the semantic layer, via `get_db_table_fields`",
    # )
    result = {}
    for tb_name in tb_names:
        if tb_name not in ANALYTICS_DB_TABLE_DETAILS:
            text = f"Table not found: {tb_name}"
        else:
            text = ANALYTICS_DB_TABLE_DETAILS[tb_name]
        result[tb_name] = text
    return result


# Metabase/Datasette
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
    # tool_ui_message(
    #     message_type="markdown",
    #     text=f"**:material/construction: Tool use**: Getting data ({num_rows} rows) from Datasette, via `execute_query`",
    # )

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

        if data.data == []:
            return QueryResult(
                message="Query returned no results. Try it on Datasette",
                valid=False,
                url_datasette=url_datasette,
            )

        try:
            question = create_question(
                query=query,
                title=title,
                description=description,
            )
            # Obtain URL to the question
            url_metabase = _generate_question_url(question)
            card_id = question.get("id", None)
        except Exception as _:
            url_metabase = None
            card_id = None
        return QueryResult(
            message="SUCCESS",
            valid=True,
            result=data,
            url_metabase=url_metabase,
            url_datasette=url_datasette,
            card_id_metabase=card_id,
        )
    except (DatasetteSQLError,) as e:
        # Handle specific Datasette-related errors
        return QueryResult(
            message=f"ERROR. Query is invalid! Check for correctness, it must be DuckDB compatible!\nError: {e}",
            valid=False,
        )


# @agent.tool_plain(docstring_format="google")
# async def create_question_with_filters(query: str, title: str, description: str) -> None:
#     """Create a Metabase question that has filters."""
#     _ = create_question(
#         query=query,
#         title=title,
#         description=description,
#     )


# @agent.tool_plain(docstring_format="google")
# async def get_question_query(card_id: int):
#     # Get question
#     _ = get_question_info(card_id)
#     # Generate URL
#     # url = _generate_question_url(question)
#     # return url


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
    from etl.analytics.metabase import COLLECTION_EXPERT_ID, list_questions

    # tool_ui_message(
    #     message_type="markdown",
    #     text="**:material/construction: Tool use**: Listing available questions in Metabase, via `list_available_questions_metabase`",
    # )

    # Get questions
    questions = list_questions()

    # Skip those under collection expert
    questions = [q for q in questions if q.get("collection_id") != COLLECTION_EXPERT_ID]

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
async def get_question_url(card_id: int) -> str:
    """Get the URL to a Metabase question by its card ID.
    After choosing a question from the list of available questions, use this tool to get the URL to that question.

    Args:
        card_id: The ID of the question card in Metabase.
    Returns:
        str: The URL to the Metabase question.
    """
    # Get question
    question = get_question_info(card_id)
    # Generate URL
    url = _generate_question_url(question)
    return url


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
    # Getting data
    data = _get_question_data(card_id)

    if data.empty:
        return QueryResult(
            message="Query returned no results. Try it on Metabase",
            valid=False,
        )

    # q_name = question.get("name", "Unknown")
    # tool_ui_message(
    #     message_type="markdown",
    #     text=f"**:material/construction: Tool use**: Getting data from a Metabase question, via `get_question_data`, using id `{card_id}` for question named '{q_name}'",
    # )

    # Generate URL
    url = await get_question_url(card_id)

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


@agent.tool(docstring_format="google")
async def generate_plot(
    ctx: RunContext[dict],
    card_id: int,
    plot_instructions: str,
) -> str:
    """Generate a plot from Metabase question data with custom plotting instructions.

    This tool creates visualizations by:
    1. Fetching data from a Metabase question
    2. Using AI to generate appropriate plotly code based on plotting instructions
    3. Executing the code safely to create the plot with automatic retry on errors
    4. Saving the plot as an image file

    Args:
        card_id: The ID of the Metabase question containing the data to plot. Typically a table, where only a subset of the columns are needed for plotting.
        plot_instructions: Natural language instructions for the plot (e.g., "create a line chart showing trend over time of num_users", "make a scatter plot with GDP vs population", "generate a bar chart grouped by country")

    Returns:
        str: Success message with file path or error details after all retries exhausted.

    Raises:
        ValueError: If data cannot be fetched or plot generation fails after retries
    """
    max_retries = 3
    model_name = ctx.model.model_name
    question_id = ctx.deps.get("question_id", f"plot_{card_id}")

    # Get data from Metabase
    df = _get_question_data(card_id)
    if df.empty:
        raise ValueError(f"DataFrame is empty for question with ID {card_id}")

    # Prepare context for plotting agent
    sample_data_str = df.head(3).to_string()
    context_info = (
        f"DataFrame info:\n"
        f"- Columns: {df.columns.tolist()}\n"
        f"- Data types: {dict(df.dtypes)}\n"
        f"- Shape: {df.shape}\n"
        f"- Sample data (first 3 rows):\n{sample_data_str}\n\n"
        f"User instructions: {plot_instructions}"
    )

    # Retry loop for code generation and execution
    error_history = []
    for attempt in range(max_retries):
        try:
            log.info(f">>>>>> Generating plot attempt {attempt + 1}/{max_retries} for question {question_id}")

            # Build prompt with error feedback from previous attempts
            if error_history:
                error_feedback = "\n\nPREVIOUS ATTEMPT ERRORS:\n" + "\n".join(
                    f"Attempt {i+1} error: {err}" for i, err in enumerate(error_history)
                )
                prompt = context_info + error_feedback + "\n\nPlease fix the code to avoid these errors."
            else:
                prompt = context_info

            # Generate code using sub-agent
            plotting_result = await plotting_agent.run(
                prompt,
                model=model_name,
                usage=ctx.usage,
            )
            plotting_code = plotting_result.output

            # Clean markdown formatting
            if plotting_code.startswith("```python"):
                plotting_code = plotting_code.split("```python")[1].split("```")[0].strip()
            elif plotting_code.startswith("```"):
                plotting_code = plotting_code.split("```")[1].split("```")[0].strip()

            # Execute the generated code safely
            local_vars = {
                "df": df,
                "px": px,
                "go": go,
                "pd": pd,
            }

            exec(plotting_code, {}, local_vars)

            if "fig" not in local_vars:
                raise ValueError("Generated code did not create a 'fig' variable")

            fig = local_vars["fig"]

            # Save to files
            unique_id = uuid.uuid4().hex[:8]
            filename_base = f"{question_id}_plot_{unique_id}"
            filepath = save_plot_file(fig, filename_base)
            save_code_file(plotting_code, filename_base, question_id, card_id)

            log.info(f"Successfully generated plot for question {question_id}: {filepath}")
            return f"Successfully generated plot: {filepath}"

        except Exception as e:
            error_msg = f"{type(e).__name__}: {str(e)}"
            error_history.append(error_msg)
            log.warning(f"Plot generation attempt {attempt + 1} failed: {error_msg}")

            # If this was the last retry, raise the error
            if attempt == max_retries - 1:
                full_error = "\n".join(f"Attempt {i+1}: {err}" for i, err in enumerate(error_history))
                log.error(f"All {max_retries} attempts failed for question {question_id}:\n{full_error}")
                raise ValueError(f"Failed to generate plot after {max_retries} attempts. Errors:\n{full_error}")

    # This line should never be reached due to the raise in the last retry
    raise ValueError("Unexpected code path: all retries exhausted without success or error")
