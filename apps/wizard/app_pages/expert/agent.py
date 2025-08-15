import streamlit as st
from pydantic_ai import Agent


@st.cache_data(show_spinner="Loading analytics documentation...", show_time=True)
def cached_docs():
    """Cache the analytics documentation (5 minutes)."""
    from apps.wizard.app_pages.expert.read_analytics import get_analytics_db_docs

    docs = get_analytics_db_docs(max_workers=10)

    summary = []
    table_summary = {}
    for doc in docs:
        # Main summary
        summary.append(f"{doc['name']}: {doc['description']}")
        # Per-table summary
        s = [f"{col['name']}: {col["description"]}" for col in docs[0]["columns"]]
        table_summary[doc["name"]] = "\n".join(s)
    summary = "\n".join(summary)

    return summary, table_summary


SUMMARY, TABLE_SUMMARY = cached_docs()

SYSTEM_PROMPT_DATABASE = """
## Context
We have our main user database "Semantic Layer", which is based on DuckDB. It is the result of careful curation of our other more raw databases. It is intended to be used mostly for analytics purposes.

Various users don't have SQL expertise, but still want to get their questions answered. That's where "Database Expert" comes in. It is an expert that can understand these user's questions, generate SQL queries to answer them and link them to relevant Datasette results.

## Your job as "Database Expert"
Utilize the database documentation, making intelligent use of foreign key constraints to deduce relationships from natural language inquiries. You will prioritize identifying and using actual table and column names from the schema to ensure accuracy in SQL query generation. When the system infers table or column names, it may confirm with the user to ensure correctness. The SQL dialect used is SQLite.

To get details on the tables available in the database, you can use the `get_tables` tool. To get details on the columns of a specific table, you can use the `get_table_fields` tool.

Your job is to create a SQL query for the user that answers their question given the schema above. You may ask the user for clarification, e.g. if it is unclear if unpublished items should be included (when applicable) or if there is ambiguity in which tables to use to answer a question.

## Response format
Start with a brief comment on the user's question, but avoid being overly verbose.

Then, upon generating a query, always provide the SQL query both as text and as a clickable Datasette link, formatted for the user's convenience:

  - SQL: Provide the SQL query in a code block (i.e. make use of '```sql...```').
  - Datasette link: The datasette URL is http://analytics/analytics and the database name is owid. An example query to get all rows from the articles table is this one that demonstrates the escaping: `http://analytics/analytics?sql=select+*+from+articles`. Remember, you cannot actually run the SQL query, you are just to output the query as text and a datasette link that will run that query! Put the link nicely, with the link text "Run this query in Datasette". Also, if the SQL query contains '+', replace it with '%2B' to ensure the URL is correctly formatted.
"""

# Pydantic AI Agent
agent = Agent(
    system_prompt=SYSTEM_PROMPT_DATABASE,
    retries=2,
)


@agent.tool_plain
async def get_tables() -> str:
    """
    Get an overview of the database tables. Retrieve the names of the tables in the database, along with their short descriptions.

    Returns:
        str: Table short descriptions in format "table1: ...\ntable2: ...".
    """
    st.toast("Tool use: get_tables", icon=":material/robot:")
    return SUMMARY


@agent.tool_plain
async def get_table_fields(tb_name: str) -> str:
    """
    Retrieve the documentation of the columns of database table "tb_name".

    Args:
        tb_name: Name of the table

    Returns:
        str: Table documentation as string, mapping column names to their descriptions. E.g. "column1: description1\ncolumn2: description2".
    """
    st.toast(f"Tool use: `get_table_fields`, table {tb_name}", icon=":material/robot:")
    if tb_name not in TABLE_SUMMARY:
        print("Table not found:", tb_name)
        return ""
    return TABLE_SUMMARY[tb_name]
