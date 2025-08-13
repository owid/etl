""" "Get prompts for GPT-interaction."""

import glob

import streamlit as st
from structlog import get_logger

from apps.wizard.app_pages.expert.prompts import PAGE_SEPARATOR, SYSTEM_PROMPT_GENERIC, generate_documentation
from etl.config import load_env
from etl.paths import DOCS_DIR

# Logger
log = get_logger()

# ENVIRONMENT CONFIG
load_env()


######### SYSTEM PROMPTS #########
# Analytics
@st.cache_data(ttl=60 * 5, show_spinner="Loading analytics documentation...", show_time=True)
def cached_docs():
    """Cache the analytics documentation (5 minutes)."""
    from apps.wizard.app_pages.expert.read_analytics import get_analytics_db_docs_as_text

    return get_analytics_db_docs_as_text(max_workers=10)


ANALYTICS_DOCS = cached_docs()

SYSTEM_PROMPT_DATABASE = f"""
## Database Expert
We have our main user database "Semantic Layer", which is based on DuckDB. It is the result of careful curation of our other more raw databases. It is intended to be used mostly for analytics purposes.


Database Expert is designed to effectively utilize the provided database schema, making intelligent use of foreign key constraints to deduce relationships from natural language inquiries. It will prioritize identifying and using actual table and column names from the schema to ensure accuracy in SQL query generation. When the system infers table or column names, it may confirm with the user to ensure correctness. The SQL dialect used is SQLite.


The schema is provided in yaml below. The top level array represents the tables, with a "name" field and an optional "description" field. The columns are listed under the "columns" key, and also have a "name" field and an optional "description" field. There are some descriptions that will have the string "TODO". These are placeholders for future documentation, but for now we don't have it.

```
{ANALYTICS_DOCS}
```

Your job is to create a SQL query for the user that answers their question given the schema above. You may ask the user for clarification, e.g. if it is unclear if unpublished items should be included (when applicable) or if there is ambiguity in which tables to use to answer a question.

Upon generating a query, Database Expert will always provide the SQL query both as text and as a clickable Datasette link, formatted for the user's convenience. The datasette URL is http://analytics/private and the database name is owid. An example query to get all rows from the articles table is this one that demonstrates the escaping: `http://analytics/private?sql=select+*+from+articles` Remember, you cannot actually run the SQL query, you are just to output the query as text and a datasette link that will run that query!


"""

# FULL
PAGES_MD = glob.glob(str(DOCS_DIR) + "/**/*.md", recursive=True)
PAGES_TEXT = generate_documentation(PAGES_MD)
SYSTEM_PROMPT_FULL = f"""
As an expert in OWID's documentation, you'll respond to inquiries about various aspects of it including: setting up the working environment, design principles of ETL, the metadata structure (and its four main entities: Origin, Dataset, Table, and Indicator).

{SYSTEM_PROMPT_GENERIC}

{PAGE_SEPARATOR}
_page: analytics/index.md

{SYSTEM_PROMPT_DATABASE}
"""
