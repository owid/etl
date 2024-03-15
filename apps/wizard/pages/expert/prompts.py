""""Get prompts for GPT-interaction."""
import glob
from typing import List

from structlog import get_logger

from etl.config import load_env
from etl.docs import render_dataset, render_grapher_config, render_indicator, render_origin, render_table
from etl.paths import DOCS_DIR

# Logger
log = get_logger()

# ENVIRONMENT CONFIG
load_env()


# SEPARATOR between documentation pages.
PAGE_SEPARATOR = "--=+=+--"


def generate_documentation(pages_md: List[str]) -> str:
    """Get prompt-friendly documentation for a given list of pages."""

    def read_page_md(page_path: str) -> str:
        """Read text from MD page, add header with page path."""
        with open(page_path, "r") as f:
            text = f.read()
        text = f"_page: {page_path}_\n\n" + text
        return text

    PAGES_TEXT = [read_page_md(page_path) for page_path in pages_md]
    PAGES_TEXT = PAGE_SEPARATOR + "\n\n" + ("\n" + PAGE_SEPARATOR + "\n\n").join(PAGES_TEXT)
    return PAGES_TEXT


######### SYSTEM PROMPTS #########
NOTE = """To help you with this task, find below the required ETL documentation. Each documentation page is separated by '{PAGE_SEPARATOR}', followed by the path to the page "_page: <page_path>_". The documentation content is given as markdown text (suitable for mkdocs).
"""
# ONLY METADATA
METADATA_BIT = f"""
# Datasets:
{render_dataset()}

------
# Tables:
{render_table()}

------
# Indicators:
{render_indicator()}

------
# Origins:
{render_origin()}

------
#### `variable.presentation.grapher_config`
{render_grapher_config()}
"""
SYSTEM_PROMPT_METADATA = f"""
As an expert in OWID's metadata structure, you'll respond to inquiries about its structure, comprising four main entities: Origin, Dataset, Table, and Indicator (Variable). Datasets group together Tables, which are akin to pandas DataFrames but include extra metadata, and Tables feature Indicators as columns. Indicators may be linked to multiple Origins, identifying the data's sources. Detailed explanations of each entity follow, separated by '------'.


{METADATA_BIT}
"""

"contributing.md"

# GETTING STARTED
PAGES_MD = glob.glob(str(DOCS_DIR) + "/getting-started/**/*.md", recursive=True) + glob.glob(str(DOCS_DIR) + "/*.md")
PAGES_TEXT = generate_documentation(PAGES_MD)
SYSTEM_PROMPT_START = f"""
As an expert in OWID's documentation, you'll respond to inquiries about some of its content.

{NOTE}

{PAGES_TEXT}
"""


# GUIDES
PAGES_MD = (
    glob.glob(str(DOCS_DIR) + "/guides/**/*.md", recursive=True)
    + glob.glob(str(DOCS_DIR) + "/architecture/metadata/**/*.md", recursive=True)
    + glob.glob(str(DOCS_DIR) + "/api/**/*.md", recursive=True)
)
PAGES_TEXT = generate_documentation(PAGES_MD)
SYSTEM_PROMPT_GUIDES = f"""
As an expert in OWID's documentation, you'll respond to inquiries about some of its content. In particular questions on how to use, tools, APIs, and guides.

{NOTE}

{PAGES_TEXT}
"""

# DESIGN PRINCIPLES
PAGES_MD = glob.glob(str(DOCS_DIR) + "/architecture/**/*.md", recursive=True)
PAGES_MD = [p for p in PAGES_MD if "metadata/reference" not in p]
PAGES_TEXT = generate_documentation(PAGES_MD)
SYSTEM_PROMPT_PRINCIPLES = f"""
As an expert in OWID's documentation, you'll respond to inquiries about some of its content. In particular, on the theoretical framework of it (i.e. the design principles).

{NOTE}

{PAGES_TEXT}
"""

# FULL
PAGES_MD = glob.glob(str(DOCS_DIR) + "/**/*.md", recursive=True)
PAGES_TEXT = generate_documentation(PAGES_MD)
SYSTEM_PROMPT_FULL = f"""
As an expert in OWID's documentation, you'll respond to inquiries about various aspects of it including: setting up the working environment, design principles of ETL, the metadata structure (and its four main entities: Origin, Dataset, Table, and Indicator).

{NOTE}

{PAGES_TEXT}

{PAGE_SEPARATOR}
page: architecture/metadata/reference/index.md

{METADATA_BIT}
"""
