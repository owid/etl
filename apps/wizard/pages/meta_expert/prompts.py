"""Prompts for GPT-interaction.

Also: Generate reduced version of the documentation using chat GPT.

We need to provide some documentation context to the GPT model, via the system prompt. However, the original documentation is too long, especially if using a simple model like 3.5. Therefore, we need to reduce the length of the documentation before passing it to the GPT model. This process is done using GPT-4.

Run this as follows (from python shell):

from apps.wizard.pages.meta_expert.generate_prompt import generate_documentation
generate_documentation()
"""
from structlog import get_logger

from apps.wizard.utils import WIZARD_DIR
from apps.wizard.utils.gpt import GPTQuery, OpenAIWrapper, get_number_tokens
from etl.config import load_env
from etl.docs import render_dataset, render_indicator, render_origin, render_table

# Logger
log = get_logger()

# ENVIRONMENT CONFIG
load_env()


#########################################
# GENERATE REDUCED DOCUMENTATION
#########################################
# Path
DOCS_REDUCED_DIR = WIZARD_DIR / "pages" / "meta_expert" / "docs_reduced"

# GPT CONFIG
# Model name
MODEL_NAME_REDUCED_DEFAULT = "gpt-4-turbo-preview"  # "gpt-4"
# System prompt
SYSTEM_PROMPT = """
- You are a technical expert.
- You are given the documentation of a certain data API and you are asked to make it shorter, more consise and better structured, while not losing any any information.
"""
# User prompt (template)
USER_PROMPT = """
Reduce the token length of the following documentation, without losing any information:

{docs_original}
"""

# LOAD ORIGINAL DOCUMENTATION (markdown)
metadata_original = {
    "dataset": render_dataset(),
    "tables": render_table(),
    "origin": render_origin(),
    "indicators": render_indicator(),
}


def generate_documentation(model_name: str = MODEL_NAME_REDUCED_DEFAULT) -> None:
    """Generate reduced version of the documentation using chat GPT."""
    # Initiate OpenAI
    api = OpenAIWrapper()
    # Generate docs for each category
    for docs_name, docs in metadata_original.items():
        log.info(f"Generating reduced docs for {docs_name} with model {model_name}.")
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": USER_PROMPT.format(docs_original=docs)},
        ]

        # Query ChatGPT
        query = GPTQuery(
            messages=messages,
            temperature=0,
        )

        try:
            response = api.query_gpt(query=query, model=model_name)
        except Exception:
            raise ValueError("Error in GPT query!")
        else:
            if response:
                # Get response text
                text = response.message_content

                tokens_before = get_number_tokens(docs, model_name)
                tokens_after = get_number_tokens(text, model_name)
                path = f"{DOCS_REDUCED_DIR}/{docs_name}.txt"
                log.info(
                    f"Reducing '{docs_name}' documentation from {tokens_before} to {tokens_after} tokens. Cost: {response.cost} USD. Saving it to file {path}"
                )
                with open(f"{path}.original", "w") as f:
                    f.write(docs)
                with open(path, "w") as f:
                    f.write(text)
            else:
                raise ValueError("Error in GPT query!")


#########################################
# OTHER PROMPTS
#########################################
# Prompt using original documentation
SYSTEM_PROMPT_FULL = f"""
As an expert in OWID's metadata structure, you'll respond to inquiries about its structure, comprising four main entities: Origin, Dataset, Table, and Indicator (Variable). Datasets group together Tables, which are akin to pandas DataFrames but include extra metadata, and Tables feature Indicators as columns. Indicators may be linked to multiple Origins, identifying the data's sources. Detailed explanations of each entity follow, separated by '------'.


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
"""


# Prompt using reduced documentation
def render_docs_reduced(entity_name):
    with open(DOCS_REDUCED_DIR / f"{entity_name}.txt", "r") as f:
        return f.read()


SYSTEM_PROMPT_REDUCED = f"""
You are a technical expert at Our World in Data. Your expertise is in the metadata structure of the ETL pipeline. You'll respond to inquiries about ETL's metadata structure.

You should answer in the context of OWID's metadata structure, which is explained below.


There are four main entities: Origin, Dataset, Table, and Indicator (Variable). Datasets group together Tables, which are akin to pandas DataFrames but include extra metadata, and Tables feature Indicators as columns. Indicators may be linked to multiple Origins, identifying the data's sources. Detailed explanations of each entity follow, separated by '------'.


# Datasets:
{render_docs_reduced('dataset')}

------
# Tables:
{render_docs_reduced('tables')}

------
# Indicators:
{render_docs_reduced('indicators')}

------
# Origins:
{render_docs_reduced('origin')}

"""
