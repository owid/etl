"""Prompts for chat GPT.

Contains instructions to correctly query chat GPT for the different use cases (snapshot, garden, grapher, etc.). This includes details on what each metadata field means.
"""
import json
from typing import Any, Dict, List

from apps.metagpt.utils import read_metadata_file
from apps.wizard.utils.gpt import GPTQuery
from etl.paths import BASE_DIR

# Example of new metadata format
NEW_METADATA_EXAMPLE = (
    BASE_DIR / "snapshots" / "emissions" / "2023-11-23" / "national_contributions_annual_emissions.csv.dvc"
)

# Additional instructions or configurations
ADDITIONAL_INSTRUCTIONS = """
Metadata Field Guidelines:

1. attribution (string, optional)
   - Capital letter start, except names like 'van Haasteren'.
   - No ending period.
   - Ends with year of date_published in parenthesis.
   - No semicolons.

2. attribution_short (string, recommended)
   - Use if producer name is long; shorten the producer name in an informative way.
   - Capital letter start, except names like 'van Haasteren'.
   - No ending period.
   - Refers to producer or well-known data product, not year.
   - Use acronym if well-known.

3. citation_full (string, required)
   - Capital letter start.
   - Ends with period.
   - Includes year of publication.
   - Match producer's format with minor edits.
   - List multiple sources for compilations.

4. date_accessed (string, required)
   - Format: YYYY-MM-DD.
   - Reflects access date of current version.

5. date_published (string, required)
   - Format: YYYY-MM-DD or YYYY.
   - Reflects publication date of current version.

6. description (string, recommended)
   - Capital letter start and period end.
   - Avoid other metadata fields unless crucial.
   - Succinct description of data product.

7. description_snapshot (string, recommended)
   - Capital letter start and period end.
   - Define if data product and snapshot differ.
   - Describe snapshot specifically.

8. license (string, required)
   - Standard license names or producer's custom text.
   - CC BY 4.0 if unspecified, pending confirmation.

9. license.url (string, required if existing)
   - Complete URL to license on producer's site.
   - Avoid generic license pages.

10. producer (string, required)
    - Capital letter start, except names like 'van Haasteren'.
    - No ending period, except 'et al.'.
    - Exclude dates, semicolons, OWID references.

11. title (string, required)
    - Capital letter start.
    - No ending period.
    - Identify data product, not snapshot.

12. title_snapshot (string, required if different)
    - Capital letter start.
    - No ending period.
    - Use if snapshot differs from data product.

13. url_download (string, required if existing)
    - Direct download URL or S3 URI.

14. url_main (string, required)
    - URL to data product's main site.

15. version_producer (string or number, recommended if existing)
    - Use producer's version naming.
"""

# Docs for garden metadata fields
DOCS = BASE_DIR / "schemas" / "dataset-schema.json"
with open(DOCS, "r") as f:
    docs = json.load(f)
DOCS_METADATA_INDICATORS = docs["properties"]["tables"]["additionalProperties"]["properties"]["variables"][
    "additionalProperties"
]["properties"]


def create_system_prompt_snapshot(metadata_old_str: str) -> List[Dict[str, str]]:
    """Create the system prompt for the GPT model based on file path."""
    # Load example of new metadata format
    new_metadata_file = read_metadata_file(NEW_METADATA_EXAMPLE)
    system_prompt = f"""
    You are given an old metadata file with information about the sources of the data in the old format. Now, we've transitioned to a new format.

    The new metadata file needs to be structured in an identical way. Infer the fields and arrange them in the same order as in the new metadata file. Update the old metadata file to the new format based on this example.

    Please format your responses (e.g., year shouldn't exist in producer field etc) and add any additional fields if possible/necessary based on these additional instructions:
    {ADDITIONAL_INSTRUCTIONS}

    The new metadata file is as follows. Structure your response in the same way:
    {new_metadata_file}

    Please output it in the same format (yaml).License" is a part of "origin", not a separate dictionary. Don't include any additional responses/notes in your response beyond the existing field as this will be saved directly as a file.

    In any of the fields please avoid using ":" anywhere - e.g., instead of "Country Activity Tracker: Artificial Intelligence" use "Country Activity Tracker - Artificial Intelligence".

    """
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": metadata_old_str},
    ]
    return messages


def create_query_snapshot(metadata_old_str: str) -> GPTQuery:
    """Build GPT query."""
    messages = create_system_prompt_snapshot(metadata_old_str)
    # Build query for GPT
    query = GPTQuery(
        messages=messages,
        temperature=0,
    )

    return query


def create_system_prompt_data_step(
    variable_title: str, metadata_field: str, ds_meta_description: Dict[str, Any]
) -> List[Dict[str, str]]:
    """
    Generates a system prompt for a gardening application.

    Parameters:
    variable_title (str): The title of the variable in the dataset.
    metadata_field (str): The metadata field related to the variable.
    metadata_instructions (str): Instructions for filling out the metadata field.
    ds_meta_description (str): Description of the dataset.

    Returns:
    Optional[List[Dict[str, str]]]: A list of dictionaries containing the system prompt, or None.
    """
    metadata_instructions = DOCS_METADATA_INDICATORS[metadata_field]["description"]
    base_template_instructions = (
        "You are given a description of the dataset here:\n"
        f"'{ds_meta_description}'\n\n"
        f"We have a variable called '{variable_title}' in this dataset.\n"
        "By using the information you already have in the dataset description, and browsing the web, can you "
        f"infer what this metadata field '{metadata_field}' might be for this specific indicator?\n\n"
        "Depending on which field you are filling out take into account these extra instructions:\n"
        " - description_key - based on a web search and your knowledge come up with some key bullet points (in a sentence format) that would help someone interpret the indicator. Can you make sure that these are going to be useful for the public to understand the indicator? Expand on any acronyms or any terms that a layperson might not be familiar with. Each bullet point can be more than one sentence if necessary but don't make it too long.\n"
        " - description_short use the description_key and a web search to come up with one sentence to describe the indicator. It should be very brief and to the point.\n"
        f"Here are more specific instructions on how to fill out the field - '{metadata_instructions}'.\n"
        "Now, can you try to infer the above based on the other information in the metadata file and by browsing the web? You can use any links in the metadata file to help you."
    )

    base_template_prompt = (
        "Output the filled out field in the following format. Make sure your responses make sense:\n"
        f"'{metadata_field}': Your suggestion for how it should be filled out."
    )

    messages = [{"role": "system", "content": base_template_instructions + base_template_prompt}]
    return messages


def create_query_data_step(variable_title: str, metadata_field: str, ds_meta_description: Dict[str, Any]) -> GPTQuery:
    """Build GPT query."""
    messages = create_system_prompt_data_step(
        variable_title,
        metadata_field,
        ds_meta_description,
    )
    # Build query for GPT
    query = GPTQuery(
        messages=messages,
        temperature=0,
    )

    return query
