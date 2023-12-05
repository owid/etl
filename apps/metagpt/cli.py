import json
import os
import re

import click
import structlog
import yaml
from openai import OpenAI

from etl.paths import BASE_DIR

# Initialize logger
log = structlog.get_logger()

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


# Main CLI command setup with Click
@click.command()
@click.option("--path-to-file", prompt=True, type=str, help="Path to the metadata file.")
@click.option("--output-dir", default=None, type=str, help="Path to save the new metadata file.")
@click.option(
    "--overwrite",
    default=False,
    is_flag=True,
    help="Overwrite input file if set to True. Otherwise, save the new file in the output directory.",
)
def main(path_to_file, output_dir: str, overwrite: bool):
    """Process and update metadata using GPT-based tool."""
    log.info("Starting metadata update process.")

    # Determine the output file path
    if overwrite:
        output_file_path = path_to_file
    else:
        if output_dir is None:
            output_dir = os.path.dirname(path_to_file)
        output_file_path = os.path.join(output_dir, "gpt_" + os.path.basename(path_to_file))

    try:
        metadata = read_metadata_file(path_to_file)
        generate_metadata_update(path_to_file, metadata, output_file_path)
    except Exception as e:
        log.error("Metadata update process failed.", error=str(e))


def read_metadata_file(path_to_file):
    """Reads a metadata file and returns its content."""
    with open(path_to_file, "r") as file:
        return file.read()


def process_chat_completion(chat_completion):
    """Processes the chat completion response."""
    if chat_completion is not None:
        chat_completion_tokens = chat_completion.usage.total_tokens
        log.info(f"Cost GPT4: ${chat_completion_tokens/ 1000 * 0.03:.2f}")
        message_content = chat_completion.choices[0].message.content
        return message_content
    return None


def generate_metadata_update(path_to_file: str, metadata: str, output_file_path: str):
    """Generates updated metadata using OpenAI GPT."""
    client = OpenAI()
    messages = create_system_prompt(path_to_file, metadata)

    if "snap" in path_to_file:
        chat_completion = client.chat.completions.create(messages=messages, model="gpt-4", temperature=0)
        message_content = process_chat_completion(chat_completion)
        if message_content:
            new_yaml_content = yaml.safe_load(message_content)
            if new_yaml_content:
                with open(output_file_path, "w") as file:
                    yaml.dump(new_yaml_content, file, default_flow_style=False, sort_keys=False, indent=4)
                    log.info("Metadata update for 'snap' completed successfully.")

    elif "grapher" in path_to_file:
        for attempt in range(5):  # MAX_ATTEMPTS
            chat_completion = client.chat.completions.create(messages=messages, model="gpt-4", temperature=0)
            message_content = process_chat_completion(chat_completion)
            if message_content:
                try:
                    # Fix the single quotes
                    json_string_fixed = re.sub(r"(\W)'|'(\W)", r'\1"\2', message_content)
                    parsed_dict = json.loads(json_string_fixed)
                    if check_gpt_response_format(parsed_dict):
                        with open(path_to_file, "r") as file:
                            original_yaml_content = yaml.safe_load(file)
                            if original_yaml_content:
                                for table, table_data in parsed_dict["tables"].items():
                                    for variable, variable_updates in table_data["variables"].items():
                                        if (
                                            table in original_yaml_content["tables"]
                                            and variable in original_yaml_content["tables"][table]["variables"]
                                        ):
                                            # Formatting 'description_key' as bullet points
                                            # If 'description_key' is in variable_updates, keep it as a list of strings
                                            if "description_key" in variable_updates:
                                                variable_updates["description_key"] = [
                                                    f"{item}" for item in variable_updates["description_key"]
                                                ]
                                            original_yaml_content["tables"][table]["variables"][variable].update(
                                                variable_updates
                                            )
                            with open(output_file_path, "w") as file:
                                yaml.dump(original_yaml_content, file, default_flow_style=False, sort_keys=False)
                                log.info("Metadata update for 'grapher' completed successfully.")
                                return
                except json.JSONDecodeError as e:
                    log.error(f"JSON decoding failed on attempt {attempt + 1}: {e}")
        raise Exception("Unable to parse GPT response after multiple attempts.")


def create_system_prompt(path_to_file, metadata: str):
    """Creates the system prompt for the GPT model based on file path."""

    if "snapshot" in path_to_file:
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

        """
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": metadata},
        ]
        return messages

    elif "grapher" in path_to_file:
        system_prompt = """
            You are given a metadata file. Could you help us fill out the following for each variable:

            - description_from_producer - do a web search based on other information and URLs in the metadata file to find a description of the variable.
            - description_key - based on a web search and if description exists come up with some key bullet points (in a sentence format) that would help someone interpret the indicator. Can you make sure that these are going to be useful for the public to understand the indicator? Expand on any acronyms or any terms that a layperson might not be familiar with. Each bullet point can be more than one sentence if necessary but don't make it too long.
            - if description_short is not filled out, use the description_key and a web search to come up with one sentence to describe the indicator. It should be very brief and to the point.

            The output should always have these fields but as a python dictionary. This format is mandatory, don't miss fields and don't include any irrelevant information but make it JSON readable.:

            {'tables': {'maddison_gdp': {'variables': {'gdp_per_capita': {'description_short': '...', 'description_from_producer': '...', 'description_key': ['...', '...']}}}}}


            Don't include any other fields. This is mandatory.

            Now, can you try to infer the above based on the other information in the metadata file and by browsing the web?

            You can use any links in the metadata file to help you.


            Don't include any other information so that I can easily access these fields. If you can't fill these out for some indicators, please fill them out with 'ChatGPT could not infer'.

            e.g. never include a starting sentence like 'Based on the metadata file and a web search, here is the inferred information'. Don't include `json` or `yaml` at the start of your response. Just include the fields you've changed.

            'description_key' should be a list of bullet points. Each bullet point should be a string. e.g. ['bullet point 1', 'bullet point 2']

            If information is already filled out, just try to improve it. .
            """

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": metadata},
        ]
        return messages

    elif "garden" in path_to_file:
        log.error("Prompt for editing metadata in the garden step is not available yet.", file_path=path_to_file)

    else:
        log.error("Invalid file path", file_path=path_to_file)


def check_gpt_response_format(message_content):
    """
    Processes ChatGPT response and checks if it's in the correct format.

    :param data: The data to be processed.
    :return: A message indicating whether the data is correctly formatted.
    """

    # Check if the top-level structure is a dictionary with the key 'tables'
    if not isinstance(message_content, dict) or "tables" not in message_content:
        return False

    tables = message_content["tables"]
    if not isinstance(tables, dict):
        return False

    # Iterate over each table
    for table_name, table_content in tables.items():
        if not isinstance(table_content, dict) or "variables" not in table_content:
            return False

        variables = table_content["variables"]
        if not isinstance(variables, dict):
            return False

        # Iterate over each variable in the table
        for var_name, var_content in variables.items():
            if not isinstance(var_content, dict):
                return False

            # Check for 'description_short' and 'description_key'
            if "description_short" not in var_content or "description_key" not in var_content:
                return False

            # Check if 'description_key' is a list
            if not isinstance(var_content["description_key"], list):
                return False

    return True


if __name__ == "__main__":
    main()
