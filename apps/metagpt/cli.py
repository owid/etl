import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Literal

import click
import structlog
import yaml
from openai import OpenAI
from rich_click.rich_command import RichCommand
from typing_extensions import Self

from etl.paths import BASE_DIR

# GPT Model
GPT_MODEL = "gpt-3.5-turbo"

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
@click.command(cls=RichCommand)
@click.option("--path-to-file", prompt=True, type=str, help="Path to the metadata file.")
@click.option("--output-dir", default=None, type=str, help="Path to save the new metadata file.")
@click.option(
    "--overwrite",
    default=False,
    is_flag=True,
    help="Overwrite input file if set to True. Otherwise, save the new file in the output directory.",
)
def main(path_to_file: str, output_dir: str, overwrite: bool) -> None:
    """Process and update metadata using GPT-based tool.

    If `path-to-file` must either be a 'snapshot' or a 'grapher' metadata file.
    To learn more about the behaviour of this tool, please refer to https://docs.owid.io/projects/etl/architecture/metadata/.
    """
    log.info("Starting metadata update process.")

    # Determine the output file path
    if overwrite:
        output_file_path = path_to_file
    else:
        if output_dir is None:
            output_dir = os.path.dirname(path_to_file)
        output_file_path = os.path.join(output_dir, "gpt_" + os.path.basename(path_to_file))

    try:
        # Start tool (initialise gpt client)
        gpt_updater = MetadataGPTUpdater(path_to_file)
        # Run update
        gpt_updater.run()
        # Save updated metadata
        gpt_updater.save_updated_metadata(output_file_path)
    except Exception as e:
        log.error("Metadata update process failed.", error=str(e))


class MetadataGPTUpdater:
    """Update metadata file using Chat GPT."""

    def __init__(self: Self, path_to_file: str) -> None:
        """Initialize the metadata updater."""
        # Path to the metadata file
        self.path_to_file: str = path_to_file
        # Will contain the original metadata file content. Access to it via the property `metadata_old_str`
        self.__metadata_old: str | None = None
        # Will contain the new metadata file content. Access to it via the property `metadata_new_str`
        self.__metadata_new: str | None = None
        # Initialize OpenAI client
        self.client = OpenAI()

    @property
    def channel(self: Self) -> Literal["snapshot", "grapher", "garden"]:
        """Get the channel from the metadata file path.

        It is susceptible to errors:
            - What if the path contains more than one of the keywords? e.g. 'grapher/un_snapshots/2023-12-01/file.yaml'
        """
        if "snapshots/" in self.path_to_file:
            return Channels.SNAPSHOT
        elif "garden/" in self.path_to_file:
            return Channels.GARDEN
        elif "grapher/" in self.path_to_file:
            return Channels.GRAPHER
        else:
            raise Exception("Invalid file path")

    @property
    def metadata_new_str(self: Self) -> str:
        """Get new metadata.

        This only works if `run` has been executed first.
        """
        if self.__metadata_new:
            return self.__metadata_new
        else:
            raise Exception("Metadata not generated yet. Please make sure you successfully run `run` first.")

    @property
    def metadata_old_str(self: Self) -> str:
        """Read a metadata file and returns its content."""
        if self.__metadata_old is None:
            self.__metadata_old = _read_metadata_file(self.path_to_file)
        return self.__metadata_old

    def save_updated_metadata(self: Self, output_file: str) -> None:
        """Save the metadata file and returns its content."""
        with open(output_file, "w") as file:
            yaml.dump(self.metadata_new_str, file, default_flow_style=False, sort_keys=False, indent=4)

    def run(self: Self) -> str | None:
        """Update metadata using OpenAI GPT."""
        # Create system prompt
        messages = self.create_system_prompt()
        # Update metadata
        match self.channel:
            case Channels.SNAPSHOT:
                message_content = get_message_content(self.client, messages=messages, model=GPT_MODEL, temperature=0)  # type: ignore
                if message_content:
                    new_yaml_content = yaml.safe_load(message_content)
                    if new_yaml_content:
                        self.__metadata_new = new_yaml_content
            case Channels.GRAPHER:
                for attempt in range(5):  # MAX_ATTEMPTS
                    message_content = get_message_content(
                        self.client, messages=messages, model=GPT_MODEL, temperature=0
                    )  #
                    if message_content:
                        try:
                            # Fix the single quotes
                            json_string_fixed = re.sub(r"(\W)'|'(\W)", r'\1"\2', message_content)
                            parsed_dict = json.loads(json_string_fixed)
                            if check_gpt_response_format(parsed_dict):
                                with open(self.path_to_file, "r") as file:
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
                                                    original_yaml_content["tables"][table]["variables"][
                                                        variable
                                                    ].update(variable_updates)
                                    self.__metadata_new = original_yaml_content
                        except json.JSONDecodeError as e:
                            log.error(f"JSON decoding failed on attempt {attempt + 1}: {e}")
                raise Exception("Unable to parse GPT response after multiple attempts.")

    def create_system_prompt(self: Self) -> List[Dict[str, str]] | None:
        """Create the system prompt for the GPT model based on file path."""
        match self.channel:
            case Channels.SNAPSHOT:
                # Load example of new metadata format
                new_metadata_file = _read_metadata_file(NEW_METADATA_EXAMPLE)
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
                    {"role": "user", "content": self.metadata_old_str},
                ]
                return messages
            case Channels.GRAPHER:
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
                    {"role": "user", "content": self.metadata_old_str},
                ]
                return messages
            case Channels.GARDEN:
                log.error("Prompt for editing metadata in the garden step is not available yet.")
            case _:
                log.error(f"Invalid channel {self.channel}")


def _read_metadata_file(path_to_file: str | Path) -> str:
    """Read a metadata file and returns its content."""
    with open(path_to_file, "r") as file:
        return file.read()


def get_message_content(client, **kwargs):
    """Get message content from the chat completion."""
    chat_completion = client.chat.completions.create(**kwargs)  # type: ignore
    message_content = process_chat_completion(chat_completion)
    return message_content


def process_chat_completion(chat_completion) -> Any | None:
    """Process the chat completion response."""
    if chat_completion is not None:
        chat_completion_tokens = chat_completion.usage.total_tokens
        log.info(f"Cost GPT4: ${chat_completion_tokens/ 1000 * 0.03:.2f}")
        message_content = chat_completion.choices[0].message.content
        return message_content
    return None


def check_gpt_response_format(message_content) -> bool:
    """Process ChatGPT response and checks if it's in the correct format.

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
    for _, table_content in tables.items():
        if not isinstance(table_content, dict) or "variables" not in table_content:
            return False

        variables = table_content["variables"]
        if not isinstance(variables, dict):
            return False

        # Iterate over each variable in the table
        for _, var_content in variables.items():
            if not isinstance(var_content, dict):
                return False

            # Check for 'description_short' and 'description_key'
            if "description_short" not in var_content or "description_key" not in var_content:
                return False

            # Check if 'description_key' is a list
            if not isinstance(var_content["description_key"], list):
                return False

    return True


class Channels:
    """Channels for metadata files.

    Using this to avoid hardcoding strings."""

    SNAPSHOT = "snapshot"
    GRAPHER = "grapher"
    GARDEN = "garden"


if __name__ == "__main__":
    main()
