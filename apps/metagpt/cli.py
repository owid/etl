import json
import os
from pathlib import Path
from typing import Any, Dict, List, Literal

import click
import structlog
import yaml
from openai import OpenAI
from owid.catalog import Dataset
from rich_click.rich_command import RichCommand
from typing_extensions import Self

from etl.paths import BASE_DIR

# GPT Model
GPT_MODEL = "gpt-3.5-turbo"
RATE_PER_1000_TOKENS = 0.0015  # Approximate average cost per 1000 tokens from here - https://openai.com/pricing

# Initialize logger
log = structlog.get_logger()

# Example of new metadata format
NEW_METADATA_EXAMPLE = (
    BASE_DIR / "snapshots" / "emissions" / "2023-11-23" / "national_contributions_annual_emissions.csv.dvc"
)
# Docs for garden metadata fields
DOCS = BASE_DIR / "schemas" / "dataset-schema.json"


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

    def estimate_cost(
        self: Self, original_yaml_content, fields_to_fill_out, metadata_indicator_docs, ds_meta_description
    ) -> float:
        if self.channel == Channels.GARDEN:
            total_estimated_cost = 0
            for _, table_data in original_yaml_content["tables"].items():
                for _, variable_data in table_data["variables"].items():
                    variable_title = variable_data["title"]
                    for metadata_field in fields_to_fill_out:
                        metadata_instructions = metadata_indicator_docs[metadata_field]
                        messages = self.create_system_prompt_garden_grapher(
                            variable_title,
                            metadata_field,
                            metadata_instructions,
                            ds_meta_description,
                        )
                        char_count = len(messages[0]["content"])  # type: ignore
                        est_cost = calculate_gpt_cost(char_count)
                        total_estimated_cost += len(original_yaml_content["tables"].items()) * est_cost
            return total_estimated_cost
        else:
            raise Exception("Invalid channel")

    def run(self: Self) -> str | None:
        """Update metadata using OpenAI GPT."""

        # Update metadata
        match self.channel:
            case Channels.SNAPSHOT:
                self.run_snapshot()
            case Channels.GARDEN | Channels.GRAPHER:
                self.run_garden_grapher()
                return

    def run_snapshot(self: Self):
        # Create system prompt
        messages = self.create_system_prompt()
        message_content, cost = get_message_content(self.client, messages=messages, model=GPT_MODEL, temperature=0)  # type: ignore
        log.info(f"Cost GPT4: ${cost:.3f}")

        if message_content:
            new_yaml_content = yaml.safe_load(message_content)
            if new_yaml_content:
                self.__metadata_new = new_yaml_content

    def run_garden_grapher(self):
        # Load the actual dataset file to extract description of the dataset
        # Amend path to be compatiable to work with Dataset class
        parts = self.path_to_file.split("/")
        parts.remove("steps")
        parts.remove("etl")
        parts[-1] = parts[-1].split(".")[0]
        path_to_dataset = "/".join(parts)
        # Check if the garden step file exists
        if os.path.isfile(path_to_dataset + "/index.json"):
            ds = Dataset(path_to_dataset)
            ds_meta_description = ds.metadata.to_dict()
        else:
            log.error(f"Required garden dataset {path_to_dataset} does not exist. Run the garden step first.")
            raise Exception("Required garden dataset does not exist. Run the garden step first.")

        # Open the file with descriptions of metadata fields from our docs
        with open(DOCS, "r") as f:
            docs = json.load(f)

        metadata_indicator_docs = docs["properties"]["tables"]["additionalProperties"]["properties"]["variables"][
            "additionalProperties"
        ]["properties"]
        fields_to_fill_out = [
            "title",
            "unit",
            "short_unit",
            "description_short",
            "description_key",
        ]

        with open(self.path_to_file, "r") as file:
            original_yaml_content = yaml.safe_load(file)

        # Calculate the total estimated cost
        total_estimated_cost = self.estimate_cost(
            original_yaml_content,
            fields_to_fill_out,
            metadata_indicator_docs,
            ds_meta_description,
        )

        # Ask the user if they want to proceed
        proceed = input(f"The total estimated cost is ${total_estimated_cost:.3f}. Do you want to proceed? (yes/no): ")

        all_variables = {}
        final_cost = 0
        if proceed.lower() == "yes":
            for table_name, table_data in original_yaml_content["tables"].items():
                for variable_name, variable_data in table_data["variables"].items():
                    variable_title = variable_data["title"]
                    indicator_metadata = []
                    for metadata_field in fields_to_fill_out:
                        metadata_instructions = metadata_indicator_docs[metadata_field]
                        messages = self.create_system_prompt_garden_grapher(
                            variable_title,
                            metadata_field,
                            metadata_instructions,
                            ds_meta_description,
                        )
                        message_content, cost = get_message_content(
                            self.client, messages=messages, model=GPT_MODEL, temperature=0
                        )
                        final_cost += cost

                        indicator_metadata.append(message_content)
                    all_variables[variable_name] = indicator_metadata
        log.info(f"Cost GPT4: ${final_cost:.3f}")

        for table_name, table_data in original_yaml_content["tables"].items():
            for variable_name, variable_data in table_data["variables"].items():
                variable_updates = all_variables[variable_name]
                variable_updates_dict = convert_list_to_dict(variable_updates)
                if "description_key" in variable_updates:
                    variable_updates["description_key"] = [f"{item}" for item in variable_updates["description_key"]]
                original_yaml_content["tables"][table_name]["variables"][variable_name].update(variable_updates_dict)

        self.__metadata_new = original_yaml_content

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

                In any of the fields please avoid using ":" anywhere - e.g., instead of "Country Activity Tracker: Artificial Intelligence" use "Country Activity Tracker - Artificial Intelligence".

                """
                messages = [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": self.metadata_old_str},
                ]
                return messages
            case _:
                log.error(f"Invalid channel {self.channel}")

    def create_system_prompt_garden_grapher(
        self, variable_title: str, metadata_field: str, metadata_instructions: str, ds_meta_description: str
    ) -> List[Dict[str, str]] | None:
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


def _read_metadata_file(path_to_file: str | Path) -> str:
    """Read a metadata file and returns its content."""
    with open(path_to_file, "r") as file:
        return file.read()


def get_message_content(client, **kwargs):
    """Get message content from the chat completion."""
    chat_completion = client.chat.completions.create(**kwargs)  # type: ignore
    message_content, cost = process_chat_completion(chat_completion)  # type: ignore
    return message_content, cost


def process_chat_completion(chat_completion) -> Any | None:
    """Process the chat completion response."""
    if chat_completion is not None:
        chat_completion_tokens = chat_completion.usage.total_tokens
        cost = (chat_completion_tokens / 1000) * RATE_PER_1000_TOKENS
        message_content = chat_completion.choices[0].message.content
        return message_content, cost
    return None, None


def convert_list_to_dict(data_list):
    """
    Function to convert a list of string elements in the format "'key': 'value'" into a dictionary.
    """
    data_dict = {}
    for item in data_list:
        # Removing leading and trailing single quotes, then splitting the string by ':'
        key, value = item.strip("'").split(": ", 1)
        data_dict[key.strip("'")] = value.strip("'\"")
    return data_dict


def calculate_gpt_cost(char_count):
    """
    Calculate the cost of using GPT based on the number of characters.

    This function estimates the cost of using GPT by converting the number of characters into tokens,
    rounding up to the nearest thousand tokens, and then multiplying by the rate per thousand tokens.

    Args:
        char_count (int): The number of characters in the text to be processed by GPT.

    Returns:
        float: The estimated cost of using GPT for the given number of characters.
    """

    tokens = char_count / 4  # Average size of a token is 4 characters
    tokens_rounded_up = -(-tokens // 1000) * 1000  # Round up to the nearest 1000 tokens
    estimated_cost = (tokens_rounded_up / 1000) * RATE_PER_1000_TOKENS
    return estimated_cost


class Channels:
    """Channels for metadata files.

    Using this to avoid hardcoding strings."""

    SNAPSHOT = "snapshot"
    GRAPHER = "grapher"
    GARDEN = "garden"


if __name__ == "__main__":
    main()
