import json
import os
from typing import Literal

import click
import structlog
import yaml
from owid.catalog import Dataset
from rich_click.rich_command import RichCommand
from typing_extensions import Self

from apps.metagpt.prompts import create_system_prompt_data_step, create_system_prompt_snapshot
from apps.metagpt.utils import Channels, OpenAIWrapper, _read_metadata_file
from etl.paths import BASE_DIR

# GPT Model
RATE_PER_1000_TOKENS = 0.0015  # Approximate average cost per 1000 tokens from here - https://openai.com/pricing

# Initialize logger
log = structlog.get_logger()

# Docs for garden metadata fields
DOCS = BASE_DIR / "schemas" / "dataset-schema.json"


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
        self.client = OpenAIWrapper()

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
                        messages = create_system_prompt_data_step(
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
                self.run_data_step()
                return

    def run_snapshot(self: Self):
        # Create system prompt
        messages = create_system_prompt_snapshot(self.metadata_old_str)
        gpt_result = self.client.query_gpt(
            messages=messages,
            temperature=0,
        )

        if gpt_result:
            new_yaml_content = yaml.safe_load(gpt_result.message_content)
            if new_yaml_content:
                self.__metadata_new = new_yaml_content

    def run_data_step(self):
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
                        messages = self.create_system_prompt_data_step(
                            variable_title,
                            metadata_field,
                            metadata_instructions,
                            ds_meta_description,
                        )
                        gpt_result = self.client.query_gpt(
                            messages=messages,
                            temperature=0,
                        )
                        # Act based on reply (only if valid)
                        if gpt_result is not None:
                            final_cost += gpt_result.cost
                            indicator_metadata.append(gpt_result.message_content)
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


if __name__ == "__main__":
    main()
