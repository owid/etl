"""Clientn module."""
import os
from typing import Any, Dict, Literal

import click
import structlog
import yaml
from owid.catalog import Dataset
from rich_click.rich_command import RichCommand
from typing_extensions import Self

from apps.metagpt.prompts import create_query_data_step, create_query_snapshot
from apps.metagpt.utils import Channels, convert_list_to_dict, read_metadata_file
from apps.wizard.utils.gpt import GPTResponse, OpenAIWrapper
from etl.files import yaml_dump

# Fields to ask GPT for (garden, grapher)
FIELDS_TO_FILL_OUT = [
    "title",
    "unit",
    "short_unit",
    "description_short",
    "description_key",
]

# Initialize logger
log = structlog.get_logger()


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
        if gpt_updater.channel in [Channels.GARDEN, Channels.GRAPHER]:
            # Calculate the total estimated cost
            cost_estimated = gpt_updater.run(lazy=True)
            # Ask the user if they want to proceed
            proceed = input(f"The total estimated cost is ${cost_estimated:.3f}. Do you want to proceed? (yes/no): ")
            if proceed.lower() == "yes":
                # Actually run
                final_cost = gpt_updater.run()
                log.info(f"Cost GPT4: ${final_cost:.3f}")
        else:
            final_cost = gpt_updater.run()
            log.info(f"Cost GPT4: ${final_cost:.3f}")
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
            self.__metadata_old = read_metadata_file(self.path_to_file)
        return self.__metadata_old

    @property
    def path_to_dataset(self: Self) -> str:
        """Path to the dataset file based on the path to the metadata file."""
        parts = self.path_to_file.split("/")
        parts.remove("steps")
        parts.remove("etl")
        parts[-1] = parts[-1].split(".")[0]
        return "/".join(parts)

    def load_dataset(self: Self) -> Dataset:
        """Load dataset.

        Only works when channel is 'garden' or 'grapher'.
        """
        if self.channel not in [Channels.GARDEN, Channels.GRAPHER]:
            raise Exception(f"Invalid channel. Should either be '{Channels.GARDEN}' or '{Channels.GRAPHER}'.")
        # Check if the garden step file exists
        if os.path.isfile(self.path_to_dataset + "/index.json"):
            ds = Dataset(self.path_to_dataset)
            # ds_meta_description = ds.metadata.to_dict()
            return ds
        else:
            log.error(f"Required garden dataset {self.path_to_dataset} does not exist. Run the garden step first.")
            raise Exception("Required garden dataset does not exist. Run the garden step first.")

    def load_yaml_metadata(self: Self) -> Dict[str, Any]:
        """Load dataset metadata from YAML."""
        with open(self.path_to_file, "r") as file:
            original_yaml_content = yaml.safe_load(file)
        return original_yaml_content

    def save_updated_metadata(self: Self, output_file: str) -> None:
        """Save the metadata file and returns its content."""
        with open(output_file, "w") as file:
            yaml_dump(self.metadata_new_str, file, width=float("inf"))  # type: ignore
        log.info(f"Metadata file saved to {output_file}")

    def run(self: Self, lazy: bool = False) -> float | None:
        """Update metadata using OpenAI GPT."""
        # Update metadata
        match self.channel:
            case Channels.SNAPSHOT:
                if lazy:
                    log.info("Running snapshot in lazy mode is not implemented. Nothing was executed.")
                else:
                    return self.run_snapshot()
            case Channels.GARDEN | Channels.GRAPHER:
                return self.run_data_step(lazy)
            case _:
                raise Exception(
                    f"Invalid channel. Should either be '{Channels.SNAPSHOT}', '{Channels.GARDEN}' or '{Channels.GRAPHER}'."
                )

    def run_snapshot(self: Self) -> float | None:
        """Run main code for snapshot."""
        # Create system prompt
        query = create_query_snapshot(self.metadata_old_str)
        response = self.client.query_gpt(query=query)

        if isinstance(response, GPTResponse):
            self.__metadata_new = response.message_content_as_dict  # type: ignore
            return response.cost

    def run_data_step(self: Self, lazy: bool = False) -> float:
        """Actually run the data step.

        1. Load necessary data: dataset metadata, YAML metadata.
        2. iterate over all tables, indicators and indicators' fields.
        3. for each (table, indicator, field), build a GPT query and
            a. if lazy mode, just estimate the cost of the overall query.
            b. otherwise, query GPT, update the metadata and get the real cost (a posteriori).

        Returns:
            float: the cost of the query (in usd). if lazy mode is on, the cost is estimated (no query has been performed). otherwise, the cost is real (query has been performed).
        """
        ## Load dataset
        ds = self.load_dataset()
        ds_meta_description = ds.metadata.to_dict()

        ## Load metadata yaml file
        original_yaml_content = self.load_yaml_metadata()

        cost = 0
        # Iterate over all tables
        for table_name, table_data in original_yaml_content["tables"].items():
            # Iterate over all indicators
            for variable_name, variable_data in table_data["variables"].items():
                variable_title = variable_data["title"]
                indicator_metadata = []
                # Iterate over all indicator fields
                for metadata_field in FIELDS_TO_FILL_OUT:
                    # Build query for GPT
                    query = create_query_data_step(
                        variable_title,
                        metadata_field,
                        ds_meta_description,
                    )
                    # Query GPT (or just estimate cost if lazy mode is on)
                    if lazy:
                        est_cost = query.estimated_cost
                        cost += len(original_yaml_content["tables"].items()) * est_cost
                    else:
                        result = self.client.query_gpt(query=query)
                        # Act based on reply (only if valid)
                        if result:
                            cost += result.cost
                            indicator_metadata.append(result.message_content)
                # Update indicator metadata (when lazy mode is OFF)
                if not lazy:
                    indicator_metadata_dict = convert_list_to_dict(indicator_metadata)

                    # Format description_key to be bullet points
                    indicator_metadata_dict["description_key"] = [  # type: ignore
                        f"{item}" for item in indicator_metadata_dict["description_key"].split(". ")
                    ]

                    original_yaml_content["tables"][table_name]["variables"][variable_name].update(
                        indicator_metadata_dict
                    )
        # Update metadata (when lazy mode is OFF)
        if not lazy:
            # Update metadata
            self.__metadata_new = original_yaml_content  # type: ignore
        return cost


if __name__ == "__main__":
    main()
