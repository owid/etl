"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("github_stats")

    #
    # Process data.
    #
    tb_contrib = ds_garden.read_table("user_contributions")

    # Add entity
    tb_contrib["country"] = "World"

    #
    # Save outputs.
    #
    tables = [
        tb_contrib.format(["country", "date", "interval"], short_name="user_contributions"),
    ]

    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=tables, check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
