"""Load a garden dataset and create a grapher dataset."""

from typing import cast

from owid.catalog import Dataset

from etl.helpers import PathFinder, create_dataset, grapher_checks

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = cast(Dataset, paths.load_dependency("yougov_robots"))

    # Read table from garden dataset.
    tb = ds_garden["yougov_robots"]
    tb.reset_index(inplace=True)
    #
    # Process data.
    #
    # Rename the 'question' column to 'country' and days_since_2021 to year for visualising in the grapher
    tb.rename(columns={"days_since_2021": "year", "group": "country"}, inplace=True)
    tb.set_index(["year", "country"], inplace=True)

    #
    # Process data.
    #

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb], default_metadata=ds_garden.metadata)

    #
    # Checks.
    #
    grapher_checks(ds_grapher)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
