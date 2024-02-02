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
    ds_garden = cast(Dataset, paths.load_dependency("yougov_end_of_humanity"))

    # Read table from garden dataset.
    tb = ds_garden["yougov_end_of_humanity"]
    tb.reset_index(inplace=True)

    # Select only rows with "Very concerned" responses
    tb = tb[tb["options"] == "Very concerned"]
    # Drop the options (where response categories are) column as it's not needed
    tb.drop("options", axis=1, inplace=True)
    # Set year and rename 'age_group' to 'country' for plotting in grapher
    tb["year"] = 2023
    tb.rename(columns={"age_group": "country"}, inplace=True)
    tb.reset_index(drop=True, inplace=True)

    tb.set_index(["country", "year"], inplace=True)
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
