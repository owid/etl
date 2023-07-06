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
    ds_garden = cast(Dataset, paths.load_dependency("yougov_jobs"))

    # Read table from garden dataset.
    tb = ds_garden["yougov_jobs"]
    # Combine two question columns into one 'country' column for plotting and set a year column
    tb["country"] = tb[tb.columns[0]].astype(str)
    tb.loc[tb[tb.columns[0]].isna(), "country"] = tb.loc[tb[tb.columns[0]].isna(), tb.columns[-2]].astype(str)
    tb["year"] = 2023
    # Drop the two questions columns
    tb.drop(
        [
            "do_you_believe_it_will_increase_or_decrease_each_of_the_following",
            "ai__will_increase_or_decrease_the_number_of_jobs_available",
        ],
        axis=1,
        inplace=True,
    )

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
