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
    ds_garden = cast(Dataset, paths.load_dependency("population_doubling_times"))

    # Read table from garden dataset.
    tb = ds_garden["population_doubling_times"]

    #
    # Process data.
    #
    tb = tb.reset_index()
    # Change entity
    tb["country"] = (
        tb["population_rounded"].astype(str) + " to " + (tb["population_rounded"] / 2).astype(int).astype(str)
    )
    # Filter columns, rename columns, set index
    tb = tb[["country", "year", "years_since_half_population"]]
    tb = tb.rename(columns={"years_since_half_population": "World"})
    tb = tb.set_index(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb], default_metadata=ds_garden.metadata)
    ds_grapher.metadata.title += "[transitions as entities]"
    #
    # Checks.
    #
    grapher_checks(ds_grapher)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
