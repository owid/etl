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
    ds_garden = cast(Dataset, paths.load_dependency("papers_with_code_benchmarks_state_of_the_art"))

    # Read table from garden dataset.
    tb = ds_garden["papers_with_code_benchmarks_state_of_the_art"]
    tb.reset_index(inplace=True)

    #
    # Process data.
    #
    tb.rename(columns={"days_since": "year"}, inplace=True)
    tb["country"] = "State of the Art"
    tb["year"] = tb["year"].astype(int)

    tb.set_index(["year", "country"], inplace=True)

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
