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
    ds_garden = cast(Dataset, paths.load_dependency("guinea_worm"))

    # Read table from garden dataset.
    tb = ds_garden["guinea_worm"]
    tb["year_certified"] = tb["year_certified"].replace({"Pre-certification": 3000, "Endemic": 4000})
    tb["year_certified"] = tb["year_certified"].astype("float64")
    tb["guinea_worm_reported_cases"] = tb["guinea_worm_reported_cases"].astype("float64")
    tb = tb.sort_values(["country", "year"])
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
