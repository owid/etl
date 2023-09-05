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
    ds_garden = cast(Dataset, paths.load_dependency("mie"))

    # Read table from garden dataset.
    tb = ds_garden["mie"]

    #
    # Process data.
    #
    # Rename index column `region` to `country`.
    tb = tb.reset_index().rename(columns={"region": "country"})

    # Remove suffixes in region names
    tb["country"] = tb["country"].str.replace(r" \(.+\)", "", regex=True)

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["year", "country", "hostility_level"]).sort_index()

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
