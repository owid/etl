"""Load a garden dataset and create a grapher dataset."""

from typing import cast

from owid.catalog import Dataset

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = cast(Dataset, paths.load_dependency("igme"))

    # Read table from garden dataset.
    tb = ds_garden["igme"]
    tb_youth = ds_garden["igme_under_fifteen_mortality"]

    tb = tb.reorder_levels(["country", "year", "unit_of_measure", "indicator", "sex", "wealth_quintile"])
    tb_youth = tb_youth.reorder_levels(["country", "year", "unit_of_measure", "indicator", "sex", "wealth_quintile"])
    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb, tb_youth], default_metadata=ds_garden.metadata)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
