"""Load a meadow dataset and create a garden dataset.

This step adds a new column: "country" with the value "World", since the data in this dataset
is only for the world."""

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    paths.log.info("start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("ozone_hole_area")

    # Read table from meadow dataset.
    tb = ds_meadow["ozone_hole_area"].reset_index()

    # Add country column (only one entity: "World")
    tb["country"] = "World"

    # Format
    tb = tb.format(["year", "country"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

    paths.log.info("end")
