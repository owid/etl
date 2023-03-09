"""Load a meadow dataset and create a garden dataset.

This step adds a new column: "country" with the value "World", since the data in this dataset
is only for the world."""

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("ozone_hole_area.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("ozone_hole_area")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["ozone_hole_area"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)
    # Add country column (only one entity: "World")
    df["country"] = "World"

    # Create a new table with the processed data.
    tb_garden = Table(df, like=tb_meadow)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden])
    ds_garden.update_metadata(paths.metadata_path)
    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("ozone_hole_area.end")
