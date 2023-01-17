"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("near_surface_temperature.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("near_surface_temperature")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["near_surface_temperature"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)

    #
    # Process data.
    #
    # Create a new table with the processed data.
    tb_garden = Table(df, like=tb_meadow)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = Dataset.create_empty(dest_dir, metadata=ds_meadow.metadata)

    # Add table of processed data to the new dataset.
    ds_garden.add(tb_garden)

    # Update dataset and table metadata using the adjacent yaml file.
    ds_garden.update_metadata(paths.metadata_path)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("near_surface_temperature.end")
