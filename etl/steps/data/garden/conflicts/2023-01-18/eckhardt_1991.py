"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Dataset
from shared import clean_data as _clean_data
from shared import make_tables
from structlog import get_logger

from etl.helpers import PathFinder

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("eckhardt_1991: starting")

    #
    # Load inputs.
    #
    log.info("eckhardt_1991: loading inputs")
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("eckhardt_1991")
    # Read table from meadow dataset.
    tb_meadow = ds_meadow["eckhardt_1991"]
    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)

    #
    # Process data.
    #
    log.info("eckhardt_1991: processing dataframe")
    df = clean_data(df)

    # Create a new table with the processed data.
    log.info("eckhardt_1991: generating tables")
    tables = make_tables(df, paths.short_name)

    #
    # Save outputs.
    #
    log.info("eckhardt_1991: adding tables to dataset")
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = Dataset.create_empty(dest_dir, metadata=ds_meadow.metadata)
    # Add table of processed data to the new dataset.
    ds_garden.add(tables["main"])
    ds_garden.add(tables["notes"])
    ds_garden.add(tables["bulk_id"])
    # Update dataset and table metadata using the adjacent yaml file.
    ds_garden.update_metadata(paths.metadata_path)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("eckhardt_1991.end")


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    # Standardize names of conflict participants
    df = _clean_data(df)
    return df
