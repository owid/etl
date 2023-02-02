"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Dataset, Table
from shared import make_tables, table_to_clean_df
from structlog import get_logger

from etl.helpers import PathFinder

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Short name
SHORT_NAME = paths.short_name


def run(dest_dir: str) -> None:
    log.info(f"{SHORT_NAME}: starting")

    #
    # Load inputs.
    #
    log.info(f"{SHORT_NAME}: loading inputs")
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency(SHORT_NAME)
    # Read table from meadow dataset.
    tb_meadow = ds_meadow[SHORT_NAME]
    # Create a dataframe with data from the table.

    #
    # Process data.
    #
    log.info(f"{SHORT_NAME}: processing dataframe")
    df = clean_df(tb_meadow)

    # Create a new table with the processed data.
    log.info(f"{SHORT_NAME}: generating tables")
    tables = make_tables(df, SHORT_NAME)

    #
    # Save outputs.
    #
    log.info(f"{SHORT_NAME}: adding tables to dataset")
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = Dataset.create_empty(dest_dir, metadata=ds_meadow.metadata)
    # Add table of processed data to the new dataset.
    for table in tables:
        ds_garden.add(table)
    # Update dataset and table metadata using the adjacent yaml file.
    ds_garden.update_metadata(paths.metadata_path)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info(f"{SHORT_NAME}: end")


def clean_df(tb: Table) -> pd.DataFrame:
    # Standardize names of conflict participants
    entities_with_comma = [
        "Great Colombia independence campaigners (in Venezuela, Colombia and Ecuador)",
    ]
    df = table_to_clean_df(tb, entities_with_comma=entities_with_comma)
    return df
