"""Load World Inequality Database meadow dataset and create a garden dataset."""


import pandas as pd
from owid.catalog import Dataset, Table
from shared import add_metadata_vars, add_metadata_vars_distribution
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Data processing function (cleaning and small transformations)
def data_processing(df: pd.DataFrame) -> pd.DataFrame:
    # Multiply shares by 100
    df[list(df.filter(like="share"))] *= 100

    # Delete age and pop, two one-value variables
    df = df.drop(columns=["age", "pop", "age_extrapolated", "pop_extrapolated"])

    # Delete some share ratios we are not using, and also the p0p40 (share) variable only available for pretax
    drop_list = ["s90_s10_ratio", "s90_s50_ratio", "p0p40"]

    for var in drop_list:
        df = df[df.columns.drop(list(df.filter(like=var)))]

    # Verify index and sort
    df = df.set_index(["country", "year"], verify_integrity=True).sort_index()

    return df


def run(dest_dir: str) -> None:
    log.info("world_inequality_database.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("world_inequality_database")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["world_inequality_database"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow).reset_index()

    #
    # Process data.
    # Change units and drop unnecessary columns
    df = data_processing(df)

    # Create a new table with the processed data.
    tb_garden = Table(df, short_name="world_inequality_database")

    tb_garden = add_metadata_vars(tb_garden)

    ########################################
    # Percentile data
    ########################################

    # Read table from meadow dataset.
    tb_percentiles = ds_meadow["world_inequality_database_distribution"]

    #
    # Process data.
    # Multiple share and share_extrapolated columns by 100
    tb_percentiles[["share", "share_extrapolated"]] *= 100

    # Add metadata by code
    tb_percentiles = add_metadata_vars_distribution(tb_percentiles)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset and add the garden table.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden, tb_percentiles], default_metadata=ds_meadow.metadata)

    # Update dataset and table metadata using the adjacent yaml file.
    ds_garden.update_metadata(paths.metadata_path)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("world_inequality_database.end")
