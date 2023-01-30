"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore
from shared import harmonize_countries
from structlog import get_logger

from etl.data_helpers.misc import check_values_in_column
from etl.helpers import PathFinder

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Minimum and maximum years expected in data
YEAR_MIN_EXPECTED = 2015
YEAR_MAX_EXPECTED = 2023


def run(dest_dir: str) -> None:
    log.info("wmd: start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("wmd")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["wmd"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)

    #
    # Process data.
    #
    log.info("wmd: processing data")
    df = process(df)

    # Create a new table with the processed data.
    tb_garden = Table(df, short_name=tb_meadow.metadata.short_name)

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

    log.info("wmd: end")


def process(df: pd.DataFrame) -> pd.DataFrame:
    # Check dataframe fields and values
    log.info("\thmd_stmf: initial dataframe API check")
    df_api_check(df)
    # Harmonize country names
    log.info("\twmd: harmonising country names")
    df = harmonize_countries(df, "country_name", paths.country_mapping_path)
    # Reshape dataframe
    log.info("\twmd: reshaping dataframe")
    df = reshape_df(df)
    # Clean age display names
    log.info("\twmd: clean age display names")
    df = format_age(df)
    # Ensure columns match expected format
    log.info("\twmd: format columns in dataframe")
    df = format_columns(df)
    return df


def df_api_check(df: pd.DataFrame) -> None:
    # Check years
    check_values_in_column(df, "year", list(range(YEAR_MIN_EXPECTED, YEAR_MAX_EXPECTED + 1)))
    # Check time and time_unit
    check_values_in_column(df, "time", list(range(1, 54)))
    check_values_in_column(df, "time_unit", ["monthly", "weekly"])
    # If time_unit=="monthly", time should be in range(1, 13).
    check_values_in_column(df[df["time_unit"] == "monthly"], "time", list(range(1, 13)))
    # If time_unit=="weekly", time should be in range(1, 54).
    check_values_in_column(df[df["time_unit"] == "weekly"], "time", list(range(1, 54)))


def reshape_df(df: pd.DataFrame) -> pd.DataFrame:
    # Make wide [...l -> [[index], [years]]
    df = (
        df.pivot(
            index=["entity", "time", "time_unit"],
            columns="year",
            values="deaths",
        )
        .sort_values(["entity", "time"])
        .reset_index()
    )
    return df


def format_age(df: pd.DataFrame) -> pd.DataFrame:
    """Create column `age` with value 'all_ages'."""
    return df.assign(**{"age": "all_ages"})


def format_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Final touches."""
    # Sort columns
    cols_first = ["entity", "time", "time_unit", "age"]
    df = df[cols_first + sorted(col for col in df.columns if col not in cols_first)]
    # String columns
    df.columns = [underscore(str(col)) for col in df.columns]
    return df
