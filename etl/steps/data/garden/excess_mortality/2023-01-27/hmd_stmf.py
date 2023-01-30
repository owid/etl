"""Load a meadow dataset and create a garden dataset."""
from datetime import date

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
# This year
THIS_YEAR = date.today().year
# Minimum and maximum years expected in data
YEAR_MIN_EXPECTED = 1990
YEAR_MAX_EXPECTED = 2023
# Year range to be used (rest is filtered out)
YEAR_MIN = 2010
YEAR_MAX = 3000  # (No actual limit)


def run(dest_dir: str) -> None:
    log.info("hmd_stmf: start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("hmd_stmf")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["hmd_stmf"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)

    #
    # Process data.
    #
    log.info("hmd_stmf: processing data")
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

    log.info("hmd_stmf: end")


def process(df: pd.DataFrame) -> pd.DataFrame:
    # Check dataframe fields and values
    log.info("\thmd_stmf: initial dataframe API check")
    df_api_check(df)
    # Harmonize country names
    log.info("\thmd_stmf: harmonizing country names")
    df = harmonize_countries(df, "countrycode", paths.country_mapping_path, paths.excluded_countries_path)
    # Filter some rows
    log.info("\thmd_stmf: filtering entries")
    df = filter_entries(df)
    # Reshape dataframe
    log.info("\thmd_stmf: reshaping dataframe")
    df = reshape_df(df)
    # Add UK entries (sum nations)
    log.info("\thmd_stmf: adding UK entries")
    df = add_uk(df)
    # Clean age display names
    log.info("\thmd_stmf: clean age display names")
    df = format_age(df)
    # Final touches on dataframe format
    log.info("\thmd_stmf: format columns in dataframe")
    df = format_columns(df)
    return df


def df_api_check(df: pd.DataFrame) -> None:
    check_values_in_column(df, "year", list(range(YEAR_MIN_EXPECTED, YEAR_MAX_EXPECTED + 1)))
    check_values_in_column(df, "week", list(range(1, 54)))
    check_values_in_column(df, "sex", ["m", "f", "b"])


def filter_entries(df: pd.DataFrame) -> pd.DataFrame:
    """Filter some rows."""
    # Select only years YEAR_MIN - YEAR_MAX (2010-2019 (for baseline) and 2020-now)
    df = df[(df["year"] >= YEAR_MIN) & (df["year"] <= YEAR_MAX)]
    # Keep only both sex data
    df = df[df["sex"] == "b"].drop(columns=["sex"])
    return df


def reshape_df(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot/unpivot to get data in the right format."""
    # Pivot long [Entity, Year, Week, Sex, [D*]] -> [Entity, Week, Sex, Age, [Years]]
    df = df.melt(
        id_vars=["entity", "year", "week"],
        value_vars=["d0_14", "d15_64", "d65_74", "d75_84", "d85p", "dtotal"],
        var_name="age",
        value_name="deaths",
    )
    # Pivot wide
    df = df.pivot(
        index=["entity", "week", "age"],
        columns="year",
        values="deaths",
    ).reset_index()

    # Rename columns
    return df


def add_uk(df: pd.DataFrame):
    """Add UK to main dataframe.

    By default, the dataset only contains data for England & Wales, Scotland and Northern Ireland.
    """
    # Get UK Nations
    df_uk = df[df["entity"].isin(["England & Wales", "Scotland", "Northern Ireland"])].copy()
    # Years to consider (starting from 2015
    column_years = list(filter(lambda x: x >= 2015, df_uk.filter(regex=r"20\d\d").columns))
    # Sanity check
    assert (
        df_uk[[col for col in column_years if col != THIS_YEAR]].isna().sum() < 20
    ).all(), "Too many missing values. Check values in year columns!"
    # Group by and get sum
    df_uk = df_uk.groupby(["week", "age"], as_index=False)[column_years].sum(min_count=3)
    # Assign Entity name
    df_uk["entity"] = "United Kingdom"
    # Add UK to main dataset
    df = pd.concat([df, df_uk], ignore_index=True)
    return df


def format_age(df: pd.DataFrame) -> pd.DataFrame:
    """Remove 'd' from age strings."""
    df["age"] = df["age"].str.replace("d", "")
    df.loc[df["age"] == "total", "age"] = "all_ages"
    return df


def format_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Adapt dataframe column names to WMD-like format."""
    # Sort columns
    cols_first = ["entity", "week", "age"]
    df = df[cols_first + sorted(col for col in df.columns if col not in cols_first)]
    # String columns
    df.columns = [underscore(str(col)) for col in df.columns]
    return df
