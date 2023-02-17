"""Load a meadow dataset and create a garden dataset."""
from typing import Any, Dict, List

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
# Year range to be used (rest is filtered out)
YEAR_MIN = 2020
YEAR_MAX = 2023


def run(dest_dir: str) -> None:
    log.info("xm_karlinsky_kobak.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("xm_karlinsky_kobak")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["xm_karlinsky_kobak"]
    tb_meadow_age = ds_meadow["xm_karlinsky_kobak_by_age"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)
    df_age = pd.DataFrame(tb_meadow_age)

    #
    # Process data.
    #
    log.info("xm_karlinsky_kobak.main: processing data")
    df = process(df)
    log.info("xm_karlinsky_kobak.ages: processing data")
    df_age = process_age(df_age)

    # Create a new table with the processed data.
    tb_garden = Table(df, short_name=tb_meadow.metadata.short_name)
    tb_garden_age = Table(df_age, short_name=tb_meadow_age.metadata.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = Dataset.create_empty(dest_dir, metadata=ds_meadow.metadata)

    # Add table of processed data to the new dataset.
    ds_garden.add(tb_garden)
    ds_garden.add(tb_garden_age)

    # Update dataset and table metadata using the adjacent yaml file.
    ds_garden.update_metadata(paths.metadata_path)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("xm_karlinsky_kobak: end")


#
# Main table (`xm_karlinsky_kobak`) ########################################################
#
# Minimum and maximum years expected in data
YEAR_MIN_EXPECTED = 2020
YEAR_MAX_EXPECTED = 2023


def process(df: pd.DataFrame) -> pd.DataFrame:
    # Check dataframe fields and values
    log.info("\txm_karlinsky_kobak.main: initial dataframe API check")
    df_api_check(df)
    # Harmonize country names
    log.info("\txm_karlinsky_kobak.main: harmonising country names")
    df = harmonize_countries(df, "country", paths.country_mapping_path)
    # Filter some rows
    log.info("\txm_karlinsky_kobak.main: filtering entries")
    df = filter_entries(df)
    # Estimate time unit and add column
    log.info("\txm_karlinsky_kobak.main: estimating time_unit")
    df = estimate_time_unit(df, ["entity", "time", "year"])
    # Reshape dataframe
    log.info("\txm_karlinsky_kobak.main: reshaping dataframe")
    df = reshape_df(df)
    # Rename column names
    log.info("\txm_karlinsky_kobak.main: renaming columns in dataframe")
    df = rename_columns(df)
    # Format age
    log.info("\txm_karlinsky_kobak.main: assign age value")
    df = format_age(df)
    # Check dataframe fields and values
    log.info("\txm_karlinsky_kobak.main: final formatting")
    df = format_columns(df)
    return df


def df_api_check(df: pd.DataFrame) -> None:
    # Check years
    check_values_in_column(df, "year", list(range(YEAR_MIN_EXPECTED, YEAR_MAX_EXPECTED + 1)))
    # # Check time and time_unit
    check_values_in_column(df, "time", list(range(1, 54)))


def filter_entries(df: pd.DataFrame) -> pd.DataFrame:
    """Filter some rows."""
    # Filter 2023 while kobak_age has no data for this year
    df = df[(df["year"] >= YEAR_MIN) & (df["year"] <= YEAR_MAX)].reset_index(drop=True)
    return df


def estimate_time_unit(df: pd.DataFrame, column_idx: List[str]) -> pd.DataFrame:
    """Deduce time unit from time column."""
    # Ensure that for each entity and year, we don't have the same time value repeated
    assert df[column_idx].value_counts().max() == 1, "There are duplicate year-entitiy-time pairs."
    # Estimate time unit based on the number of different times.
    ds = df.groupby("entity")["time"].nunique().sort_values(ascending=False)
    ds = ds.map({12: "monthly", 53: "weekly"})
    ds.name = "time_unit"
    assert (
        ds.isna().sum() == 0
    ), "Some entities have neither 12 nor 53 time values. Therefore `time_unit` cannot be estimated!"
    # Add `time_unit` column to df
    shape_0 = df.shape
    df = df.merge(ds, on="entity")
    assert shape_0[0] == df.shape[0], "Something went wrong in merging! Some rows went missing"
    return df


def reshape_df(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot/unpivot to get data in the right format."""
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


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns."""
    # Rename columns
    column_renaming = _get_column_renaming(df)
    df = df.rename(columns=column_renaming)
    return df


def _get_column_renaming(df: pd.DataFrame) -> Dict[Any, str]:
    """Build column renaming dictionary."""
    # Build column rename dictionary
    template = "baseline_proj"
    # Special naming for 2020
    column_renaming = {
        2020: template,
    }
    for year in range(YEAR_MIN + 1, YEAR_MAX + 1):
        year_yy = str(year)[-2:]
        column_renaming[year] = f"{template}_{year_yy}"
    return column_renaming


def format_age(df: pd.DataFrame) -> pd.DataFrame:
    """Remove 'd' from age strings."""
    df["age"] = "all_ages"
    return df


def format_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Adapt dataframe column names to WMD-like format."""
    cols_first = ["entity", "time", "time_unit", "age"]
    column_metrics = list(_get_column_renaming(df).values())
    # Sort columns
    df = df[cols_first + sorted(column_metrics)]
    # String columns
    df.columns = [underscore(str(col)) for col in df.columns]
    return df


#
# By age table (`xm_karlinsky_kobak`) ########################################################
#
# Minimum and maximum years expected in data
YEAR_MIN_EXPECTED_AGE = 2020
YEAR_MAX_EXPECTED_AGE = 2023


def process_age(df: pd.DataFrame) -> pd.DataFrame:
    # Check dataframe fields and values
    df_api_check_by_age(df)
    # Harmonize country names
    log.info("\txm_karlinsky_kobak.ages: harmonising country names")
    country_mapping_path = paths.directory / (paths.short_name + ".age.countries.json")
    df = harmonize_countries(df, "country", country_mapping_path)
    # Filter entries
    log.info("\txm_karlinsky_kobak.ages: filtering entries")
    df = filter_entries_by_age(df)
    # Estimate `time_unit`
    log.info("\txm_karlinsky_kobak.ages: estimating `time_unit`")
    df = estimate_time_unit(df, ["entity", "time", "year", "age"])
    # Format age
    log.info("\txm_karlinsky_kobak.ages: add UK values")
    df = add_uk_by_age(df)
    # Reshaping dataframe
    log.info("\txm_karlinsky_kobak.ages: reshaping dataframe")
    df = reshape_df_by_age(df)
    # Renaming columns
    log.info("\txm_karlinsky_kobak.ages: rename dataframe columns")
    df = rename_columns(df)
    # Format age
    log.info("\txm_karlinsky_kobak.ages: format age")
    df = format_age_by_age(df)
    # Final formatting
    log.info("\txm_karlinsky_kobak.ages: final formatting")
    df = format_columns_by_age(df)
    return df


def df_api_check_by_age(df: pd.DataFrame) -> None:
    # Check years
    check_values_in_column(df, "year", list(range(YEAR_MIN_EXPECTED_AGE, YEAR_MAX_EXPECTED_AGE + 1)))
    # Check time and time_unit
    check_values_in_column(df, "time", list(range(1, 54)))
    # Check time and time_unit
    check_values_in_column(df, "sex", {" m", " f", " b"})
    # Check time and time_unit
    check_values_in_column(df, "age", {" D0_14", " D15_64", " D65_74", " D75_84", " D85p", " DTotal"})


def filter_entries_by_age(df: pd.DataFrame):
    # Only keep both sexes
    df = df[df["sex"] == " b"].drop(columns=["sex"])
    # Don't include Australia by-age data, bc it's not from WMD
    df = df[~df["entity"].isin(["Australia", "Canada"])]
    df["entity"] = df["entity"].cat.remove_unused_categories()
    # Don't include Total, that is included in kobak (all ages)
    df = df[df["age"] != " DTotal"]
    df["age"] = df["age"].cat.remove_unused_categories()
    return df


def add_uk_by_age(df: pd.DataFrame):
    # Get UK Nations data
    df_uk = df[df["entity"].isin(["England & Wales", "Scotland", "Northern Ireland"])].copy()
    # Check time_unit
    time_units = df_uk["time_unit"].unique()
    assert len(time_units) == 1, "There are multiple time units for UK Nations"
    # Estimate metrics
    df_uk = df_uk.groupby(["year", "time", "age"], as_index=False).sum(min_count=3)
    # Reassign entity name and time unit
    df_uk["entity"] = "United Kingdom"
    df_uk["time_unit"] = time_units[0]
    # Add UK
    df = pd.concat([df, df_uk], ignore_index=True)
    return df


def reshape_df_by_age(df: pd.DataFrame):
    # Make wide [...] -> [[index], [years]]
    df = (
        df.pivot(
            index=["entity", "time", "time_unit", "age"],
            columns="year",
            values="deaths",
        )
        .reset_index()
        .sort_values(["entity", "time", "age"])
    )
    return df


def format_age_by_age(df: pd.DataFrame):
    # Remove 'D' from age
    df["age"] = df["age"].str.replace(" D", "")
    return df


def format_columns_by_age(df: pd.DataFrame) -> pd.DataFrame:
    """Adapt dataframe column names to WMD-like format."""
    cols_first = ["entity", "time", "time_unit", "age"]
    column_metrics = list(_get_column_renaming(df).values())
    # # Sort columns
    df = df[cols_first + sorted(column_metrics)]
    # # String columns
    df.columns = [underscore(str(col)) for col in df.columns]
    return df
