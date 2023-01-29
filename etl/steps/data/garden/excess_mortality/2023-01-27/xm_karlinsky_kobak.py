"""Load a meadow dataset and create a garden dataset.

TODO:
    - Detect year range (changes in pivoting might be needed)
    - Review output
    - Imporve values in column checks
"""
import json
from typing import List

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.data_helpers.misc import check_values_in_column
from etl.helpers import PathFinder

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


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
    log.info("xm_karlinsky_kobak.harmonize_countries")
    df = process(df)
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

    log.info("xm_karlinsky_kobak.end")


#
# Main table (`xm_karlinsky_kobak`) ########################################################
#


def process(df: pd.DataFrame) -> pd.DataFrame:
    df = harmonize_countries(df, paths.country_mapping_path)
    df = filter_entries(df)
    df = estimate_time_unit(df, ["entity", "time", "year"])
    # TODO: check year entries
    df = reshape_df(df)
    df = rename_columns(df)
    check_column_values(df)
    return df


def harmonize_countries(df: pd.DataFrame, countries_file: str) -> pd.DataFrame:
    country_column = "country"
    with open(countries_file) as f:
        country_mapping = json.load(f)
    check_values_in_column(df, country_column, list(country_mapping.keys()))
    df = geo.harmonize_countries(
        df=df,
        countries_file=countries_file,
        country_col=country_column,
        warn_on_missing_countries=True,
        warn_on_unused_countries=True,
    ).rename(columns={country_column: "entity"})
    return df


def filter_entries(df: pd.DataFrame) -> pd.DataFrame:
    """Filter some rows."""
    # Filter 2023 while kobak_age has no data for this year
    df = df[(df["year"] != 2023)].reset_index(drop=True)
    return df


def estimate_time_unit(df: pd.DataFrame, column_idx: List[str]) -> pd.DataFrame:
    """Deduce time unit from time column."""
    # Ensure that for each entity, we don't have the same time value repeated
    assert df[column_idx].value_counts().max() == 1, "There are duplicate year-entitiy-time pairs."
    # Deduce time unit
    ds = df.groupby("entity")["time"].nunique().sort_values(ascending=False)
    ds = ds.map({12: "monthly", 53: "weekly"})
    ds.name = "time_unit"
    assert ds.isna().sum() == 0
    # Add column to df
    shape_0 = df.shape
    df = df.merge(ds, on="entity")
    assert shape_0[0] == df.shape[0], "Something went wrong in merging! Some rows went missing"
    return df


def reshape_df(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot/unpivot to get data in the right format."""
    # Make wide [...l -> [[index], [years]]
    df = (
        df.assign(age="Total")
        .pivot(
            index=["entity", "time", "time_unit", "age"],
            columns="year",
            values="deaths",
        )
        .sort_values(["entity", "time"])
        .reset_index()
    )
    return df


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns."""
    df = df.rename(
        columns={
            2020: "baseline_proj",
            2021: "baseline_proj_21",
            2022: "baseline_proj_22",
            # 2023: "baseline_proj_23",
        }
    )
    return df


def check_column_values(df: pd.DataFrame) -> pd.DataFrame:
    check_values_in_column(df, "age", ["Total"])
    check_values_in_column(df, "time", list(range(1, 54)))


#
# By age table (`xm_karlinsky_kobak`) ########################################################
#


def process_age(df: pd.DataFrame) -> pd.DataFrame:
    path = paths.directory / (paths.short_name + ".age.countries.json")
    df = harmonize_countries(df, path)
    check_column_values_age(df)
    df = filter_entries_age(df)
    df = estimate_time_unit(df, ["entity", "time", "year", "age"])
    df = format_age(df)
    df = add_uk_age(df)
    df = reshape_df_age(df)
    df = rename_columns_age(df)
    return df


def check_column_values_age(df: pd.DataFrame):
    check_values_in_column(df, "age", {" D0_14", " D15_64", " D65_74", " D75_84", " D85p", " DTotal"})
    check_values_in_column(df, "sex", {" m", " f", " b"})
    check_values_in_column(df, "year", {2020, 2021, 2022})
    check_values_in_column(df, "time", list(range(1, 54)))


def filter_entries_age(df: pd.DataFrame):
    # Only keep both sexes
    df = df[df["sex"] == " b"].drop(columns=["sex"])
    # Don't include Australia by-age data, bc it's not from WMD
    df = df[df["entity"] != "Australia"]
    df["entity"] = df["entity"].cat.remove_unused_categories()
    # Don't include Total, that is included in kobak (all ages)
    df = df[df["age"] != " dtotal"]
    df["age"] = df["age"].cat.remove_unused_categories()
    return df


def format_age(df: pd.DataFrame):
    # Remove 'D' from age
    df["age"] = df["age"].str.replace(" d", "")
    return df


def add_uk_age(df: pd.DataFrame):
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


def reshape_df_age(df: pd.DataFrame):
    # Make wide [...l -> [[index], [years]]
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


def rename_columns_age(df: pd.DataFrame):
    """Rename columns."""
    df = df.rename(
        columns={
            2020: "baseline_proj",
            2021: "baseline_proj_21",
            2022: "baseline_proj_22",
        }
    )
    return df
