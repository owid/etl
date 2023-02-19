"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("flunet.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("flunet")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["flunet"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)

    #
    # Process data.
    #
    log.info("flunet.harmonize_countries")
    df = geo.harmonize_countries(
        df=df, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    df = clean_and_format_data(df)
    df = aggregate_surveillance_type(df)
    df = calculate_percent_positive(df)
    # Create a new table with the processed data.
    # tb_garden = Table(df, like=tb_meadow)
    tb_garden = Table(df, short_name=paths.short_name)
    tb_garden.update_metadata_from_yaml(paths.metadata_path, paths.short_name)
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)
    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("flunet.end")


def combine_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Combine columns of:
    * Influenza A with no subtype
    * Influenza B Victoria substrains
    """
    df["a_no_subtype"] = df["anotsubtyped"] + df["anotsubtypable"] + df["aother_subtype"]
    df["bvic"] = df["bvic_2del"] + df["bvic_3del"] + df["bvic_nodel"] + df["bvic_delunk"]

    return df


def create_date_from_iso_week(date_iso: pd.Series) -> pd.Series:
    """
    Convert iso week to date format
    """
    date = pd.to_datetime(date_iso, format="%Y-%m-%d", utc=True).dt.date.astype(str)
    return date


def clean_and_format_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean data by:
    * Converting date to date format
    * Combining subtype columns together
    * Drop unused columns
    """

    df["date"] = create_date_from_iso_week(df["iso_weekstartdate"])
    df = combine_columns(df)
    columns_to_drop = [
        "hemisphere",
        "anotsubtyped",
        "anotsubtypable",
        "aother_subtype",
        "bvic_2del",
        "bvic_3del",
        "bvic_nodel",
        "bvic_delunk",
        "ili_activity",
    ]
    df = df.drop(columns=columns_to_drop)
    return df


def aggregate_surveillance_type(combined_df: pd.DataFrame) -> pd.DataFrame:
    sel_cols = [
        "country",
        "date",
        "origin_source",
        "ah1n12009",
        "ah1",
        "ah3",
        "ah5",
        "ah7n9",
        "a_no_subtype",
        "inf_a",
        "byam",
        "bnotdetermined",
        "bvic",
        "inf_b",
        "inf_all",
        "inf_negative",
        "spec_processed_nb",
        "spec_received_nb",
    ]
    df = combined_df[sel_cols]
    df = df.copy(deep=True)
    # Summing all cases by country, hemisphere and date
    df_agg = df.groupby(["country", "date"]).sum().reset_index()
    # Check we haven't lost any cases along the way
    assert combined_df["inf_all"].sum() == df_agg["inf_all"].sum()
    return df_agg


def calculate_percent_positive(df: pd.DataFrame) -> pd.DataFrame:
    """
    Because the data is patchy in some places the WHO recommends three methods for calclating the share of influenza tests that are positive.
    In order of preference
    1. Postive tests divided by positive and negative tests summmed: inf_all/(inf_all + inf_neg)
    2. Positive tests divided by specimens processed: inf_all/spec_processed_nb
    3. Positive tests divided by specimens received: inf_all/spec_received_nb

    Remove rows where the percent is > 100
    Remove rows where the percent = 100 but all available denominators are 0.
    """

    df["pcnt_pos_1"] = (df["inf_all"] / (df["inf_all"] + df["inf_negative"])) * 100
    df["pcnt_pos_2"] = (df["inf_all"] / df["spec_processed_nb"]) * 100
    df["pcnt_pos_3"] = (df["inf_all"] / df["spec_received_nb"]) * 100

    # hierachically fill the 'pcnt_pos' column with values from the columns described above in order of preference: 1->2->3
    df["pcnt_pos"] = df["pcnt_pos_1"]
    df["pcnt_pos"] = df["pcnt_pos"].fillna(df["pcnt_pos_2"])
    df["pcnt_pos"] = df["pcnt_pos"].fillna(df["pcnt_pos_3"])

    df = df.drop(columns=["pcnt_pos_1", "pcnt_pos_2", "pcnt_pos_3"])

    # Drop rows where pcnt_pos is >100
    df.loc[df["pcnt_pos"] > 100, "pcnt_pos"] = np.nan

    # Rows where the percentage positive is 100 but all possible denominators are 0
    df.loc[
        (df["pcnt_pos"] == 100)
        & (df["inf_negative"] == 0)
        & (df["spec_processed_nb"] == 0)
        & (df["spec_received_nb"] == 0),
        "pcnt_pos",
    ] = np.nan
    return df
