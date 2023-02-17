"""Load a meadow dataset and create a garden dataset."""

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


def clean_and_format_data(df: pd.DataFrame) -> pd.DataFrame:
    df["date"] = df["iso_weekstartdate"]
    return df


def create_date_from_iso_week(date_iso: pd.Series) -> pd.Series:
    date = pd.to_datetime(date_iso, format="%Y-%m-%d", utc=True).dt.date.astype(str)
    return date


def aggregate_surveillance_type(combined_df: pd.DataFrame) -> pd.DataFrame:
    sel_cols = [
        "country",
        "hemisphere",
        "date",
        "origin_source",
        "ah1n12009",
        "ah1",
        "ah3",
        "ah5",
        "ah7n9",
        "anotsubtyped",
        "anotsubtypable",
        "aother_subtype",
        "aother_subtype_details",
        "inf_a",
        "bvic_2del",
        "bvic_3del",
        "bvic_nodel",
        "bvic_delunk",
        "byam",
        "bnotdetermined",
        "inf_b",
        "inf_all",
        "inf_negative",
        "ili_activity",
        "spec_processed_nb",
        "spec_received_nb",
    ]
    df = combined_df[sel_cols]
    df = df.copy(deep=True)
    # Summing all cases by country, hemisphere and date
    df_agg = df.groupby(["country", "hemisphere", "date"]).sum().reset_index()
    # Check we haven't lost any cases along the way
    assert combined_df["inf_all"].sum() == df_agg["inf_all"].sum()
    return df_agg
