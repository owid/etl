"""Load a meadow dataset and create a garden dataset."""
import json

import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore
from structlog import get_logger

from etl.data_helpers import geo
from etl.data_helpers.misc import check_values_in_column
from etl.helpers import PathFinder

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


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
    log.info("wmd: processing dataframe")
    df = process(df)

    # Create a new table with the processed data.
    tb_garden = Table(df, short_name=paths.short_name)

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
    # Harmonize country names
    df = harmonize_countries(df)
    # Reshape dataframe
    df = reshape_df(df)
    # Check values in column
    check_column_values(df)
    # Ensure columns match expected format
    df.columns = [underscore(str(col)) for col in df.columns]
    return df


def harmonize_countries(df: pd.DataFrame) -> pd.DataFrame:
    country_column = "country_name"
    with open(paths.country_mapping_path) as f:
        country_mapping = json.load(f)
    check_values_in_column(df, country_column, list(country_mapping.keys()))
    df = geo.harmonize_countries(
        df=df,
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
        country_col=country_column,
        warn_on_missing_countries=True,
        warn_on_unused_countries=True,
    ).rename(columns={country_column: "entity"})
    return df


def reshape_df(df: pd.DataFrame) -> pd.DataFrame:
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


def check_column_values(df: pd.DataFrame):
    check_values_in_column(df, "age", ["Total"])
    check_values_in_column(df, "time", list(range(1, 54)))
    check_values_in_column(df, "time_unit", ["monthly", "weekly"])
    return df
