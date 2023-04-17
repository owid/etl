"""Load a meadow dataset and create a garden dataset."""

import os

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def filter_countries(df: pd.DataFrame) -> pd.DataFrame:
    # Only keep data from the United States and the United Kingdom. These time series are some of
    # the most complete in the dataset, and the most relevant to OWID readers.
    df["country"] = df.country.astype(str)
    selected_countries = ["United States", "United Kingdom"]
    return df[df.country.isin(selected_countries)]


def load_variable_files() -> pd.DataFrame:
    return pd.read_csv(os.path.join(paths.directory, "hcctad_variables.csv"))


def convert_and_rename(df: pd.DataFrame, vars: pd.DataFrame) -> pd.DataFrame:
    df = df.merge(vars, on="variable", validate="many_to_one")

    # Convert values to simple units
    # List of source units are available at https://data.nber.org/hccta/hcctadhelp.pdf
    df["value"] = df.value * df.multiply_by
    df = df.drop(columns="multiply_by")

    # Improve column names
    # Some variables are converted to the same names,
    # so that they can be summed up later in aggregate_variables()
    df = df.drop(columns="variable").rename(columns={"new_name": "variable"})

    return df


def fix_us_freight(df: pd.DataFrame) -> pd.DataFrame:
    # The source dataset contains a typo for railway freight traffic in 1959,
    # roughly 10x what it is in the years before and after.
    # See https://github.com/owid/owid-issues/issues/993#issuecomment-1473332534
    filter = (
        (df.variable == "Railway freight traffic (metric ton-km)") & (df.country == "United States") & (df.year == 1959)
    )
    return df[-filter]


def aggregate_variables(df: pd.DataFrame) -> pd.DataFrame:
    # Values are summed up across variable-country-year, to sum together the different types of
    # steel production and textile spindles.
    df = df.groupby(["country", "variable", "year"], as_index=False).sum()
    return df


def reshape_to_wide(df: pd.DataFrame) -> pd.DataFrame:
    # For this dataset, we want to be able to chart the UK and the US on completely separate charts,
    # and allow users to add/remove technologies. Therefore, countries should be variables and
    # technologies should be entities.
    df = (
        df.pivot(index=["variable", "year"], columns="country", values="value")
        .reset_index()
        .rename(columns={"variable": "country"})
    )
    return df


def run(dest_dir: str) -> None:
    log.info("hcctad.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("hcctad")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["hcctad"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)

    #
    # Process data.
    #
    vars = load_variable_files()
    df = (
        df.pipe(filter_countries)
        .pipe(convert_and_rename, vars)
        .pipe(aggregate_variables)
        .pipe(fix_us_freight)
        .pipe(reshape_to_wide)
    )

    # Create a new table with the processed data.
    tb_garden = Table(df, short_name="hcctad")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("hcctad.end")
