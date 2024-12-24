"""Load a meadow dataset and create a garden dataset."""

import datetime

import pandas as pd
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def define_orbits(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[df.periapsis <= 2000, "orbit"] = "Low Earth orbit"
    df.loc[(df.periapsis >= 2000) & (df.periapsis <= 35586), "orbit"] = "Medium Earth orbit"
    df.loc[(df.periapsis >= 35586) & (df.periapsis <= 35986), "orbit"] = "Geostationary orbit"
    df.loc[df.periapsis >= 35986, "orbit"] = "High Earth orbit"
    return df


def create_year_cols(df: pd.DataFrame) -> pd.DataFrame:
    df["launch_year"] = pd.to_datetime(df.launch_date, format="%Y-%m-%d").dt.year
    df["decay_year"] = pd.to_datetime(df.decay_date, format="%Y-%m-%d").dt.year
    return df


def filter_years(df: pd.DataFrame) -> pd.DataFrame:
    # Remove events from current year
    df = df[df.launch_year < datetime.date.today().year]
    return df


def count_leo_by_type(df: pd.DataFrame) -> pd.DataFrame:
    # Objects in Lower Earth orbit over time, broken down by object type

    df = df[df.object_type.isin(["PAYLOAD", "ROCKET BODY", "DEBRIS"])]
    df = df[df.orbit == "Low Earth orbit"]

    years = range(df.launch_year.min().astype(int), df.launch_year.max().astype(int) + 1)

    dataframes = []
    for year in years:
        # For each year, keep all launched objects up to that year & that haven't decayed yet
        df_year = df[(df.launch_year <= year) & (df.decay_date.isnull() | (df.decay_year > year))]
        df_year = (
            df_year[["object_type"]].groupby("object_type", as_index=False, observed=True).size().assign(year=year)
        )
        dataframes.append(df_year)

    leo_by_type = (
        pd.concat(dataframes).reset_index(drop=True).rename(columns={"object_type": "entity", "size": "objects"})
    )  # type: ignore

    return leo_by_type


def count_non_debris_by_orbit(df: pd.DataFrame) -> pd.DataFrame:
    # Non-debris objects in space over time, broken down by orbit

    df = df[df.object_type.isin(["PAYLOAD", "ROCKET BODY"])]

    years = range(df.launch_year.min().astype(int), df.launch_year.max().astype(int) + 1)

    dataframes = []
    for year in years:
        # For each year, keep all launched objects up to that year & that haven't decayed yet
        df_year = df[(df.launch_year <= year) & (df.decay_date.isnull() | (df.decay_year > year))]
        df_year = df_year[["orbit"]].groupby("orbit", as_index=False).size().assign(year=year)
        dataframes.append(df_year)

    non_debris_by_orbit = (
        pd.concat(dataframes).reset_index(drop=True).rename(columns={"orbit": "entity", "size": "objects"})
    )  # type: ignore

    return non_debris_by_orbit


def run(dest_dir: str) -> None:
    log.info("space_track.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("space_track")

    # Read table from meadow dataset.
    tb = ds_meadow["space_track"]

    #
    # Process data.
    #
    tb = tb.pipe(define_orbits).pipe(create_year_cols).pipe(filter_years)

    final = pd.concat([count_leo_by_type(tb), count_non_debris_by_orbit(tb)]).reset_index(drop=True)
    final["entity"] = final["entity"].replace(
        {"ROCKET BODY": "Rocket bodies", "PAYLOAD": "Payloads", "DEBRIS": "Debris"}
    )
    final.metadata.short_name = "space_track"

    # Clean for ETL
    final = final.rename(columns={"entity": "country"}).set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[final], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("space_track.end")
