"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from owid.catalog import Table
from owid.datautils.dataframes import multi_merge
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define columns to select from sheet and how to rename them.
COLUMNS = {
    "Accidents \n(excl. suicide, sabotage, hijackings etc.) Year": "year",
    "Accidents ": "accidents_excluding_hijacking_etc",
    "Fatalities": "fatalities_excluding_hijacking_etc",
    "Accidents \n(incl. suicide, sabotage, hijackings etc.) Accidents ": "accidents_including_hijacking_etc",
    "Fatalities.1": "fatalities_including_hijacking_etc",
    "Accidents with passenger flights \n(incl. suicide, sabotage, hijackings etc.) Accidents ": "accidents_with_passenger_flights_including_hijacking_etc",
    "Fatalities.2": "fatalities_with_passenger_flights_including_hijacking_etc",
    "Accidents with passenger + cargo flights\n(incl. suicide, sabotage, hijackings etc.) Accidents ": "accidents_with_passenger_and_cargo_flights_including_hijacking_etc",
    "Fatalities.3": "fatalities_with_passenger_and_cargo_flights_including_hijacking_etc",
    # 'World air traffic (departures)': '',
    # '1 accident \nper x flights': '',
    # 'fatal accidents \nper mln flights': '',
    # '5-year \nmoving avg': '',
    "Corporate jets (civilian) Accidents ": "accidents_with_corporate_jets",
    "Fatalities.4": "fatalities_with_corporate_jets",
    # 'moving 5 year average # of accidents': '',
}


def run(dest_dir: str) -> None:
    log.info("aviation_statistics.start")

    #
    # Load inputs.
    #
    # Retrieve snapshots.
    snap: Snapshot = paths.load_dependency("aviation_statistics.csv")
    snap_by_period: Snapshot = paths.load_dependency("aviation_statistics_by_period.csv")
    snap_by_nature: Snapshot = paths.load_dependency("aviation_statistics_by_nature.csv")

    # Load data from snapshots.
    df = pd.read_csv(snap.path)
    df_by_period = pd.read_csv(snap_by_period.path)
    df_by_nature = pd.read_csv(snap_by_nature.path)

    #
    # Process data.
    #
    # Select necessary columns and rename them appropriately.
    df = df[list(COLUMNS)].rename(columns=COLUMNS)
    df_by_period = df_by_period.rename(columns={"Year": "year"})
    df_by_nature = df_by_nature.rename(columns={"Year": "year"})

    # Drop last row (which should be the only one without a year), which gives a grand total.
    df = df.dropna(subset="year").reset_index(drop=True).astype({"year": int})

    # Combine all dataframes.
    df_combined = multi_merge([df, df_by_period, df_by_nature], how="outer", on=["year"])

    # Add a country column (that only contains "World").
    df_combined["country"] = "World"

    # Set an appropriate index and sort conveniently.
    df_combined = df_combined.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    # Create a new table and ensure all columns are snake-case.
    tb = Table(df_combined, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("aviation_statistics.end")
