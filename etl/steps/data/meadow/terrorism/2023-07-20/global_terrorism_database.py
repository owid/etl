"""Load a snapshot and create a meadow dataset."""

from typing import cast

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    """
    Process global terrorism data from the years 2020 and 2021 and save the results in a new Meadow dataset.

    Args:
        dest_dir (str): The directory where the new Meadow dataset will be created.

    targtype1_txt: the target/victim type field captures the general type of target/victim.
    weaptype1_txt:  the type of weapon used in the incident.
    attacktype1_txt:  the hierarchy of attack types, including:
          - Assassination
          - Hijacking
          - Kidnapping
          - Barricade Incident
          - Bombing/Explosion
          - Armed Assault
          - Unarmed Assault
          - Facility/Infrastructure Attack
          - Unknown


    Casualty Fields:
        - nkill: total confirmed fatalities for the incident
        - nwound: confirmed non-fatal injuries to both perpetrators and victims
    """
    #
    # Load inputs.

    #
    # Retrieve snapshots for terrorism data up intil 2020 and terrorism data between 2020-2021.
    snap_2020 = cast(Snapshot, paths.load_dependency("global_terrorism_database.csv"))
    snap_2021 = cast(Snapshot, paths.load_dependency("global_terrorism_database_2021.csv"))
    # Select columns of interest
    COLUMNS_OF_INTEREST = [
        "iyear",
        "country_txt",
        "region_txt",
        "attacktype1_txt",
        "weaptype1_txt",
        "targtype1_txt",
        "nkill",
        "nwound",
        "suicide",
    ]

    # Load data from snapshots.
    df_2020 = pd.read_csv(snap_2020.path, low_memory=False)
    df_2021 = pd.read_csv(snap_2021.path, low_memory=False)
    # Combine terrorism data up until 2020 and 2020-2021.
    df = pd.concat([df_2020[COLUMNS_OF_INTEREST], df_2021[COLUMNS_OF_INTEREST]])
    # Rename country and year columns
    df.columns = ["country" if col == "country_txt" else "year" if col == "iyear" else col for col in df.columns]

    df.set_index(["country", "year"], inplace=True)
    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap_2020.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()
