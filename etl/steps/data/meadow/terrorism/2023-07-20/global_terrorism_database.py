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
    weaptype1_txt: Represents the type of weapon used in the incident.
    attacktype1_txt:  Represents the hierarchy of attack types, including:
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
        - nkill: Stores the number of total confirmed fatalities for the incident.
        - nwound: Records the number of confirmed non-fatal injuries to both perpetrators and victims.
    """
    #
    # Load inputs.

    #
    # Retrieve snapshot.
    snap_2020 = cast(Snapshot, paths.load_dependency("global_terrorism_database.xlsx"))
    snap_2021 = cast(Snapshot, paths.load_dependency("global_terrorism_database_2021.xlsx"))

    COLUMNS_OF_INTEREST = [
        "iyear",
        "imonth",
        "iday",
        "country_txt",
        "region_txt",
        "attacktype1_txt",
        "weaptype1_txt",
        "targtype1_txt",
        "nkill",
        "nwound",
    ]

    # Load data from snapshot.
    df_2020 = pd.read_excel(snap_2020.path)[COLUMNS_OF_INTEREST]
    df_2021 = pd.read_excel(snap_2021.path)[COLUMNS_OF_INTEREST]
    df = pd.concat([df_2020, df_2021])
    df.rename(columns={"country_txt": "country", "iyear": "year", "imonth": "month", "iday": "day"}, inplace=True)
    df.set_index(["country", "year", "day", "month"], inplace=True)

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
