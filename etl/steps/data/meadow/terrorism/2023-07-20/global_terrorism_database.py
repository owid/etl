"""Load a snapshot and create a meadow dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

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


def run() -> None:
    """
    Process global terrorism data from the years 2020 and 2021 and save the results in a new Meadow dataset.

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
    # Retrieve snapshots for terrorism data up until 2020 and terrorism data between 2020-2021.
    snap_2020 = paths.load_snapshot("global_terrorism_database.csv")
    snap_2021 = paths.load_snapshot("global_terrorism_database_2021.csv")

    # Load data from snapshots using snap.read.
    tb_2020 = snap_2020.read(low_memory=False)
    tb_2021 = snap_2021.read(low_memory=False)

    #
    # Process data.
    #

    # Select columns of interest and combine terrorism data.
    tb = pr.concat([tb_2020[COLUMNS_OF_INTEREST], tb_2021[COLUMNS_OF_INTEREST]])

    # Rename country and year columns.
    tb = tb.rename(columns={"country_txt": "country", "iyear": "year"})
    # Create a new table and ensure all columns are snake-case.
    tb = tb.set_index(
        [
            "country",
            "year",
        ]
    )

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
