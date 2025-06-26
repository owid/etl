"""Load snapshot of IRENA's Renewable Energy Patents and create a raw data table."""

from etl.helpers import PathFinder

# Get naming conventions.
paths = PathFinder(__file__)

# Columns to use from raw data and how to rename them.
COLUMNS = {
    "Country": "country",
    "Year": "year",
    "Sector": "sector",
    "Technology": "technology",
    "Subtechnology": "sub_technology",
    "Filed Patents": "patents",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("renewable_energy_patents.csv")
    tb = snap.read()

    #
    # Process data.
    #
    # Select and rename columns conveniently.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Set an appropriate index and sort conveniently.
    tb = tb.format(keys=["country", "year", "sector", "technology", "sub_technology"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb])
    ds_meadow.save()
