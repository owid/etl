"""Load snapshot of IRENA's Renewable Energy Patents and create a raw data table.

"""
from etl.helpers import PathFinder, create_dataset

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


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("renewable_energy_patents.xlsx")
    tb = snap.read(sheet_name="INSPIRE_data")

    #
    # Process data.
    #
    # Select and rename columns conveniently.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year", "sector", "technology", "sub_technology"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_meadow.save()
