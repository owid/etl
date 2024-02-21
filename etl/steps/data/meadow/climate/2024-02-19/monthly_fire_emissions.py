"""Load a snapshot and create a meadow dataset."""

import zipfile

from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("monthly_fire_emissions.zip")
    file_name = "emission_gfed_full_2002_2022.csv"

    # Create a temporary directory to extract the file to
    with zipfile.ZipFile(snap.path) as z:
        # open the csv file in the dataset
        with z.open(file_name) as f:
            # Extract the file
            tb = pr.read_csv(f, metadata=snap.to_table_metadata(), origin=snap.m.origin)

    tb.metadata = snap.to_table_metadata()
    columns_to_keep = [
        "year",
        "month",
        "country",
        "region",
        "CO2",
        "CO",
        "TPM",
        "PM25",
        "TPC",
        "NMHC",
        "OC",
        "CH4",
        "SO2",
        "BC",
        "NOx",
    ]
    tb = tb[columns_to_keep]
    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.set_index(["country", "year", "month", "region"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
