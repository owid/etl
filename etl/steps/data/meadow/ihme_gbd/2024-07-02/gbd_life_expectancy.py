"""Load a snapshot and create a meadow dataset."""
import zipfile

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("gbd_life_expectancy.zip")
    tb = Table()
    with zipfile.ZipFile(snap.path, "r") as z:
        # Find the first CSV file in the zip archive
        csv_filename = next((name for name in z.namelist() if name.endswith(".csv")), None)

        if csv_filename is not None:
            # Open the CSV file within the zip archive
            with z.open(csv_filename) as csv_file:
                # Read the CSV file into a pandas DataFrame
                tb = pr.read_csv(csv_file, metadata=snap.to_table_metadata(), origin=snap.m.origin)
        else:
            print("No CSV file found in the zip archive.")

    tb = tb.format(["location_name", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
