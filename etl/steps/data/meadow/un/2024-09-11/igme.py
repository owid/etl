"""Load a snapshot and create a meadow dataset."""
import zipfile

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("igme.zip")

    # Load data from snapshot.
    zf = zipfile.ZipFile(snap.path)
    tb = pr.read_csv(zf.open("UN IGME 2023.csv"), low_memory=False, metadata=snap.to_table_metadata())

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])
    tb = tb.rename(columns={"Geographic area": "country", "REF_DATE": "year"}, errors="raise")
    tb = tb.format(["country", "year", "indicator", "sex", "wealth_quintile", "series_name", "regional_group"])
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
