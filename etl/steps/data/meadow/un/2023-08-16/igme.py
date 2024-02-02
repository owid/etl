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
    tb = pr.read_csv(zf.open("UN IGME 2022.csv"), low_memory=False, metadata=snap.to_table_metadata())
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = tb.underscore()
    tb = tb.rename(columns={"geographic_area": "country", "ref_date": "year"})
    tb = tb.set_index(
        ["country", "year", "indicator", "sex", "wealth_quintile", "series_name", "regional_group"],
        verify_integrity=True,
    )

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()
