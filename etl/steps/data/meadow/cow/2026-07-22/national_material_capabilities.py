"""Load a snapshot and create a meadow dataset."""

import io
import zipfile

import owid.catalog.processing as pr
import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot. It is a nested archive: NMCv7.zip -> NMCv7/NMC-v7-abridged.zip -> NMC-*-abridged.csv.
    snap = paths.load_snapshot("national_material_capabilities.zip")

    # Extract the abridged CSV from the zip-in-zip. Missing values are coded as -9.
    with zipfile.ZipFile(snap.path, "r") as outer:
        inner_name = next(name for name in outer.namelist() if name.endswith("-abridged.zip"))
        with outer.open(inner_name) as inner_file:
            with zipfile.ZipFile(io.BytesIO(inner_file.read())) as inner:
                csv_name = next(name for name in inner.namelist() if name.endswith("-abridged.csv"))
                with inner.open(csv_name) as f:
                    df = pd.read_csv(f, na_values=[-9])

    # Wrap the dataframe as a Table carrying the snapshot's origin metadata.
    tb = pr.read_from_df(data=df, metadata=snap.to_table_metadata(), origin=snap.metadata.origin, underscore=True)

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["stateabb", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
