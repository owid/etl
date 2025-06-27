"""Load a snapshot and create a meadow dataset."""

import geopandas as gpd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("wb_admin_boundaries.zip")

    # Load data from snapshot.
    with snap.open_archive():
        tb = snap.read_from_archive(
            filename="WB_GAD_ADM0.shp",
            read_function=gpd.read_file,
        )

    #
    # Process data.
    #
    # Type
    tb["geometry"] = tb["geometry"].astype("string")

    # Improve tables format.
    tb = tb.rename(columns={"NAM_0": "name"})
    tables = [tb.format(["name", "gaul_0"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
