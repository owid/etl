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
    snap = paths.load_snapshot("geoboundaries_cgaz.zip")

    # Load data from snapshot.
    with snap.open_archive():
        tb = snap.read_from_archive(
            filename="geoBoundariesCGAZ_ADM0.shp",
            read_function=gpd.read_file,
        )

    #
    # Process data.
    #
    # Type
    tb["geometry"] = tb["geometry"].astype("string")

    # Improve tables format.
    tables = [tb.format(["shapename"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
