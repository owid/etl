"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder
import geopandas as gpd
from owid.catalog import Table
from owid.catalog.tables import _add_table_and_variables_metadata_to_table


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
        gdf = gpd.read_file(snap.path_unarchived / "WB_GAD_ADM0.shp")
        tb = _add_table_and_variables_metadata_to_table(
            table=Table(gdf),
            metadata=snap.to_table_metadata(),
            origin=snap.metadata.origin,
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
