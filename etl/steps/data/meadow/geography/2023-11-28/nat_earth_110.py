"""Load a snapshot and create a meadow dataset."""

import tempfile
from pathlib import Path

import geopandas as gpd
from owid.catalog import Table
from owid.catalog.tables import _add_table_and_variables_metadata_to_table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("nat_earth_110.zip")

    # Load data from snapshot.
    with tempfile.TemporaryDirectory() as tmpdir:
        snap.extract(tmpdir)
        gdf = gpd.read_file(Path(tmpdir) / "ne_10m_admin_0_countries.shp")
        tb = _add_table_and_variables_metadata_to_table(
            table=Table(gdf),
            metadata=snap.to_table_metadata(),
            origin=snap.metadata.origin,
        )

    #
    # Process data.
    #
    tb = tb.astype({"geometry": str})
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["name"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
