"""Load a snapshot and create a meadow dataset."""

import os
import tempfile
import zipfile

import geopandas as gpd
import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("anthromes_input.zip")
    # Get the table from the shapefile - metadata currently gets lost along the way
    tb = get_table_from_shp_file(snap)

    # Process data.
    #
    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["id", "regn_nm"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def get_table_from_shp_file(snap: Snapshot) -> Table:
    with tempfile.TemporaryDirectory() as temp_dir:
        with zipfile.ZipFile(snap.path, "r") as zip_ref:
            zip_ref.extractall(temp_dir)

        # List all files in the temporary directory
        extracted_dir = os.listdir(temp_dir)
        extracted_files = os.listdir(os.path.join(temp_dir, extracted_dir[0]))

        # Keep only the .shp file and read it into a GeoDataFrame
        shp_file = None
        for file_name in extracted_files:
            if file_name.endswith(".shp"):
                shp_file = os.path.join(temp_dir, extracted_dir[0], file_name)
        gdf = gpd.read_file(shp_file)
        gdf = gdf.drop(columns=["geometry"])
        gdf = pr.read_df(gdf, metadata=snap.to_table_metadata(), origin=snap.metadata.origin)
        return gdf
