"""Load a snapshot and create a meadow dataset."""

import zipfile

import geopandas as gpd
import numpy as np
import rasterio
from owid.catalog import Table
from rasterio.features import geometry_mask
from shapely.geometry import mapping
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Initialize logger.
log = get_logger()


def _load_shapefile(snap_geo: Snapshot, shapefile_name: str, file_path: str) -> gpd.GeoDataFrame:
    with zipfile.ZipFile(snap_geo.path, "r"):
        # Construct the correct path for Geopandas
        file_path = f"zip://{snap_geo.path}!/{shapefile_name}"
    shapefile = gpd.read_file(file_path)
    return shapefile[["geometry", "WB_NAME"]]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("dominant_driver.tif")
    snap_geo = paths.load_snapshot("world_bank.zip")
    shapefile_name = "WB_countries_Admin0_10m/WB_countries_Admin0_10m.shp"

    shp = _load_shapefile(snap_geo, shapefile_name, snap_geo.path)

    with rasterio.open(snap.path) as src:
        ds = src.read(1)
        transform = src.transform
        ds_crs = src.crs
        # Get the cell size (in metres) and conver to hectares
        pixel_width = transform[0]
        pixel_height = transform[4]
        pixel_area_hectares = abs(pixel_width * pixel_height) / 10_000
        # Reproject the shapefile to the same projection as the raster, which is a more appropriate projection for calculating areas - Goode Homosline
        shp = shp.to_crs(ds_crs)
        # Iterate through each geometry in the shapefile
        results = []
        for idx, geom in shp.iterrows():
            geometry = geom.geometry
            country_name = geom["WB_NAME"]
            # Create a mask for the current geometry
            mask = geometry_mask(
                [mapping(geometry)], transform=transform, invert=True, out_shape=(src.height, src.width)
            )

            # Apply the mask to the raster data
            masked_data = ds[mask]
            # Calculate the area for each category in the mask
            unique_values, counts = np.unique(masked_data, return_counts=True)
            areas = counts * pixel_area_hectares
            # Collect the results
            for value, area in zip(unique_values, areas):
                results.append({"country": country_name, "category": value, "area_hectares": area})

    tb = Table(results)
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "category"], short_name=paths.short_name)
    for col in tb.columns:
        tb[col].metadata.origins = snap.metadata.origin

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
