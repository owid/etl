"""Load a snapshot and create a meadow dataset."""
import os
import tempfile
import zipfile

import dask
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import xarray as xr
from dask.diagnostics import ProgressBar
from owid.catalog import Table
from rasterio.features import rasterize
from shapely.geometry import mapping
from structlog import get_logger
from tqdm import tqdm

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Initialize logger.
log = get_logger()


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("surface_land_temperature.zip")
    file_name = "data.nc"

    # Create a temporary directory to extract the file to
    with tempfile.TemporaryDirectory() as temp_dir:
        # Open the ZIP file
        with zipfile.ZipFile(snap.path, "r") as zip_ref:
            # Extract the file
            zip_ref.extract(file_name, path=temp_dir)

        # Construct the path to the extracted file
        _file = os.path.join(temp_dir, file_name)

        # Open the file with xarray
        ds = xr.open_dataset(_file)

    # The latest 3 months in this dataset are made available through ERA5T, which is slightly different to ERA5. In the downloaded file, an extra dimenions ‘expver’ indicates which data is ERA5 (expver = 1) and which is ERA5T (expver = 5).
    # If a value is missing in the first dataset, it is filled with the value from the second dataset.
    ERA5_combine = ds.sel(expver=1).combine_first(ds.sel(expver=5))

    # Select the 't2m' variable from the combined dataset and assign it to 'da'.
    da = ERA5_combine["t2m"]

    # Convert the temperature values from Kelvin to Celsius by subtracting 273.15.
    da = da - 273.15

    # Read the shapefile to extract country informaiton
    snap_geo = paths.load_snapshot("world_bank.zip")
    shapefile_name = "WB_countries_Admin0_10m/WB_countries_Admin0_10m.shp"

    # Check if the shapefile exists in the ZIP archive
    with zipfile.ZipFile(snap_geo.path, "r"):
        # Construct the correct path for Geopandas
        file_path = f"zip://{snap_geo.path}!/{shapefile_name}"

        # Read the shapefile directly from the ZIP archive
        shapefile = gpd.read_file(file_path)
        shapefile = shapefile[["geometry", "WB_NAME"]]

    #
    # Process data.
    #

    # Initialize an empty dictionary to store the country-wise average temperature.
    temp_country = {}

    # Initialize a list to keep track of small countries where temperature data extraction fails.
    small_countries = []

    # Assuming shapefile is your GeoDataFrame with country boundaries
    # Ensure CRS match
    shapefile = shapefile.to_crs(da.rio.crs)

    # Rasterize country shapes to match the temperature data grid
    shapes = [(geom, value) for geom, value in zip(shapefile.geometry, range(len(shapefile)))]

    # Create a template DataArray to hold country indices
    country_indices = xr.full_like(da.isel(time=0, drop=True), fill_value=-1, dtype=np.int32)

    # Rasterize using the bounds and resolution of the temperature data
    transform = rasterio.transform.from_bounds(*da.rio.bounds(), da.rio.width, da.rio.height)
    rasterized_countries = rasterize(
        shapes, out_shape=country_indices.shape, fill=-1, transform=transform, dtype=np.int32
    )

    country_indices.values = rasterized_countries

    # Pre-compute weights based on latitude for weighted mean calculation
    weights = np.cos(np.deg2rad(da.latitude))
    weights.name = "weights"

    # Initialize dictionary for country-wise average temperature
    temp_country = {}

    # Use Dask's parallel processing capabilities
    with ProgressBar(), dask.config.set(scheduler="threads"):
        for i, country_name in enumerate(shapefile["WB_NAME"]):
            # Select country's data using the rasterized country index
            country_da = da.where(country_indices == i, drop=True)

            # If no data for country, skip
            if country_da.size == 0:
                continue

            # Calculate weighted mean temperature for the country
            clim_month_weighted = country_da.weighted(weights)
            country_weighted_mean = clim_month_weighted.mean(dim=["longitude", "latitude"]).compute()
            print(country_weighted_mean)
            # Store the result
            temp_country[country_name] = country_weighted_mean

    # Add Global mean temperature
    weights = np.cos(np.deg2rad(da.latitude))
    weights.name = "weights"
    clim_month_weighted = da.weighted(weights)
    global_mean = clim_month_weighted.median(["longitude", "latitude"])
    temp_country["Global"] = global_mean

    # Define the start and end dates
    start_time = da["time"].min().dt.date.astype(str).item()
    end_time = da["time"].max().dt.date.astype(str).item()

    # Generate a date range from start_time to end_time with monthly frequency
    month_starts = pd.date_range(start=start_time, end=end_time, freq="MS")

    # month_starts is a DateTimeIndex object; you can convert it to a list if needed
    month_starts_list = month_starts.tolist()

    # df of temperatures for each country
    df_temp = pd.DataFrame(temp_country)
    df_temp["time"] = month_starts_list

    melted_df = df_temp.melt(id_vars=["time"], var_name="country", value_name="temperature_2m")

    # Create a new table and ensure all columns are snake-case and add relevant metadata.
    tb = Table(melted_df, short_name=paths.short_name, underscore=True)
    tb = tb.set_index(["time", "country"], verify_integrity=True)

    tb["temperature_2m"].metadata.origins = [snap.metadata.origin]
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()
