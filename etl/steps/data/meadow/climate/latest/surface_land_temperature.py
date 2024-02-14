"""Load a snapshot and create a meadow dataset."""
import os
import tempfile
import zipfile

import dask
import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr
from owid.catalog import Table
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

    da.rio.write_crs("epsg:4326", inplace=True)

    # Initialize an empty dictionary to store the country-wise average temperature.
    temp_country = {}

    # Initialize a list to keep track of small countries where temperature data extraction fails.
    small_countries = []

    da.rio.write_crs("epsg:4326", inplace=True)

    # Chunk the data array using Dask
    da_chunked = da.chunk({"time": 100})  # Adjust the chunk size as needed

    for i in tqdm(shapefile.index):
        # Directly access the geometry and other attributes without filtering the GeoDataFrame
        current_geometry = shapefile.at[i, "geometry"]
        country_name = shapefile.at[i, "WB_NAME"]

        try:
            # Use Dask's lazy evaluation
            with dask.config.set(scheduler="threads"):  # Use the threaded scheduler
                # Clip the temperature data to the current country's shape
                clip = da_chunked.rio.clip([current_geometry], crs=shapefile.crs).compute()

                # Calculate weights based on latitude
                weights = np.cos(np.deg2rad(clip.latitude))
                weights.name = "weights"
                clim_month_weighted = clip.weighted(weights)

                # Calculate the weighted mean temperature
                country_weighted_mean = clim_month_weighted.mean(dim=["longitude", "latitude"]).values

                # Store the result
                temp_country[country_name] = country_weighted_mean

        except Exception as e:
            # Handle errors, possibly by logging or appending the country to a list of failed attempts
            log.info(f"Error processing {country_name}: {e}")
            small_countries.append(country_name)

    # Log information about countries for which temperature data could not be extracted.
    log.info(
        f"It wasn't possible to extract temperature data for {len(small_countries)} small countries as they are too small for the resolution of the Copernicus data."
    )
    # Add Global mean temperature
    weights = np.cos(np.deg2rad(da.latitude))
    weights.name = "weights"
    clim_month_weighted = da.weighted(weights)
    global_mean = clim_month_weighted.mean(["longitude", "latitude"])
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
