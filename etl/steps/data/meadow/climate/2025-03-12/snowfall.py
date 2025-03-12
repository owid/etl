"""Load a snapshot and create a meadow dataset."""

import tempfile
import zipfile

import geopandas as gpd
import numpy as np
import pandas as pd
import pyproj
import xarray as xr
from owid.catalog import Table
from rioxarray.exceptions import NoDataInBounds, OneDimensionalRaster
from shapely.geometry import mapping
from structlog import get_logger
from tqdm import tqdm

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Initialize logger.
log = get_logger()


def _load_data_array(snap: Snapshot) -> xr.DataArray:
    log.info("load_data_array.start")
    with zipfile.ZipFile(snap.path, "r") as zip_file:
        for file_info in zip_file.infolist():
            if file_info.filename.endswith((".grb", ".grib")):  # Filter GRIB files
                with zip_file.open(file_info) as file:
                    file_content = file.read()

                # Write to a temporary file
                with tempfile.NamedTemporaryFile(delete=True, suffix=".grib") as tmp_file:
                    tmp_file.write(file_content)
                    tmp_file.flush()  # Ensure all data is written

                    # Load the GRIB file using xarray and cfgrib
                    da = xr.open_dataset(tmp_file.name, engine="cfgrib").load()

    print(da)
    # Convert temperature from Kelvin to Celsius.
    da = da["t2m"] - 273.15

    # Set the coordinate reference system for the temperature data to EPSG 4326.
    da = da.rio.write_crs("epsg:4326")

    return da


def _load_shapefile(file_path: str) -> gpd.GeoDataFrame:
    log.info("load_shapefile.start")
    shapefile = gpd.read_file(file_path)
    return shapefile[["geometry", "WB_NAME"]]


def run(dest_dir: str) -> None:
    # Activates the usage of the global context. Using this option can enhance the performance
    # of initializing objects in single-threaded applications.
    pyproj.set_use_global_context(True)  # type: ignore

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("snowfall.zip")

    # Read surface temperature data from snapshot
    da = _load_data_array(snap)

    # Read the shapefile to extract country information
    snap_geo = paths.load_snapshot("world_bank.zip")
    shapefile_name = "WB_countries_Admin0_10m/WB_countries_Admin0_10m.shp"

    # Check if the shapefile exists in the ZIP archive
    with zipfile.ZipFile(snap_geo.path, "r"):
        # Construct the correct path for Geopandas
        file_path = f"zip://{snap_geo.path}!/{shapefile_name}"

        # Read the shapefile directly from the ZIP archive
        shapefile = _load_shapefile(file_path)

    #
    # Process data.
    #

    # Initialize an empty dictionary to store the country-wise average temperature.
    temp_country = {}

    # Add Global mean temperature
    weights = np.cos(np.deg2rad(da.latitude))
    weights.name = "weights"
    clim_month_weighted = da.weighted(weights)
    global_mean = clim_month_weighted.mean(["longitude", "latitude"])
    temp_country["World"] = global_mean

    # Initialize a list to keep track of small countries where temperature data extraction fails.
    small_countries = []

    # Iterate over each row in the shapefile data.
    for i in tqdm(range(shapefile.shape[0])):
        # Extract the data for the current row.
        geometry = shapefile.iloc[i]["geometry"]
        country_name = shapefile.iloc[i]["WB_NAME"]

        try:
            # Clip to the bounding box for the country's shape to significantly improve performance.
            xmin, ymin, xmax, ymax = geometry.bounds
            clip = da.rio.clip_box(minx=xmin, miny=ymin, maxx=xmax, maxy=ymax)

            # Clip data to the country's shape.
            # NOTE: if memory is an issue, we could use `from_disk=True` arg
            clip = clip.rio.clip([mapping(geometry)], shapefile.crs)

            # Calculate weights based on latitude to account for area distortion in latitude-longitude grids.
            weights = np.cos(np.deg2rad(clip.latitude))
            weights.name = "weights"

            # Apply the weights to the clipped temperature data.
            clim_month_weighted = clip.weighted(weights)

            # Calculate the weighted mean temperature for the country.
            country_weighted_mean = clim_month_weighted.mean(dim=["longitude", "latitude"]).values

            # Store the calculated mean temperature in the dictionary with the country's name as the key.
            temp_country[country_name] = country_weighted_mean

            # Clean up the memory
            del clip
            del weights
            del clim_month_weighted

        except (NoDataInBounds, OneDimensionalRaster):
            log.info(
                f"No data was found in the specified bounds for {country_name}."
            )  # If an error occurs (usually due to small size of the country), add the country's name to the small_countries list.  # If an error occurs (usually due to small size of the country), add the country's name to the small_countries list.
            small_countries.append(shapefile.iloc[i]["WB_NAME"])

    # Log information about countries for which temperature data could not be extracted.
    log.info(
        f"It wasn't possible to extract temperature data for {len(small_countries)} small countries as they are too small for the resolution of the Copernicus data."
    )
    # Define the start and end dates
    da["time"] = xr.DataArray(pd.to_datetime(da["time"].values), dims=da["valid_time"].dims)

    # Now you can access the 'dt' accessor
    start_time = da["time"].min().dt.date.astype(str).item()
    end_time = da["time"].max().dt.date.astype(str).item()

    # Generate a date range from start_time to end_time with monthly frequency
    month_middles = pd.date_range(start=start_time, end=end_time, freq="MS") + pd.offsets.Day(14)

    # month_starts is a DateTimeIndex object; you can convert it to a list if needed
    month_starts_list = month_middles.tolist()

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
