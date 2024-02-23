"""Script to create a snapshot of the monthly averaged surface temperature data from 1950 to present from the Copernicus Climate Change Service.

   The script assumes that the data is available on the CDS API.
   Instructions on how to access the API on a Mac are here: https://confluence.ecmwf.int/display/CKB/How+to+install+and+use+CDS+API+on+macOS

   More information on how to access the data is here: https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-single-levels-monthly-means?tab=overview

   The data is downloaded as a NetCDF file. Tutorials for using the Copernicus API are here and work with the NETCDF format are here: https://ecmwf-projects.github.io/copernicus-training-c3s/cds-tutorial.html
   """

import tempfile
import zipfile
from pathlib import Path

# CDS API
import cdsapi
import click
import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr
from owid.datautils.io import df_to_file
from rioxarray.exceptions import NoDataInBounds
from shapely.geometry import mapping
from structlog import get_logger
from tqdm import tqdm

from etl.snapshot import Snapshot

log = get_logger()

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"climate/{SNAPSHOT_VERSION}/surface_temperature.csv")
    # Save data as a compressed temporary file.
    with tempfile.TemporaryDirectory() as temp_dir:
        c = cdsapi.Client()
        output_file = Path(temp_dir) / "era5_monthly_t2m_eur.nc"

        c.retrieve(
            "reanalysis-era5-single-levels-monthly-means",
            {
                "product_type": "monthly_averaged_reanalysis",
                "variable": "2m_temperature",
                "year": [str(year) for year in range(1950, 2024)],
                "month": ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"],
                "time": "00:00",
                "area": [90, -180, -90, 180],
                "format": "netcdf",
            },
            output_file,
        )
        ds = xr.open_dataset(output_file)
    #
    # Process data. This step is done here to avoid memory issues caused by geographical cropping in meadow step.
    #

    # The latest 3 months in this dataset are made available through ERA5T, which is slightly different to ERA5. In the downloaded file, an extra dimenions ‘expver’ indicates which data is ERA5 (expver = 1) and which is ERA5T (expver = 5).
    # If a value is missing in the first dataset, it is filled with the value from the second dataset.
    ERA5_combine = ds.sel(expver=1).combine_first(ds.sel(expver=5))

    # Select the 't2m' variable from the combined dataset and assign it to 'da'.
    da = ERA5_combine["t2m"]

    # Convert the temperature values from Kelvin to Celsius by subtracting 273.15.
    da = da - 273.15

    # Read the shapefile to extract country informaiton
    snap_geo_path = Snapshot("countries/2023-12-27/world_bank.zip").path
    shapefile_name = "WB_countries_Admin0_10m/WB_countries_Admin0_10m.shp"

    # Check if the shapefile exists in the ZIP archive
    with zipfile.ZipFile(snap_geo_path, "r"):
        # Construct the correct path for Geopandas
        file_path = f"zip://{snap_geo_path}!/{shapefile_name}"

        # Read the shapefile directly from the ZIP archive
        shapefile = gpd.read_file(file_path)
        shapefile = shapefile[["geometry", "WB_NAME"]]

    # Initialize an empty dictionary to store the country-wise average temperature.
    temp_country = {}

    # Initialize a list to keep track of small countries where temperature data extraction fails.
    small_countries = []

    # Iterate over each row in the shapefile data.
    for i in tqdm(range(shapefile.shape[0])):
        # Extract the data for the current row.
        data = shapefile[shapefile.index == i]
        country_name = shapefile.iloc[i]["WB_NAME"]

        # Set the coordinate reference system for the temperature data to EPSG 4326.
        da.rio.write_crs("epsg:4326", inplace=True)

        try:
            # Clip the temperature data to the current country's shape.
            clip = da.rio.clip(data.geometry.apply(mapping), data.crs)

            # Calculate weights based on latitude to account for area distortion in latitude-longitude grids.
            weights = np.cos(np.deg2rad(clip.latitude))
            weights.name = "weights"

            # Apply the weights to the clipped temperature data.
            clim_month_weighted = clip.weighted(weights)

            # Calculate the weighted mean temperature for the country.
            country_weighted_mean = clim_month_weighted.mean(dim=["longitude", "latitude"]).values

            # Store the calculated mean temperature in the dictionary with the country's name as the key.
            temp_country[country_name] = country_weighted_mean

        except NoDataInBounds:
            log.info(
                f"No data was found in the specified bounds for {country_name}."
            )  # If an error occurs (usually due to small size of the country), add the country's name to the small_countries list.  # If an error occurs (usually due to small size of the country), add the country's name to the small_countries list.
            small_countries.append(shapefile.iloc[i]["WB_NAME"])

    # Log information about countries for which temperature data could not be extracted.
    log.info(
        f"It wasn't possible to extract temperature data for {len(small_countries)} small countries as they are too small for the resolution of the Copernicus data."
    )

    # Add Global mean temperature
    weights = np.cos(np.deg2rad(da.latitude))
    weights.name = "weights"
    clim_month_weighted = da.weighted(weights)
    global_mean = clim_month_weighted.mean(["longitude", "latitude"])
    temp_country["World"] = global_mean

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
    df_to_file(melted_df, file_path=snap.path)

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
