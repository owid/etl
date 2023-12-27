"""Load a snapshot and create a meadow dataset."""
import numpy as np
import xarray as xr
from owid import catalog
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("surface_temperature.nc")

    # Load data from snapshot.
    ds = xr.open_dataset(snap.path)
    #
    # Process data.
    #
    # The latest 3 months in this dataset are made available through ERA5T, which is slightly different to ERA5. In the downloaded file, an extra dimenions ‘expver’ indicates which data is ERA5 (expver = 1) and which is ERA5T (expver = 5).
    # If a value is missing in the first dataset, it is filled with the value from the second dataset.
    ERA5_combine = ds.sel(expver=1).combine_first(ds.sel(expver=5))

    # Select the 't2m' variable from the combined dataset and assign it to 'da'.
    da = ERA5_combine["t2m"]

    # Convert the temperature values from Kelvin to Celsius by subtracting 273.15.
    da_degc = da - 273.15

    # import geopandas as gpd
    # import rasterio
    # import rioxarray
    # from shapely.geometry import mapping
    # # read shapefile

    # shapefile = gpd.read_file(
    #     "/Users/veronikasamborska/Downloads/world-administrative-boundaries/world-administrative-boundaries.shp"
    # )

    # temp_country = {}
    # small_countries = []
    # for i in range(shapefile.shape[0]):
    #     data = shapefile[shapefile.index == i]
    #     da_degc.rio.write_crs("epsg:4326", inplace=True)
    #     try:
    #         clip = da_degc.rio.clip(data.geometry.apply(mapping), data.crs)
    #         weights = np.cos(np.deg2rad(clip.latitude))
    #         weights.name = "weights"
    #         clim_month_weighted = clip.weighted(weights)
    #         country_weighted_mean = clim_month_weighted.mean(dim=["longitude", "latitude"]).values
    #         temp_country[shapefile.iloc[i]["name"]] = country_weighted_mean
    #     except:
    #         small_countries.append(shapefile.iloc[i]["name"])
    # import datetime

    # import pandas as pd

    # start_time = datetime.datetime(1950, 1, 1, 0, 0, 0)
    # end_time = datetime.datetime(2023, 12, 1, 0, 0, 0)
    # idx = pd.date_range(start_time, end_time, freq="1M")

    # # df of temperatures for each country
    # df_temp = pd.DataFrame(temp_country)
    # df_temp.index = idx

    # Create a new table and ensure all columns are snake-case and add relevant metadata.
    tb = Table(df, short_name=paths.short_name, underscore=True)
    tb["t2m"].metadata.origins = [snap.metadata.origin]
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()
