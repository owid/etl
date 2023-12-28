"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import owid.catalog.processing as pr
import pandas as pd

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Regions for which aggregates will be created.
REGIONS = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania", "World"]
LATEST_DATE = "2024-01-01"


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("surface_temperature")
    tb = ds_meadow["surface_temperature"].reset_index()

    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    # Load income groups dataset.
    ds_income_groups = paths.load_dataset("income_groups")
    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    # Rename time column to year for aggregation purposes
    tb = tb.rename(columns={"time": "year"})

    # Add region aggregates.
    tb = geo.add_regions_to_table(
        tb,
        aggregations={"temperature_2m": "mean"},
        regions=REGIONS,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        min_num_values_per_year=1,
    )

    # Rename back to time
    tb = tb.rename(columns={"year": "time"})

    # Set time as index
    tb = tb.set_index("time")

    # Extract month and year
    tb["year"] = tb.index.year
    tb["month"] = tb.index.month

    # Filter data for the 1991-2020 period
    reference_period = tb["1950-01-01":LATEST_DATE]

    # Calculate mean temperature for each month in the reference period
    monthly_climatology = reference_period.groupby(["country", "month"])["temperature_2m"].mean()
    monthly_climatology = monthly_climatology.reset_index()
    monthly_climatology = monthly_climatology.rename(columns={"temperature_2m": "mean_temp"})
    tb = tb.reset_index()

    # Ensure that the reference mean DataFrame has a name for the mean column, e.g., 'mean_temp'
    merged_df = pr.merge(tb, monthly_climatology, on=["country", "month"]).copy_metadata(from_table=tb)

    # Calculate the anomalies
    merged_df["temperature_anomaly"] = merged_df["temperature_2m"] - merged_df["mean_temp"]
    merged_df = merged_df.drop(columns=["mean_temp"])

    # Initialize the new columns with default values (you can choose NaN, 0, or any other placeholder)
    merged_df["anomaly_below_0"] = np.NaN
    merged_df["anomaly_above_0"] = np.NaN

    # Function to assign anomalies to the new columns
    def split_anomalies(row):
        if row["temperature_anomaly"] < 0:
            return pd.Series([row["temperature_anomaly"], np.NaN])
        else:
            return pd.Series([np.NaN, row["temperature_anomaly"]])

    # Apply the function to split anomalies
    merged_df[["anomaly_below_0", "anomaly_above_0"]] = merged_df.apply(split_anomalies, axis=1)

    # Add metadata to the new columns
    for col in ["anomaly_below_0", "anomaly_above_0", "month", "year"]:
        merged_df[col].metadata.origins = merged_df["temperature_anomaly"].metadata.origins

    merged_df = merged_df.set_index(["country", "time"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[merged_df], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )
    ds_garden.save()
