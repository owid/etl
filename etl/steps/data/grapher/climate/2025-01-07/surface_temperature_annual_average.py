"""Load a garden dataset and create a grapher dataset."""

import numpy as np
import owid.catalog.processing as pr

from etl.catalog_helpers import last_date_accessed
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("surface_temperature")
    tb = ds_garden["surface_temperature"].reset_index()

    #
    # Process data.
    #

    # Get the year
    tb["year"] = tb["time"].astype(str).str[0:4]

    # Group by year and calculate the mean of the specified columns
    tb_annual_average = (
        tb.groupby(["year", "country"])
        .agg(
            {
                "temperature_2m": "mean",
                "temperature_anomaly": "mean",
                "anomaly_below_0": "mean",
                "anomaly_above_0": "mean",
            }
        )
        .reset_index()
    )

    # Convert the 'year' column to integer type
    tb_annual_average["year"] = tb_annual_average["year"].astype(int)

    # Create a new column for the decade
    tb_annual_average["decade"] = (tb_annual_average["year"] // 10) * 10

    # Group by decade and country, then calculate the mean for specified columns
    tb_decadal_average = (
        tb_annual_average.groupby(["decade", "country"])[["temperature_anomaly", "temperature_2m"]].mean().reset_index()
    )
    # Set the decadal values for 2020 to NaN
    tb_decadal_average.loc[tb_decadal_average["decade"] == 2020, ["temperature_anomaly", "temperature_2m"]] = np.nan
    # Merge the decadal average Table with the original Table
    combined = pr.merge(
        tb_annual_average, tb_decadal_average, on=["decade", "country"], how="left", suffixes=("", "_decadal")
    )

    # Replace the decadal values with NaN for all years except the start of each decade
    combined.loc[combined["year"] % 10 != 0, ["temperature_anomaly_decadal", "temperature_2m_decadal"]] = np.nan
    combined = combined.drop(columns=["decade"])
    # Remove the latest as it's often not representation of the full year
    latest_year = combined["year"].max()
    combined = combined[combined["year"] != latest_year]

    # Set decadal values to NaN for non-decadal years
    combined.loc[combined["year"] % 10 != 0, ["temperature_anomaly_decadal", "temperature_2m_decadal"]] = np.nan
    combined = combined.set_index(["year", "country"], verify_integrity=True)

    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = paths.create_dataset(
        tables=[combined],
        default_metadata=ds_garden.metadata,
        check_variables_metadata=True,
        yaml_params={"date_accessed": last_date_accessed(combined)},
    )

    ds_grapher.save()
