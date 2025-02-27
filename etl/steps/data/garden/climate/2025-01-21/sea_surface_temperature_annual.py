"""Load a meadow dataset and create a garden dataset."""

import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Columns to select from data, and how to rename them.
COLUMNS = {
    "year": "year",
    "month": "month",
    "location": "location",
    "anomaly": "sea_temperature_anomaly",
    "lower_bound_95pct_bias_uncertainty_range": "sea_temperature_anomaly_low",
    "upper_bound_95pct_bias_uncertainty_range": "sea_temperature_anomaly_high",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("sea_surface_temperature")
    tb = ds_meadow.read("sea_surface_temperature")

    #
    # Process data.
    #
    # Extract year from date column
    tb["year"] = pd.to_datetime(tb["date"]).dt.year

    # Compute annual averages
    tb = tb.groupby(["year", "location"], as_index=False).agg(
        {
            "sea_temperature_anomaly": "mean",
            "sea_temperature_anomaly_low": "mean",
            "sea_temperature_anomaly_high": "mean",
        }
    )

    # Set an appropriate index and sort conveniently.
    tb = tb.format(["location", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the combined table.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
