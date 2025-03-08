"""Load a meadow dataset and create a garden dataset."""

import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("sea_surface_temperature")
    tb = ds_garden.read("sea_surface_temperature")

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
