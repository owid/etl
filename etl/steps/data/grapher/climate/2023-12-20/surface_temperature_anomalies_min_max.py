"""Load a garden dataset and create a grapher dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("surface_temperature")
    tb = ds_garden["surface_temperature"].reset_index()
    tb["year"] = tb["time"].astype(str).str[0:4]
    tb["year"] = tb["year"].astype(int)
    tb["month"] = tb["time"].astype(str).str[5:7]
    origin = tb["temperature_2m"].metadata.origins

    #
    # Process data.
    #
    tb = tb.drop(columns=["time"], errors="raise")

    # Filter the DataFrame to include only years less than 2023
    filtered_tb = tb[tb["year"] < 2023]

    # Group by country and month, and then calculate the maximum temperature anomaly for each group
    max_temp_anomaly = filtered_tb.groupby(["country", "month"])["temperature_anomaly"].max().reset_index()
    max_temp_anomaly.rename(columns={"temperature_anomaly": "upper_bound_anomaly"}, inplace=True)

    # Group by country and month, and then calculate the minimum temperature anomaly for each group
    min_temp_anomaly = filtered_tb.groupby(["country", "month"])["temperature_anomaly"].min().reset_index()
    min_temp_anomaly.rename(columns={"temperature_anomaly": "lower_bound_anomaly"}, inplace=True)
    tb = pr.merge(max_temp_anomaly, min_temp_anomaly, on=["country", "month"])

    for col in ["upper_bound_anomaly", "lower_bound_anomaly"]:
        tb[col].origins = origin

    tb = tb.rename(columns={"month": "year"})
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb], default_metadata=ds_garden.metadata)
    ds_grapher.metadata.title = "Monthly surface temperature anomalies by country"
    ds_grapher.save()
