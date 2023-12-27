"""Load a garden dataset and create a grapher dataset."""

import pandas as pd

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
    #
    # Process data.
    #
    tb_global = tb[tb["country"] == "World"]
    tb_anomalies = tb_global[["year", "month", "anomaly_below_0", "anomaly_above_0"]]
    tb_anomalies = tb_anomalies.rename(
        columns={"anomaly_below_0": "Below the average", "anomaly_above_0": "Above the average"}
    )

    df_melted = tb_anomalies.melt(id_vars=["year", "month"], value_vars=["Below the average", "Above the average"])
    df_melted.rename(columns={"variable": "country"}, inplace=True)
    month_map = {
        1: "January",
        2: "February",
        3: "March",
        4: "April",
        5: "May",
        6: "June",
        7: "July",
        8: "August",
        9: "September",
        10: "October",
        11: "November",
        12: "December",
    }
    df_melted["month"] = df_melted["month"].map(month_map)

    df_melted = df_melted.rename(columns={"value": "temperature_anomaly"})
    df_pivot = df_melted.pivot_table(
        index=["country", "year"], columns="month", values="temperature_anomaly"
    ).reset_index()
    df_pivot = df_pivot.set_index(["country", "year"])
    df_pivot["annual"] = df_pivot.mean(axis=1)
    df_pivot["annual"].metadata.title = "Annual surface air temperature anomalies"
    # Add metadata.
    for column in df_pivot.columns:
        df_pivot[column].metadata.origins = tb["temperature_anomaly"].metadata.origins
        df_pivot[column].metadata.description_short = tb["temperature_anomaly"].metadata.description_short
        df_pivot[column].metadata.unit = "°C"
        df_pivot[column].metadata.short_unit = "°C"
        df_pivot[column].metadata.display = {}
        df_pivot[column].metadata.display["numDecimalPlaces"] = 1

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[df_pivot], default_metadata=ds_garden.metadata)
    ds_grapher.metadata.title = "Monthly global temperature anomalies since 1950"
    ds_grapher.save()
