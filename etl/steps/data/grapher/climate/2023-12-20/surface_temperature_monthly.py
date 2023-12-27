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
    tb = tb.drop(columns=["time"])

    # Transpose the DataFrame to have a country column, a column identifying the measure, and year columns
    df_melted = tb.melt(id_vars=["country", "year", "month"], value_vars=["temperature_2m"])

    df_melted["year"] = df_melted["year"].astype(str)
    df_melted = df_melted.rename(columns={"value": "temperature_2m"})
    df_pivot = df_melted.pivot_table(index=["country", "month"], columns="year", values="temperature_2m").reset_index()
    df_pivot = df_pivot.rename(columns={"month": "year"})
    df_pivot = df_pivot.set_index(["country", "year"])
    # Add metadata.
    for column in df_pivot.columns:
        df_pivot[column].metadata.origins = tb["temperature_2m"].metadata.origins
        df_pivot[column].metadata.description_short = tb["temperature_2m"].metadata.description_short
        df_pivot[column].metadata.unit = "°C"
        df_pivot[column].metadata.short_unit = "°C"
        df_pivot[column].metadata.display = {}
        df_pivot[column].metadata.display["numDecimalPlaces"] = 1

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[df_pivot], default_metadata=ds_garden.metadata)
    ds_grapher.metadata.title = "Monthly surface temperature temperatures since 1950"
    ds_grapher.save()
