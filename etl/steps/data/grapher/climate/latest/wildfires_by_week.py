"""Load a garden dataset and create a grapher dataset."""


import owid.catalog.processing as pr
import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("weekly_wildfires")
    tb = ds_garden["weekly_wildfires"].reset_index()

    #
    # Process data.
    #
    tb["date"] = pd.to_datetime(tb["date"])
    tb["year"] = tb["date"].dt.year
    tb["week_of_year"] = ((tb["date"].dt.dayofyear - 1) // 7) + 1

    #
    # Process data.
    #
    tb = tb.drop(columns=["date"], errors="raise")

    # Create an indicator for each year (week becomes the index)
    tb_pivot_area_cum = process_data(tb, "area_ha_cumulative", "Cumulative area burnt by wildfires in ")
    tb_pivot_area_share_cum = process_data(
        tb, "share_area_ha_cumulative", "Cumulative share of total area burnt by wildfires in "
    )
    tb_pivot_co2_cum = process_data(tb, "co2_cumulative", "Cumulative CO₂ emissions released by wildfires in ")
    tb_pivot_pm25_cum = process_data(tb, "pm2_5_cumulative", "Cumulative PM2.5 emissions released by wildfires in ")

    tb_pivot_area = process_data(tb, "area_ha", "Area burnt by wildfires in ")
    tb_pivot_events = process_data(tb, "area_ha", "Number of wildfires in ")

    tb_pivot_area_share = process_data(tb, "share_area_ha", "Share of total area burnt by wildfires in ")
    tb_pivot_co2 = process_data(tb, "co2", "CO₂ emissions released by wildfires in ")
    tb_pivot_pm25 = process_data(tb, "pm2_5", "PM2.5 emissions released by wildfires in ")

    tb_pivot_all = pr.concat(
        [
            tb_pivot_area,
            tb_pivot_co2,
            tb_pivot_pm25,
            tb_pivot_area_share,
            tb_pivot_area_cum,
            tb_pivot_co2_cum,
            tb_pivot_pm25_cum,
            tb_pivot_area_share_cum,
            tb_pivot_events,
        ],
        axis=1,
    )

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb_pivot_all], default_metadata=ds_garden.metadata)
    ds_grapher.metadata.title = "Seasonal wildfire trends by week"
    ds_grapher.save()


def process_data(tb, column_name, title):
    """
    This function processes the input Table to create a pivot table based on the specified column.
    It also sets metadata for each column in the pivot table.

    """
    # Create a pivot table with 'country' and 'week_of_year' as index, 'year' as columns, and 'column_name' as values
    tb_pivot = (
        tb[["country", "year", "week_of_year", column_name]]
        .pivot(index=["country", "week_of_year"], columns="year", values=column_name)
        .reset_index()
    )

    # Rename the 'week_of_year' column to 'year'
    tb_pivot = tb_pivot.rename(columns={"week_of_year": "year"})

    # Set 'country' and 'year' as the index of the DataFrame
    tb_pivot = tb_pivot.set_index(["country", "year"])

    # Drop columns where all values are NaN
    tb_pivot = tb_pivot.dropna(axis=1, how="all")

    # Rename columns to include the first part of 'column_name'
    tb_pivot.columns = [str(col) + "_" + column_name for col in tb_pivot.columns]

    # Set metadata for each column
    for column in tb_pivot.columns:
        tb_pivot[column].metadata.title = title + " in " + str(column[:4])
        tb_pivot[column].metadata.presentation.title_public = str(column[:4])
        tb_pivot[column].metadata.display = {}
        tb_pivot[column].metadata.display["name"] = str(column[:4])

    return tb_pivot
