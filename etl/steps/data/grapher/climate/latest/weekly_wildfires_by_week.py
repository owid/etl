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

    tb_pivot_area = tb[["country", "year", "week_of_year", "area_ha_cumulative"]].pivot(
        index=["country", "week_of_year"], columns="year", values="area_ha_cumulative", join_column_levels_with="_"
    )
    tb_pivot_area = tb_pivot_area.rename(columns={"week_of_year": "year"})
    tb_pivot_area = tb_pivot_area.set_index(["country", "year"])
    tb_pivot_area = tb_pivot_area.dropna(axis=1, how="all")
    tb_pivot_area.columns = [str(col) + "_area" for col in tb_pivot_area.columns]

    for column in tb_pivot_area.columns:
        tb_pivot_area[column].metadata.title = "Cumulative area burnt in " + str(column[:4])
        tb_pivot_area[column].metadata.presentation.title_public = str(column[:4])
        tb_pivot_area[column].metadata.display = {}
        tb_pivot_area[column].metadata.display["name"] = str(column[:4])

    tb_pivot_co2 = tb[["country", "year", "week_of_year", "co2_cumulative"]].pivot(
        index=["country", "week_of_year"], columns="year", values="co2_cumulative", join_column_levels_with="_"
    )
    tb_pivot_co2 = tb_pivot_co2.rename(columns={"week_of_year": "year"})
    tb_pivot_co2 = tb_pivot_co2.set_index(["country", "year"])
    tb_pivot_co2 = tb_pivot_co2.dropna(axis=1, how="all")
    tb_pivot_co2.columns = [str(col) + "_co2" for col in tb_pivot_co2.columns]

    for column in tb_pivot_co2.columns:
        tb_pivot_co2[column].metadata.title = "Cumulative CO2 in " + str(column[:4])
        tb_pivot_co2[column].metadata.presentation.title_public = str(column[:4])
        tb_pivot_co2[column].metadata.display = {}
        tb_pivot_co2[column].metadata.display["name"] = str(column[:4])

    tb_pivot_pm25 = tb[["country", "year", "week_of_year", "pm2_5_cumulative"]].pivot(
        index=["country", "week_of_year"], columns="year", values="pm2_5_cumulative", join_column_levels_with="_"
    )
    tb_pivot_pm25 = tb_pivot_pm25.rename(columns={"week_of_year": "year"})
    tb_pivot_pm25 = tb_pivot_pm25.set_index(["country", "year"])
    tb_pivot_pm25 = tb_pivot_pm25.dropna(axis=1, how="all")
    tb_pivot_pm25.columns = [str(col) + "_pm25" for col in tb_pivot_pm25.columns]
    for column in tb_pivot_pm25.columns:
        tb_pivot_pm25[column].metadata.title = "Cumulative PM2.5 in " + str(column[:4])
        tb_pivot_pm25[column].metadata.presentation.title_public = str(column[:4])
        tb_pivot_pm25[column].metadata.display = {}
        tb_pivot_pm25[column].metadata.display["name"] = str(column[:4])

    tb_pivot_all = pr.concat([tb_pivot_area, tb_pivot_co2, tb_pivot_pm25], axis=1)
    print(tb_pivot_all)

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb_pivot_all], default_metadata=ds_garden.metadata)
    ds_grapher.metadata.title = "Cumulative area burnt by wildfires each year by week"
    ds_grapher.save()
