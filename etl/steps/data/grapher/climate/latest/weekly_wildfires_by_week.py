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

    # Transpose the DataFrame to have a country column, a column identifying the measure, and year columns
    tb_pivot = tb[["country", "year", "week_of_year", "area_ha_cumulative"]].pivot(
        index=["country", "week_of_year"], columns="year", values="area_ha_cumulative", join_column_levels_with="_"
    )
    tb_pivot = tb_pivot.rename(columns={"week_of_year": "year"})
    tb_pivot = tb_pivot.set_index(["country", "year"])
    tb_pivot = tb_pivot.dropna(axis=1, how="all")
    tb_pivot.columns = [str(col) for col in tb_pivot.columns]

    for column in tb_pivot.columns:
        tb_pivot[column].metadata.title = "Cumulative area burnt in " + str(column)
        tb_pivot[column].metadata.presentation.title_public = str(column)
        tb_pivot[column].metadata.display = {}
        tb_pivot[column].metadata.display["name"] = str(column)

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb_pivot], default_metadata=ds_garden.metadata)
    ds_grapher.metadata.title = "Cumulative area burnt by wildfires each year by week"
    ds_grapher.save()
