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
    ds_grapher = paths.load_dataset("wildfires_by_week")
    tb = ds_grapher["weekly_wildfires"].reset_index()

    #
    # Process data.
    #
    # Extract columns related to area and events
    area_cum_columns = [col for col in tb.columns if "area_ha_cumulative" in col]

    # Sort these columns by year (assuming the year is embedded in the column name after the first underscore)
    area_columns_sorted = sorted(area_cum_columns, key=lambda x: int(x.split("_")[1]))

    # Calculate the cumulative area burned (2012-2022)
    tb["cumulative_area_burned_2012_2022"] = tb[area_columns_sorted].sum(axis=1)

    # Average cumulative area burned (2012-2022)
    tb["avg_cumulative_area_burned_2012_2022"] = tb["cumulative_area_burned_2012_2022"] / len(area_columns_sorted)

    # 4. Calculate the standard deviation of area burned (2012-2022)
    tb["std_dev_cum_area_burned_2012_2022"] = tb[area_columns_sorted].std(axis=1)

    # 5. Calculate the upper and lower bounds of the standard deviation
    tb["upper_bound_cum_area"] = tb["avg_cumulative_area_burned_2012_2022"] + tb["std_dev_cum_area_burned_2012_2022"]
    tb["lower_bound_cum_area"] = tb["avg_cumulative_area_burned_2012_2022"] - tb["std_dev_cum_area_burned_2012_2022"]
    tb = tb.drop(columns=["std_dev_cum_area_burned_2012_2022", "cumulative_area_burned_2012_2022"])

    # Set 'country' and 'year' as the index of the DataFrame
    tb = tb.set_index(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb], default_metadata=ds_grapher.metadata)
    ds_grapher.metadata.title = "Seasonal wildfire trends by week - Average and Standard Deviation"
    ds_grapher.save()
