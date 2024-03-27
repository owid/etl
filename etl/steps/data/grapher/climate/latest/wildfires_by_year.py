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
    ds_garden = paths.load_dataset("weekly_wildfires")
    tb = ds_garden["weekly_wildfires"].reset_index()

    #
    # Process data.
    #

    # Get the year
    tb["year"] = tb["date"].astype(str).str[0:4]
    tb = tb[
        [
            "country",
            "year",
            "area_ha",
            "events",
            "pm2_5",
            "co2",
            "share_area_ha",
            "area_ha_per_wildfire",
            "co2_ha_per_area",
            "pm2_5_ha_per_area",
        ]
    ]
    # Aggregate the data by year and country (ignore missing values when summing the columns)
    tb_annual_sum = tb.groupby(["country", "year"]).sum(min_count=1).reset_index()

    tb_cumulative = (
        tb[["country", "year", "area_ha", "events", "pm2_5", "co2", "share_area_ha"]]
        .groupby(["country", "year"])
        .sum(min_count=1)
        .groupby("country")
        .cumsum()
        .reset_index()
    )
    for col in ["area_ha", "events", "pm2_5", "co2", "share_area_ha"]:
        tb_cumulative = tb_cumulative.rename(columns={col: col + "_cumulative"})
    tb = pr.merge(tb_annual_sum, tb_cumulative, on=["year", "country"])
    tb = tb.set_index(["country", "year"], verify_integrity=True)

    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb], default_metadata=ds_garden.metadata)
    ds_grapher.metadata.title = "Seasonal wildfire trends by year"

    ds_grapher.save()
