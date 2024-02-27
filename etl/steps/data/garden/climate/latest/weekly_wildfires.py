"""Load a meadow dataset and create a garden dataset."""

import pandas as pd

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania", "World"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("weekly_wildfires")

    # Read table from meadow dataset.
    tb = ds_meadow["weekly_wildfires"].reset_index()

    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    tb_pivot = tb.pivot(
        index=["country", "month_day", "year"], columns="indicator", values="value", join_column_levels_with="_"
    )
    cols_to_keep = [
        "area_ha",
        "area_ha_cumulative",
        "events",
        "events_cumulative",
        "PM2.5",
        "PM2.5_cumulative",
        "CO2",
        "CO2_cumulative",
    ]

    tb_pivot = tb_pivot[cols_to_keep + ["country", "month_day", "year"]]

    # Create a date column
    tb_pivot["date"] = pd.to_datetime(tb_pivot["year"].astype(str) + "-" + tb_pivot["month_day"].astype(str))
    tb_pivot = tb_pivot.drop(columns=["year", "month_day"])
    aggregations = {agg: "sum" for agg in cols_to_keep}
    # Add region aggregates.
    tb_pivot = geo.add_regions_to_table(
        tb_pivot,
        aggregations=aggregations,
        regions=REGIONS,
        ds_regions=ds_regions,
        min_num_values_per_year=1,
        year_col="date",
    )

    tb_pivot = tb_pivot.set_index(["country", "date"], verify_integrity=True)
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_pivot], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
