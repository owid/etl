"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("fra_forest_extent.xlsx")

    # Load data from snapshot.
    tb = snap.read(sheet_name="1a_Forest", skiprows=4)
    tb.columns = [
        "iso3",
        "country",
        "total_num_countries",
        "region_forest_area_2020",
        "state_num_countries",
        "state_forest_area_2020",
        "state_pct_total_forest_area",
        "forest_area_subtotal_2020",
        "trend_num_countries",
        "trend_forest_area_2020",
        "trend_pct_total_forest_area",
        "trend_datapoints",
        "forest_area_1990",
        "forest_area_2000",
        "forest_area_2010",
        "forest_area_2015",
        "forest_area_2020",
        "annual_change_1990_2000",
        "annual_change_1990_2010",
        "annual_change_1990_2015",
        "annual_change_1990_2020",
        "annual_change_2000_2010",
        "annual_change_2000_2015",
        "annual_change_2000_2020",
        "annual_change_2010_2015",
        "annual_change_2010_2020",
        "annual_change_2015_2020",
        "change_rate_1990_2000",
        "change_rate_1990_2010",
        "change_rate_1990_2015",
        "change_rate_1990_2020",
        "change_rate_2000_2010",
        "change_rate_2000_2015",
        "change_rate_2000_2020",
        "change_rate_2010_2015",
        "change_rate_2010_2020",
        "change_rate_2015_2020",
    ]
    tb = tb[
        ["country", "forest_area_1990", "forest_area_2000", "forest_area_2010", "forest_area_2015", "forest_area_2020"]
    ]
    tb_long = tb.melt(
        id_vars=["country"],
        value_vars=[
            "forest_area_1990",
            "forest_area_2000",
            "forest_area_2010",
            "forest_area_2015",
            "forest_area_2020",
        ],
        var_name="year",
        value_name="forest_area",
    )

    # Extract just the year (from 'forest_area_1990' → '1990')
    tb_long["year"] = tb_long["year"].str.extract("(\d+)")
    tb_long["year"] = tb_long["year"].astype(int)
    #
    # Process data.
    #
    # Improve tables format.
    tb_long = remove_region_duplicates(
        tb_long,
    )
    tables = [tb_long.format(["country", "year"], short_name="fra_forest_area")]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()


def remove_region_duplicates(tb_long: Table) -> Table:
    regions = ["Africa", "Asia", "Europe", "North and Central America", "Oceania", "South America"]

    # Sort the data to ensure "last" is meaningful (optional but safer)
    tb_long = tb_long.sort_values(by=["country", "year"])

    # Create a mask to keep all but the last occurrence of each region
    mask = ~tb_long[tb_long["country"].isin(regions)].duplicated(subset=["country"], keep="last")

    # Combine that with the rest of the data
    tb_long = pd.concat(
        [tb_long[~tb_long["country"].isin(regions)], tb_long[tb_long["country"].isin(regions)][mask]], ignore_index=True
    )

    return tb_long
