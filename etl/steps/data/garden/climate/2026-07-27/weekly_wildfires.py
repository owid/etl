"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

from etl.catalog_helpers import last_date_accessed
from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Regions to aggregate.
# NOTE: "Europe" is deliberately absent. Russia accounts for around 90% of the burnt area of the European
# aggregate in every year, so a plain "Europe" line tracks the Russian fire season and can move in the opposite
# direction to the rest of the continent. It is split into two explicit aggregates instead, so that readers pick
# the one they mean.
REGIONS = {
    "North America": {},
    "South America": {},
    "Europe (incl. Russia)": {"additional_regions": ["Europe"]},
    "Europe (excl. Russia)": {"additional_regions": ["Europe"], "excluded_members": ["Russia"]},
    "Africa": {},
    "Asia": {},
    "Oceania": {},
    "European Union (27)": {},
    "World": {},
}


def sanity_check_europe_aggregates(tb: Table) -> None:
    """Check that the two European aggregates replaced the old "Europe" one, and that they reconcile with Russia."""
    countries = set(tb["country"])
    assert "Europe" not in countries, "The 'Europe' aggregate should have been replaced by the incl./excl. Russia ones."
    assert {"Europe (incl. Russia)", "Europe (excl. Russia)"} <= countries, "A European aggregate is missing."

    # Both aggregates cover the same dates, so they can be compared row by row.
    incl = tb[tb["country"] == "Europe (incl. Russia)"].set_index("date").sort_index()
    excl = tb[tb["country"] == "Europe (excl. Russia)"].set_index("date").sort_index()
    russia = tb[tb["country"] == "Russia"].set_index("date").sort_index()
    assert incl.index.equals(excl.index), "The two European aggregates cover different dates."
    assert incl.index.equals(russia.index), "Russia and the European aggregates cover different dates."

    for column in ["area_ha", "area_ha_cumulative", "events", "events_cumulative"]:
        # Excluding Russia can only lower the aggregate.
        assert (excl[column] <= incl[column]).all(), f"'{column}' is larger excluding Russia than including it."
        # The difference between the two aggregates must be exactly Russia.
        assert ((incl[column] - excl[column] - russia[column]).abs() < 1e-6).all(), (
            f"'{column}' incl. minus excl. Russia does not equal Russia."
        )


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

    # Load the FAOSTAT dataset which contains data related to area burnt
    ds_faostat = paths.load_dataset("faostat_rl")
    # Reset the index of the DataFrame
    ds_faostat = ds_faostat["faostat_rl"].reset_index()

    # Filter the DataFrame to include only rows where 'item' is 'Country area', 'element' is 'Area', and 'unit' is 'hectares'
    ds_faostat_country_area = ds_faostat[
        (ds_faostat["item"] == "Country area") & (ds_faostat["element"] == "Area") & (ds_faostat["unit"] == "hectares")
    ].reset_index()

    # Select only the 'country', 'year', and 'value' columns from the filtered DataFrame
    country_area = ds_faostat_country_area[["country", "year", "value"]]
    # Rename the 'value' column to 'total_area_ha'
    country_area = country_area.rename(columns={"value": "total_area_ha"})
    # For each country, select the row where the 'year' is the maximum (i.e., most recent) and keep only the 'country', 'year', and 'total_area_ha' columns
    area_most_recent_year = country_area.loc[country_area.groupby("country")["year"].idxmax()][
        ["country", "total_area_ha"]
    ]

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
    sanity_check_europe_aggregates(tb_pivot)

    # Merge land area data with the wildfire data
    tb = pr.merge(tb_pivot, area_most_recent_year, on=["country"], how="left")

    tb["share_area_ha"] = (tb["area_ha"] / tb["total_area_ha"]) * 100
    tb["share_area_ha_cumulative"] = (tb["area_ha_cumulative"] / tb["total_area_ha"]) * 100

    tb = tb.drop(columns=["total_area_ha"])
    tb = tb.set_index(["country", "date"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
        yaml_params={"date_accessed": last_date_accessed(tb), "year": last_date_accessed(tb)[-4:]},
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
