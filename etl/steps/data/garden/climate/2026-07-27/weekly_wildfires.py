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
# NOTE: Russia accounts for around 90% of the burnt area of the European aggregate in every year, so a plain
# "Europe" line tracks the Russian fire season and can move in the opposite direction to the rest of the
# continent. It is therefore split into the two explicit aggregates below, so that readers pick the one they mean.
# "Europe" itself is still aggregated, because "World" is defined as the sum of the six continents and would
# otherwise be missing Europe entirely. It is dropped right after the aggregation, before anything is published.
# The order matters: "Europe" has to come before "World".
REGIONS = {
    "North America": {},
    "South America": {},
    "Europe": {},
    "Europe (incl. Russia)": {"additional_regions": ["Europe"]},
    "Europe (excl. Russia)": {"additional_regions": ["Europe"], "excluded_members": ["Russia"]},
    "Africa": {},
    "Asia": {},
    "Oceania": {},
    "European Union (27)": {},
    "World": {},
}

# The six continents that "World" is the sum of. Used to check that "World" stays complete.
CONTINENTS = ["Africa", "Asia", "Europe", "North America", "Oceania", "South America"]

# Aggregates that are only built so that "World" can be computed, and that are not published.
REGIONS_NOT_PUBLISHED = ["Europe"]


def sanity_check_world_is_complete(tb: Table) -> None:
    """Check that "World" is still the sum of the six continents.

    "World" is an aggregate of the six continents, not of individual countries, so it is only complete while
    the plain "Europe" aggregate is in the table. Dropping "Europe" before "World" is computed would quietly
    remove Europe from the world total instead of raising.
    """
    missing = sorted(set(CONTINENTS) - set(tb["country"]))
    assert not missing, (
        f"{missing} missing from the table, so the 'World' aggregate is incomplete. Every continent has to stay "
        "in REGIONS, even the ones that are not published."
    )

    world = tb[tb["country"] == "World"].set_index("date").sort_index()
    continents = {continent: tb[tb["country"] == continent].set_index("date").sort_index() for continent in CONTINENTS}
    for continent, tb_continent in continents.items():
        assert world.index.equals(tb_continent.index), f"'{continent}' and 'World' cover different dates."

    for column in ["area_ha", "area_ha_cumulative", "events", "events_cumulative", "CO2", "CO2_cumulative"]:
        total = sum(tb_continent[column].fillna(0) for tb_continent in continents.values())
        # NOTE: Values are stored as float32, so compare relative to the magnitude of the total.
        relative_error = (world[column].fillna(0) - total).abs() / world[column].abs().clip(lower=1)
        assert (relative_error < 1e-5).all(), (
            f"'World' does not equal the sum of the six continents for '{column}'. If the plain 'Europe' "
            "aggregate was removed from REGIONS, Europe is missing from the world total."
        )


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

    for column in ["area_ha", "area_ha_cumulative", "events", "events_cumulative", "CO2", "CO2_cumulative"]:
        # Excluding Russia can only lower the aggregate. Restrict the comparison to the dates where both
        # aggregates are informed, so that it does not depend on how null values compare.
        both_informed = incl[column].notna() & excl[column].notna()
        assert (excl[column][both_informed] <= incl[column][both_informed]).all(), (
            f"'{column}' is larger excluding Russia than including it."
        )

        # The difference between the two aggregates must be Russia. Compare only the dates where all three are
        # informed, and check that those are the majority, so that the comparison is not vacuous.
        informed = both_informed & russia[column].notna()
        assert informed.sum() > 0.5 * len(incl), f"'{column}' is informed on too few dates to be checked."
        # NOTE: Values are stored as float32, so compare relative to the magnitude of the aggregate. An exact
        # comparison fails on rounding alone (the largest aggregates are of the order of 1e7, where one float32
        # step is about 2).
        relative_error = (incl[column] - excl[column] - russia[column]).abs() / incl[column].abs().clip(lower=1)
        assert (relative_error[informed] < 1e-5).all(), f"'{column}' incl. minus excl. Russia does not equal Russia."

    # The shares of land area burnt need a land area for each aggregate. FAOSTAT only reports one for "Europe",
    # so both aggregates would silently come out all null if their denominators were not filled in.
    for column in ["share_area_ha", "share_area_ha_cumulative"]:
        for name, aggregate in [("incl.", incl), ("excl.", excl)]:
            assert aggregate[column].notna().sum() == russia[column].notna().sum(), (
                f"'{column}' is informed on fewer dates for 'Europe ({name} Russia)' than for Russia, which "
                "points to a missing land area."
            )
            assert aggregate[column].dropna().between(0, 100).all(), f"'{column}' is outside 0-100% for Europe."

    # Europe without Russia is a much smaller area, so the same burnt area weighs more heavily.
    assert (excl["total_area_ha"] < incl["total_area_ha"]).all(), (
        "The land area of Europe excluding Russia should be smaller than including it."
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
    sanity_check_world_is_complete(tb_pivot)

    # "Europe" was only needed to build the "World" aggregate. Drop it now, so that readers are left with the
    # two explicit European aggregates and cannot pick a line that is really the Russian fire season.
    tb_pivot = tb_pivot[~tb_pivot["country"].isin(REGIONS_NOT_PUBLISHED)].reset_index(drop=True)

    # Merge land area data with the wildfire data
    tb = pr.merge(tb_pivot, area_most_recent_year, on=["country"], how="left")

    # FAOSTAT reports a land area for "Europe", but not for the two aggregates built from it, so their
    # denominators have to be set explicitly. "Europe (incl. Russia)" reuses Europe's area as published, and
    # "Europe (excl. Russia)" subtracts Russia's.
    faostat_area_ha = area_most_recent_year.set_index("country")["total_area_ha"]
    assert {"Europe", "Russia"} <= set(faostat_area_ha.index), (
        "FAOSTAT is missing a land area for 'Europe' or 'Russia'."
    )
    europe_area_ha = faostat_area_ha.loc["Europe"]
    russia_area_ha = faostat_area_ha.loc["Russia"]
    tb.loc[tb["country"] == "Europe (incl. Russia)", "total_area_ha"] = europe_area_ha
    tb.loc[tb["country"] == "Europe (excl. Russia)", "total_area_ha"] = europe_area_ha - russia_area_ha

    tb["share_area_ha"] = (tb["area_ha"] / tb["total_area_ha"]) * 100
    tb["share_area_ha_cumulative"] = (tb["area_ha_cumulative"] / tb["total_area_ha"]) * 100

    sanity_check_europe_aggregates(tb)

    tb = tb.drop(columns=["total_area_ha"])

    # The land area table brings "Europe" back as an unused category, and a categorical index materialises
    # unused categories as all-null rows in the downstream groupbys. Drop them, so that no empty "Europe"
    # entity reappears in the grapher datasets.
    if isinstance(tb["country"].dtype, pd.CategoricalDtype):
        # NOTE: The ".cat" accessor returns a plain series, so the column metadata has to be restored.
        country_metadata = tb["country"].metadata
        tb["country"] = tb["country"].cat.remove_unused_categories()
        tb["country"].metadata = country_metadata

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
