"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Dataset, Table

from etl.catalog_helpers import last_date_accessed
from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Name of the aggregate added alongside "Europe".
EUROPE_EXCL_RUSSIA = "Europe (excl. Russia)"

# Regions to aggregate.
# "Europe" includes all of Russia, which accounts for around 90% of the region's burnt area in every year, so the
# continental total mostly follows the Russian fire season and can move in the opposite direction to the rest of
# the continent. EUROPE_EXCL_RUSSIA is aggregated alongside it, so readers can choose between "European Union
# (27)", "Europe" and "Europe (excl. Russia)".
# NOTE: The order matters. "World" is defined in the regions dataset as the sum of the six continents, so "Europe"
# has to be aggregated before it. "Europe (excl. Russia)" is not one of those six, so it is not double counted.
REGIONS = {
    "North America": {},
    "South America": {},
    "Europe": {},
    EUROPE_EXCL_RUSSIA: {"additional_regions": ["Europe"], "excluded_members": ["Russia"]},
    "Africa": {},
    "Asia": {},
    "Oceania": {},
    "European Union (27)": {},
    "World": {},
}

# The six continents that "World" is defined as the sum of, used to check that "World" stays complete.
CONTINENTS = ["Africa", "Asia", "Europe", "North America", "Oceania", "South America"]

# Members of "Europe" that the source does not report, and which the aggregates therefore cannot include.
# "Serbia excluding Kosovo" is an alternative to "Serbia", which the source does report, and the source folds
# Transnistria into Moldova.
EUROPE_MEMBERS_NOT_REPORTED = ["Serbia excluding Kosovo", "Transnistria"]


def sanity_check_member_coverage(tb: Table, ds_regions: Dataset, summed_columns: list[str]) -> None:
    """Check that every European country the source covers is present, and reports on every informed date.

    Regions are aggregated with min_num_values_per_year=1, which is what this dataset needs: no country reports in
    the off-season, and the burnt-area series only starts in 2012, so requiring every member would empty the
    aggregates. The cost of that tolerance is that a country dropping out of a future weekly snapshot, or missing a
    single week, would quietly shrink "Europe" and "Europe (excl. Russia)" by the same amount, leaving the
    reconciliation between the two intact. These asserts make it loud instead.
    """
    expected_members = set(
        geo.list_members_of_region("Europe", ds_regions, excluded_members=["Russia"], exclude_historical_countries=True)
    ) - set(EUROPE_MEMBERS_NOT_REPORTED)

    missing = sorted(expected_members - set(tb["country"]))
    assert not missing, f"The source no longer reports {missing}, which would silently shrink the European aggregates."

    members = tb[tb["country"].isin(expected_members)]
    for column in summed_columns:
        reporting = members.groupby("date", observed=True)[column].apply(lambda values: values.notna().sum())
        # NOTE: The source reports either every European country or none of them on a given date. If this ever
        # fails, check whether the source really has a gap before relaxing it, because a partially covered date
        # understates a published aggregate.
        partial = reporting[(reporting > 0) & (reporting < len(expected_members))]
        assert partial.empty, (
            f"'{column}' is reported by only some European countries on "
            f"{[str(date.date()) for date in partial.index[:5]]}, so the European aggregates are understated."
        )


def sanity_check_outputs(tb: Table, summed_columns: list[str]) -> None:
    """Check the new "Europe (excl. Russia)" aggregate, and that adding it left the other regions untouched."""
    tb = tb.reset_index()
    countries = set(tb["country"])
    assert {"Europe", EUROPE_EXCL_RUSSIA, "Russia", "World"} <= countries, "A required region is missing."
    assert set(CONTINENTS) <= countries, "A continent is missing, which would make 'World' incomplete."

    europe = tb[tb["country"] == "Europe"].set_index("date").sort_index()
    excl = tb[tb["country"] == EUROPE_EXCL_RUSSIA].set_index("date").sort_index()
    russia = tb[tb["country"] == "Russia"].set_index("date").sort_index()
    assert europe.index.equals(excl.index), "'Europe' and 'Europe (excl. Russia)' cover different dates."
    assert europe.index.equals(russia.index), "Russia and the European aggregates cover different dates."

    # No indicator should be entirely empty for the new aggregate. FAOSTAT publishes no land area for it, so the
    # share indicators come out null unless their denominator is derived explicitly.
    for column in [c for c in tb.columns if c not in ("country", "date")]:
        if russia[column].notna().any():
            assert excl[column].notna().any(), f"'{column}' is entirely empty for '{EUROPE_EXCL_RUSSIA}'."

    for column in summed_columns:
        # Excluding Russia can only lower the total.
        assert (excl[column] <= europe[column]).all(), f"'{column}' is larger excluding Russia than including it."
        # Europe minus Europe excluding Russia must be Russia.
        # NOTE: Values are stored as float32, so compare relative to the magnitude of the total. An exact
        # comparison fails on rounding alone (the largest totals are of the order of 1e7, where one float32 step
        # is about 2).
        informed = europe[column].notna() & excl[column].notna() & russia[column].notna()
        assert informed.sum() > 0.5 * len(europe), f"'{column}' is informed on too few dates to be checked."
        error = (europe[column] - excl[column] - russia[column]).abs() / europe[column].abs().clip(lower=1)
        assert (error[informed] < 1e-5).all(), f"'{column}' for Europe minus Europe excluding Russia is not Russia."

        # "World" is the sum of the six continents. Adding a region must not change it.
        world = tb[tb["country"] == "World"].set_index("date").sort_index()[column]
        continents = sum(tb[tb["country"] == c].set_index("date").sort_index()[column].fillna(0) for c in CONTINENTS)
        informed = world.notna()
        error = (world - continents).abs() / world.abs().clip(lower=1)
        assert (error[informed] < 1e-5).all(), f"'World' is not the sum of the six continents for '{column}'."

    for column in [c for c in tb.columns if c.startswith("share_")]:
        assert excl[column].dropna().between(0, 100).all(), f"'{column}' is outside 0-100% for {EUROPE_EXCL_RUSSIA}."


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
    # Merge land area data with the wildfire data
    tb = pr.merge(tb_pivot, area_most_recent_year, on=["country"], how="left")

    # FAOSTAT publishes a land area for "Europe", but not for aggregates derived from it, so the merge above leaves
    # the new aggregate without a denominator and its shares would silently come out null. Derive it here.
    faostat_area_ha = area_most_recent_year.set_index("country")["total_area_ha"]
    assert {"Europe", "Russia"} <= set(faostat_area_ha.index), (
        "FAOSTAT is missing a land area for 'Europe' or 'Russia'."
    )
    tb.loc[tb["country"] == EUROPE_EXCL_RUSSIA, "total_area_ha"] = (
        faostat_area_ha.loc["Europe"] - faostat_area_ha.loc["Russia"]
    )

    tb["share_area_ha"] = (tb["area_ha"] / tb["total_area_ha"]) * 100
    tb["share_area_ha_cumulative"] = (tb["area_ha_cumulative"] / tb["total_area_ha"]) * 100

    tb = tb.drop(columns=["total_area_ha"])
    tb = tb.set_index(["country", "date"], verify_integrity=True)

    sanity_check_member_coverage(tb.reset_index(), ds_regions=ds_regions, summed_columns=cols_to_keep)
    sanity_check_outputs(tb, summed_columns=cols_to_keep)

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
