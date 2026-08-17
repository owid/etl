"""Load the AQUASTAT meadow dataset and create a garden dataset on water withdrawals."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Variables to select from AQUASTAT, and how to rename them.
VARIABLES = {
    "Agricultural water withdrawal": "agricultural_water_withdrawal",
    "Agricultural water withdrawal as % of total water withdrawal": "agricultural_water_withdrawal_share",
    "Industrial water withdrawal": "industrial_water_withdrawal",
    "Industrial water withdrawal as % of total water withdrawal": "industrial_water_withdrawal_share",
    "Municipal water withdrawal": "municipal_water_withdrawal",
    "Municipal water withdrawal as % of total withdrawal": "municipal_water_withdrawal_share",
    "Total freshwater withdrawal": "total_freshwater_withdrawal",
    "Total water withdrawal": "total_water_withdrawal",
    "Total water withdrawal per capita": "total_water_withdrawal_per_capita",
}

# Expected original unit of each variable.
UNITS_EXPECTED = {
    "Agricultural water withdrawal": "10^9 m3/year",
    "Agricultural water withdrawal as % of total water withdrawal": "%",
    "Industrial water withdrawal": "10^9 m3/year",
    "Industrial water withdrawal as % of total water withdrawal": "%",
    "Municipal water withdrawal": "10^9 m3/year",
    "Municipal water withdrawal as % of total withdrawal": "%",
    "Total freshwater withdrawal": "10^9 m3/year",
    "Total water withdrawal": "10^9 m3/year",
    "Total water withdrawal per capita": "m3/inhab/year",
}

# Columns of sectoral withdrawals, which should add up to the total withdrawal.
SECTORAL_COLUMNS = ["agricultural_water_withdrawal", "industrial_water_withdrawal", "municipal_water_withdrawal"]
# Columns of shares of total withdrawal, which should add up to 100%.
SHARE_COLUMNS = [
    "agricultural_water_withdrawal_share",
    "industrial_water_withdrawal_share",
    "municipal_water_withdrawal_share",
]
# Columns originally given in billions of cubic meters per year, to be converted to cubic meters per year.
COLUMNS_TO_CONVERT = SECTORAL_COLUMNS + ["total_water_withdrawal", "total_freshwater_withdrawal"]
BILLION_CUBIC_METERS_TO_CUBIC_METERS = 1e9

# Columns aggregated (by summing member countries) for continents and income groups.
LEVEL_COLUMNS = SECTORAL_COLUMNS + ["total_water_withdrawal", "total_freshwater_withdrawal"]
REGIONS = [
    "Africa",
    "Asia",
    "Europe",
    "North America",
    "South America",
    "Oceania",
    "Low-income countries",
    "Lower-middle-income countries",
    "Upper-middle-income countries",
    "High-income countries",
]
# Minimum fraction of a region's (ever-informed) countries that must have data for an aggregate to be created.
MIN_FRAC_COUNTRIES_INFORMED = 0.7
# Maximum accepted deviation between FAO's published World row and the same aggregate computed from countries.
WORLD_MAX_DEVIATION = 0.02

# First year for which FAO's own aggregate rows (World and the "(FAO)" SDG groupings) are kept. They are progressive
# sums over the countries reporting in each year, so early years reflect incomplete coverage rather than real levels
# (in 1965, FAO's "World" row is 0.68 billion m³ — the sum of the only two reporting countries, Uruguay and Barbados).
# NOTE: In 2000 itself, the share series of a few FAO aggregates still lag (sums of 72-86%), hence 2001.
FAO_AGGREGATES_MIN_YEAR = 2001


def sanity_check_inputs(tb: Table) -> None:
    assert set(tb["variable"]) == set(VARIABLES), "Expected AQUASTAT variables not found; check their names."
    units = tb.groupby("variable", observed=True)["unit"].unique()
    for variable, unit in UNITS_EXPECTED.items():
        assert list(units[variable]) == [unit], f"Unexpected unit for variable {variable}."


def sanity_check_world(tb: Table) -> None:
    """Check FAO's published World row against the same aggregate computed by summing countries."""
    mask_countries = ~tb["country"].isin(REGIONS + ["World"]) & ~tb["country"].str.endswith("(FAO)")
    computed = tb[mask_countries].groupby("year", as_index=False)[LEVEL_COLUMNS].sum(min_count=1)
    published = tb[tb["country"] == "World"]
    comparison = published.merge(computed, on="year", suffixes=("_published", "_computed"))
    assert len(comparison) > 20, "Not enough years to compare FAO's World row with the computed aggregate."
    for column in LEVEL_COLUMNS:
        deviation = (comparison[f"{column}_computed"] / comparison[f"{column}_published"] - 1).abs()
        assert deviation.max() < WORLD_MAX_DEVIATION, (
            f"FAO's World row for {column} deviates from the sum of countries by more than "
            f"{WORLD_MAX_DEVIATION:.0%} (max {deviation.max():.1%})."
        )


def sanity_check_outputs(tb: Table) -> None:
    # Coverage should not shrink (e.g. because of a country mapping regression).
    assert tb["country"].nunique() >= 180, "Number of countries decreased unexpectedly."

    # Magnitude anchors, to catch unit regressions in the two different units of this dataset.
    # NOTE: Global withdrawals have hovered around 4 trillion m³/year in recent years.
    world_total = tb.loc[(tb["country"] == "World") & (tb["year"] == 2023), "total_water_withdrawal"].item()
    assert 3.5e12 < world_total < 4.5e12, "Global total withdrawals in 2023 outside the expected range."
    us_2010 = tb.loc[
        (tb["country"] == "United States") & (tb["year"] == 2010), "total_water_withdrawal_per_capita"
    ].item()
    assert 1400 < us_2010 < 1700, "US total water withdrawal per capita in 2010 differs from expected ~1557."

    # Each share must equal its sector divided by the total. FAO derives its shares exactly this way, and we do the
    # same for the aggregates we compute, so any deviation means columns got misaligned somewhere.
    # NOTE: FAO's own sectors do not always add up to its own total (see the note in run), so the shares of a given
    # country and year do not always add up to 100%.
    complete = tb.dropna(subset=SECTORAL_COLUMNS + SHARE_COLUMNS + ["total_water_withdrawal"])
    for sector, share in zip(SECTORAL_COLUMNS, SHARE_COLUMNS):
        deviation = (complete[share] - 100 * complete[sector] / complete["total_water_withdrawal"]).abs()
        assert deviation.max() < 0.01, f"{share} is not consistent with {sector} divided by the total."


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("aquastat")
    tb = ds_meadow.read("aquastat")

    #
    # Process data.
    #
    # Select the variables of interest.
    tb = tb[tb["variable"].isin(VARIABLES)].reset_index(drop=True)

    # Sanity checks.
    sanity_check_inputs(tb=tb)

    # Reshape table to wide format, with one column per variable.
    tb = tb.pivot(index=["country", "year"], columns="variable", values="value", join_column_levels_with="_")
    tb = tb.rename(columns=VARIABLES, errors="raise")

    # Convert withdrawals from billions of cubic meters per year to cubic meters per year.
    # NOTE: Convert in float64 and round to whole cubic meters, to avoid float32 precision artifacts
    # (e.g. 688000008192 instead of 688000000000).
    for column in COLUMNS_TO_CONVERT:
        tb[column] = (tb[column].astype("float64") * BILLION_CUBIC_METERS_TO_CUBIC_METERS).round()

    # Harmonize country names. FAO's own aggregate rows are kept: its World row as "World", and its SDG regional
    # groupings as the shared "(FAO)" region entities. Only the special groups with no counterpart in the regions
    # dataset (LDCs, LLDCs, SIDS) are excluded in the country mapping.
    tb = paths.regions.harmonize_names(tb=tb)

    # Keep FAO's aggregate rows only from a year with complete coverage (see FAO_AGGREGATES_MIN_YEAR note).
    mask_fao_aggregates = tb["country"].str.endswith("(FAO)") | (tb["country"] == "World")
    n_countries = (
        tb[~mask_fao_aggregates & (tb["year"] >= FAO_AGGREGATES_MIN_YEAR)]
        .groupby("year")["total_water_withdrawal"]
        .count()
    )
    assert (n_countries > 150).all(), "Incomplete country coverage in years where FAO's aggregates are kept."
    tb = tb[~(mask_fao_aggregates & (tb["year"] < FAO_AGGREGATES_MIN_YEAR))].reset_index(drop=True)

    # FAO compiles the sectoral series and the total series separately, so they do not always agree: for about 2%
    # of country-years the three sectors do not add up to the total (e.g. North Macedonia, whose total is several
    # times the sum of its sectors). Those values are published as they are. But where a single sector exceeds the
    # total, the resulting share is above 100%, which is impossible, so those shares are removed.
    n_impossible = 0
    for column in SHARE_COLUMNS:
        mask_impossible = tb[column] > 100
        n_impossible += mask_impossible.sum()
        tb.loc[mask_impossible, column] = None
    assert 20 < n_impossible < 60, "Unexpected number of shares above 100%."

    # Add aggregates for continents and income groups, by summing member countries, only where at least 70% of each
    # region's (ever-informed) countries have data.
    # NOTE: World is not aggregated here; FAO's own published World row is kept instead (checked right below).
    tb = paths.regions.add_aggregates(
        tb=tb,
        regions=REGIONS,
        aggregations={column: "sum" for column in LEVEL_COLUMNS},
        min_frac_countries_informed=MIN_FRAC_COUNTRIES_INFORMED,
    )

    # Sanity check: FAO's published World row should agree with the same aggregate computed from country data.
    sanity_check_world(tb=tb)

    # For aggregate rows, compute the sectoral shares from the aggregated levels (FAO's shares are country-level).
    mask_regions = tb["country"].isin(REGIONS)
    for sector, share in zip(SECTORAL_COLUMNS, SHARE_COLUMNS):
        tb.loc[mask_regions, share] = (
            100 * tb.loc[mask_regions, sector] / tb.loc[mask_regions, "total_water_withdrawal"]
        )

    # In a few region-years, the countries informing the sectors cover only part of those informing the total, making
    # the shares misleading (e.g. in Africa in 1990, sectors add up to 58% of the total). Keep aggregate shares only
    # where the sectoral breakdown is consistent with the total.
    share_sum = tb.loc[mask_regions, SHARE_COLUMNS].sum(axis=1, min_count=3)
    inconsistent = share_sum.notnull() & ~share_sum.between(95, 105)
    assert 10 < inconsistent.sum() < 60, "Unexpected number of region-years with inconsistent sectoral shares."
    tb.loc[inconsistent[inconsistent].index, SHARE_COLUMNS] = None

    # Sanity checks.
    sanity_check_outputs(tb=tb)

    # Improve table format.
    tb = tb.format(short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])

    # Save garden dataset.
    ds_garden.save()
