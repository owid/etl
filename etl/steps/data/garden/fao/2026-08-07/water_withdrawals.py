"""Load the AQUASTAT meadow dataset and create a garden dataset on water withdrawals."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Variables to select from AQUASTAT, and how to rename them.
VARIABLES = {
    "Agricultural water withdrawal": "agricultural_water_withdrawal",
    "Industrial water withdrawal": "industrial_water_withdrawal",
    "Municipal water withdrawal": "municipal_water_withdrawal",
    "Total water withdrawal per capita": "total_water_withdrawal_per_capita",
}

# Expected original unit of each variable.
UNITS_EXPECTED = {
    "Agricultural water withdrawal": "10^9 m3/year",
    "Industrial water withdrawal": "10^9 m3/year",
    "Municipal water withdrawal": "10^9 m3/year",
    "Total water withdrawal per capita": "m3/inhab/year",
}

# Columns of sectoral withdrawals, given in billions of cubic meters per year, to be converted to cubic meters.
SECTORAL_COLUMNS = ["agricultural_water_withdrawal", "industrial_water_withdrawal", "municipal_water_withdrawal"]
COLUMNS_TO_CONVERT = SECTORAL_COLUMNS
BILLION_CUBIC_METERS_TO_CUBIC_METERS = 1e9

# Columns aggregated (by summing member countries) for continents and income groups.
# NOTE: Withdrawal per capita is not aggregated, since it cannot be summed.
LEVEL_COLUMNS = SECTORAL_COLUMNS
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
    world_total = tb.loc[(tb["country"] == "World") & (tb["year"] == 2023), SECTORAL_COLUMNS].sum().sum()
    assert 3.5e12 < world_total < 4.5e12, "Global total withdrawals in 2023 outside the expected range."
    us_2010 = tb.loc[
        (tb["country"] == "United States") & (tb["year"] == 2010), "total_water_withdrawal_per_capita"
    ].item()
    assert 1400 < us_2010 < 1700, "US total water withdrawal per capita in 2010 differs from expected ~1557."


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
    for column in COLUMNS_TO_CONVERT:
        tb[column] = (tb[column].astype("float64") * BILLION_CUBIC_METERS_TO_CUBIC_METERS).round()

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Keep FAO's aggregate rows only from a year with complete coverage (see FAO_AGGREGATES_MIN_YEAR note).
    mask_fao_aggregates = tb["country"].str.endswith("(FAO)") | (tb["country"] == "World")
    n_countries = (
        tb[~mask_fao_aggregates & (tb["year"] >= FAO_AGGREGATES_MIN_YEAR)]
        .groupby("year")["agricultural_water_withdrawal"]
        .count()
    )
    assert (n_countries > 150).all(), "Incomplete country coverage in years where FAO's aggregates are kept."
    tb = tb[~(mask_fao_aggregates & (tb["year"] < FAO_AGGREGATES_MIN_YEAR))].reset_index(drop=True)

    # Add region aggregates.
    tb = paths.regions.add_aggregates(
        tb=tb,
        regions=REGIONS,
        aggregations={column: "sum" for column in LEVEL_COLUMNS},
        min_frac_countries_informed=MIN_FRAC_COUNTRIES_INFORMED,
    )

    # Sanity check: FAO's published World row should agree with the same aggregate computed from country data.
    sanity_check_world(tb=tb)

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
