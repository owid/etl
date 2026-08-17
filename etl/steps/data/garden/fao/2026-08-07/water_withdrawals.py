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

# Columns aggregated (by summing member countries) for regions, income groups and World.
LEVEL_COLUMNS = SECTORAL_COLUMNS + ["total_water_withdrawal", "total_freshwater_withdrawal"]
REGIONS = [
    "Africa",
    "Asia",
    "Europe",
    "North America",
    "South America",
    "Oceania",
    "World",
    "Low-income countries",
    "Lower-middle-income countries",
    "Upper-middle-income countries",
    "High-income countries",
]
# Minimum fraction of a region's (ever-informed) countries that must have data for an aggregate to be created.
MIN_FRAC_COUNTRIES_INFORMED = 0.7

# First year for which FAO's own regional aggregate rows (the "(FAO)" SDG groupings) are kept. They are progressive
# sums over the countries reporting in each year, so early years reflect incomplete coverage rather than real levels
# (in 1965, FAO's "World" row is 0.68 billion m³ — the sum of the only two reporting countries, Uruguay and Barbados).
# NOTE: In 2000 itself, the share series of a few FAO aggregates still lag (sums of 72-86%), hence 2001.
FAO_AGGREGATES_MIN_YEAR = 2001


def sanity_check_inputs(tb: Table) -> None:
    assert set(tb["variable"]) == set(VARIABLES), "Expected AQUASTAT variables not found; check their names."
    units = tb.groupby("variable", observed=True)["unit"].unique()
    for variable, unit in UNITS_EXPECTED.items():
        assert list(units[variable]) == [unit], f"Unexpected unit for variable {variable}."
    assert tb["value"].notnull().all(), "Unexpected missing values."
    assert (tb["value"] >= 0).all(), "Unexpected negative values."
    assert tb["year"].max() >= 2023, "Latest year is earlier than expected."
    assert not tb.duplicated(subset=["country", "variable", "year"]).any(), "Duplicated country-variable-year rows."


def sanity_check_outputs(tb: Table) -> None:
    assert tb["country"].nunique() >= 180, "Number of countries decreased unexpectedly."
    # NOTE: In the 2026-08-07 version, each variable had 6,000-6,500 data points after dropping regional aggregates.
    for column in VARIABLES.values():
        assert tb[column].notnull().sum() > 5500, f"Data points for {column} decreased unexpectedly."

    # Global withdrawals have hovered around 4 trillion m³/year in recent years (4.03 trillion m³ in 2023).
    world_total = tb.loc[(tb["country"] == "World") & (tb["year"] == 2023), "total_water_withdrawal"].item()
    assert 3.5e12 < world_total < 4.5e12, "Global total withdrawals in 2023 outside the expected range."

    # Aggregate shares are computed from the aggregated levels, so they should add up to ~100% (small deviations
    # come from FAO's sectoral and total series having different vintages for some countries).
    mask_aggregates = tb["country"].isin(REGIONS)
    share_sum = tb.loc[mask_aggregates, SHARE_COLUMNS].sum(axis=1, min_count=3).dropna()
    assert share_sum.between(95, 105).all(), "Aggregate sectoral shares far from 100%."
    # World aggregates should start once country coverage is high, not in the sparse early years.
    world_years = tb.loc[(tb["country"] == "World") & tb["total_water_withdrawal"].notnull(), "year"]
    assert 1985 <= world_years.min() <= 2005, "World aggregate starts in a year with unexpected coverage."
    for column in SHARE_COLUMNS:
        assert tb[column].dropna().between(0, 102).all(), f"{column} outside the 0-100% range."

    # FAO's own regional rows carry mild inconsistencies even in recent years (share sums between ~88 and ~110),
    # since its sectoral and total series have different vintages.
    fao_share_sum = tb.loc[tb["country"].str.endswith("(FAO)"), SHARE_COLUMNS].sum(axis=1, min_count=3).dropna()
    assert fao_share_sum.between(88, 110).all(), "FAO aggregate sectoral shares far from 100%."

    # For countries, the total should equal the sum of the three sectors in the vast majority of cases.
    # NOTE: FAO's own data is inconsistent for a few country-years (e.g. Namibia 2020-2021, and North Macedonia in
    # recent years, whose total far exceeds the sum of sectors).
    mask_fao_regions = tb["country"].str.endswith("(FAO)")
    complete = tb[~tb["country"].isin(REGIONS) & ~mask_fao_regions].dropna(
        subset=SECTORAL_COLUMNS + ["total_water_withdrawal"]
    )
    ratio = complete[SECTORAL_COLUMNS].sum(axis=1) / complete["total_water_withdrawal"]
    assert ((ratio - 1).abs() < 0.02).mean() > 0.97, "Too many countries where sectors do not add up to the total."

    # Freshwater withdrawal (surface + groundwater only) should be roughly bounded by total withdrawal.
    # NOTE: FAO compiles the two series separately, and freshwater slightly exceeds total in ~10% of rows (by more
    # than 2% in ~9%, e.g. Yemen, Malaysia, Zambia); the loose bound below only guards against unit regressions.
    complete = tb.dropna(subset=["total_freshwater_withdrawal", "total_water_withdrawal"])
    ratio = complete["total_freshwater_withdrawal"] / complete["total_water_withdrawal"]
    assert (ratio < 1.05).mean() > 0.90, "Too many rows where freshwater withdrawal exceeds total withdrawal."

    # Anchor values, to guard against unit regressions (values as published by AQUASTAT, checked against the
    # previous version of this dataset).
    india_2010 = tb.loc[(tb["country"] == "India") & (tb["year"] == 2010), "agricultural_water_withdrawal"].item()
    assert 6.0e11 < india_2010 < 7.5e11, "India's agricultural water withdrawal in 2010 differs from expected ~688e9."
    us_2010 = tb.loc[
        (tb["country"] == "United States") & (tb["year"] == 2010), "total_water_withdrawal_per_capita"
    ].item()
    assert 1400 < us_2010 < 1700, "US total water withdrawal per capita in 2010 differs from expected ~1557."

    # Per capita withdrawals have historically peaked below ~6,000 m³ (Turkmenistan).
    assert tb["total_water_withdrawal_per_capita"].max() < 10000, "Per capita withdrawal outside the expected range."


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

    # Harmonize country names. FAO's SDG regional groupings are kept, mapped to the shared "(FAO)" region entities;
    # FAO's own "World" row (replaced by the aggregate computed below) and the special groups with no counterpart in
    # the regions dataset (LDCs, LLDCs, SIDS) are excluded in the country mapping.
    tb = paths.regions.harmonize_names(tb=tb)

    # Keep FAO's regional rows only from a year with complete coverage (see FAO_AGGREGATES_MIN_YEAR note).
    mask_fao_regions = tb["country"].str.endswith("(FAO)")
    n_countries = (
        tb[~mask_fao_regions & (tb["year"] >= FAO_AGGREGATES_MIN_YEAR)]
        .groupby("year")["total_water_withdrawal"]
        .count()
    )
    assert (n_countries > 150).all(), "Incomplete country coverage in years where FAO's aggregates are kept."
    tb = tb[~(mask_fao_regions & (tb["year"] < FAO_AGGREGATES_MIN_YEAR))].reset_index(drop=True)

    # A few countries carry shares of total withdrawal above 100% (e.g. Brunei's municipal share in 2004-2023),
    # where FAO's sectoral series are more recent than its (carried-forward) total series. A share of the total
    # above 100% is internally inconsistent, so remove those values.
    n_impossible = 0
    for column in SHARE_COLUMNS:
        mask_impossible = tb[column] > 100
        n_impossible += mask_impossible.sum()
        tb.loc[mask_impossible, column] = None
    assert 20 < n_impossible < 60, "Unexpected number of shares above 100%."

    # Add aggregates for continents, income groups and World, by summing member countries, only where at least
    # 70% of each region's (ever-informed) countries have data.
    tb = paths.regions.add_aggregates(
        tb=tb,
        regions=REGIONS,
        aggregations={column: "sum" for column in LEVEL_COLUMNS},
        min_frac_countries_informed=MIN_FRAC_COUNTRIES_INFORMED,
    )

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
