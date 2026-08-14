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

    # The sum of the three sectors across countries (excluding regional aggregates) should recover the World row.
    mask_countries = ~tb["country"].str.endswith("(FAO)") & (tb["country"] != "World")
    countries_total = tb[mask_countries & (tb["year"] == 2023)][SECTORAL_COLUMNS].sum().sum()
    assert abs(countries_total / world_total - 1) < 0.02, "Countries do not add up to the World total."

    # After filtering incomplete-coverage years, aggregate rows should be internally consistent.
    mask_aggregates = tb["country"].str.endswith("(FAO)") | (tb["country"] == "World")
    share_sum = tb.loc[mask_aggregates, SHARE_COLUMNS].sum(axis=1, min_count=3).dropna()
    assert share_sum.between(98, 102).all(), "Aggregate sectoral shares do not add up to 100%."
    for column in SHARE_COLUMNS:
        assert tb[column].dropna().between(0, 101).all(), f"{column} outside the 0-100% range."

    # For countries, the total should equal the sum of the three sectors in the vast majority of cases.
    # NOTE: FAO's own data is inconsistent for a few country-years (e.g. Namibia 2020-2021, and North Macedonia in
    # recent years, whose total far exceeds the sum of sectors).
    complete = tb[~mask_aggregates].dropna(subset=SECTORAL_COLUMNS + ["total_water_withdrawal"])
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

    # Harmonize country names. AQUASTAT's regional aggregates are kept, mapped to the shared FAO SDG region
    # entities; only special groups with no counterpart (LDCs, LLDCs, SIDS) are excluded in the country mapping.
    tb = paths.regions.harmonize_names(tb=tb)

    # FAO's regional aggregates are sums over the countries reporting in each year, so in early years (when few
    # countries had data) they reflect incomplete coverage rather than real levels (e.g. the World row gives a total
    # withdrawal of 0.7 billion m³ in 1965, and sectoral shares adding up to 196% in 1972). Keep aggregate rows only
    # in years where FAO's own sectoral shares add up to ~100%, which signals complete coverage (~1997 for World).
    mask_aggregates = tb["country"].str.endswith("(FAO)") | (tb["country"] == "World")
    share_sum = tb[SHARE_COLUMNS].sum(axis=1, min_count=3)
    # NOTE: Aggregate rows missing any of the three shares also count as incomplete (fillna, since ~NA is NA).
    mask_incomplete = mask_aggregates & ~share_sum.between(98, 102).fillna(False)
    assert 400 < mask_incomplete.sum() < 900, "Unexpected number of incomplete-coverage aggregate rows."
    tb = tb[~mask_incomplete].reset_index(drop=True)

    # A few countries carry shares of total withdrawal above 100% (e.g. Brunei's municipal share in 2004-2023),
    # where FAO's sectoral series are more recent than its (carried-forward) total series. A share of the total
    # above 100% is internally inconsistent, so remove those values.
    n_impossible = 0
    for column in SHARE_COLUMNS:
        mask_impossible = tb[column] > 100
        n_impossible += mask_impossible.sum()
        tb.loc[mask_impossible, column] = None
    assert 20 < n_impossible < 60, "Unexpected number of shares above 100%."

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
