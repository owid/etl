"""Load meadow datasets of IRENA's Renewable Power Generation Costs and create a garden dataset.

This step combines three releases of the data, because each older release contains data that the newer ones
dropped:

* The latest release ("Renewable Power Generation Costs in 2025", in constant 2025 US$) covers 2010-2025, but
  dropped the country-level LCOE series of 21 countries (for onshore wind) and 3 countries (for solar
  photovoltaic) that the previous release had, as well as the global solar photovoltaic module price index.
* The previous release ("Renewable Power Generation Costs in 2024", in constant 2024 US$) fills in those dropped
  country series (up to 2024) and provides the module price index (2010-2024, discontinued afterwards).
* The oldest release ("Renewable Power Generation Costs in 2023", in constant 2023 US$) provides data prior to
  2010 (e.g. onshore wind LCOE back to 1984), which later releases no longer include.

Wherever releases overlap, the newest one is prioritized.

Each release is expressed in constant US dollars of a different year, so, before combining them, the older
releases are converted to the latest release's dollars using the US GDP deflator. A single (US) factor is applied
to all countries: IRENA's values are denominated in constant US dollars, so changing the dollar base year is a
unit change governed by the dollar's inflation, the same for every country. This can be verified empirically:
dividing the values of identical country-years across two consecutive releases yields ~1.02 for every country
(instead of each country's own deflator, which for instance would be ~3.08 for Argentina between the 2023 and
2024 releases).
"""

import re

from owid.catalog import Table
from owid.datautils.dataframes import combine_two_overlapping_dataframes

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Dollar year of the latest release (used in the output units, via the yaml_params of the metadata file).
LATEST_YEAR = 2025

# Expected unit patterns of the meadow tables (the dollar year varies per release).
LCOE_UNIT_PATTERN = r"constant (\d{4}) US\$ per kilowatt-hour"
MODULE_PRICE_UNIT_PATTERN = r"constant (\d{4}) US\$ per watt"


def get_dollar_year(tb: Table, unit_pattern: str) -> int:
    """Get the dollar year of a meadow table from the unit metadata of its columns (asserting consistency)."""
    years = set()
    for column in tb.drop(columns=["country", "year"], errors="ignore").columns:
        unit = tb[column].metadata.unit
        match = re.fullmatch(unit_pattern, unit)
        assert match, f"Unexpected unit for column '{column}': '{unit}'."
        years.add(int(match.group(1)))
    error = "Expected all columns of a meadow table to be in the same dollar year."
    assert len(years) == 1, error

    return years.pop()


def get_us_deflator() -> dict[int, float]:
    """Load the US GDP deflator (linked series) as a plain dictionary indexed by year.

    NOTE: Values are extracted as plain floats so that the deflator's metadata (from WDI, which is used as an
    auxiliary dataset) does not propagate into the indicators.
    """
    ds_deflator = paths.load_dataset("owid_deflator")
    tb_deflator = ds_deflator.read("owid_deflator")
    us = tb_deflator[tb_deflator["country"] == "United States"].dropna(subset="gdp_deflator_linked")

    return {int(year): float(value) for year, value in zip(us["year"], us["gdp_deflator_linked"])}


def convert_to_latest_usd(tb: Table, unit_pattern: str, us_deflator: dict[int, float]) -> Table:
    """Convert a table from its own constant-USD base year to constant US$ of LATEST_YEAR."""
    dollar_year = get_dollar_year(tb=tb, unit_pattern=unit_pattern)
    factor = us_deflator[LATEST_YEAR] / us_deflator[dollar_year]
    # NOTE: The latest release is already in LATEST_YEAR US$, so its factor is exactly 1.
    error = f"Unexpected US deflator factor from {dollar_year} to {LATEST_YEAR}."
    assert 1.0 <= factor < 1.15, error
    for column in tb.drop(columns=["country", "year"], errors="ignore").columns:
        tb[column] *= factor
        tb[column].metadata.unit = re.sub(r"\d{4}", str(LATEST_YEAR), tb[column].metadata.unit)

    return tb


def sanity_check_outputs(tb: Table) -> None:
    error = "Unexpected negative costs."
    assert (tb.drop(columns=["country", "year"]).fillna(0) >= 0).all().all(), error

    error = "Unexpected year range."
    assert tb["year"].min() == 1984, error
    assert tb["year"].max() == LATEST_YEAR, error

    error = "World was expected to have data for all technologies in the latest year."
    assert tb[(tb["country"] == "World") & (tb["year"] == LATEST_YEAR)].notna().all().all(), error

    # South Korea was dropped in a previous version (only because it lacked country-deflator data, which the
    # current conversion does not need); check it is present.
    error = "South Korea was expected to have onshore wind data."
    assert not tb[(tb["country"] == "South Korea") & (tb["onshore_wind"].notna())].empty, error


def run() -> None:
    #
    # Load inputs.
    #
    # Find out versions available among the dependencies of the current step, sorted from oldest to newest.
    versions = sorted(
        [step.split("/")[-2] for step in paths.dependencies if step.endswith("renewable_power_generation_costs")]
    )
    error = "Expected exactly three versions of the meadow dataset (see this step's docstring)."
    assert len(versions) == 3, error

    # Load the LCOE table of each meadow dataset, from oldest to newest.
    tables = []
    for version in versions:
        ds_meadow = paths.load_dataset("renewable_power_generation_costs", version=version)
        tables.append(ds_meadow.read("renewable_power_generation_costs", safe_types=False))

    # Load the solar photovoltaic module prices from the second-to-last release (the last one that provided the
    # global module price index, which was discontinued afterwards).
    ds_meadow_previous = paths.load_dataset("renewable_power_generation_costs", version=versions[1])
    tb_solar_pv = ds_meadow_previous.read("solar_photovoltaic_module_prices", safe_types=False)

    # Load the US GDP deflator (linked series).
    us_deflator = get_us_deflator()

    #
    # Process data.
    #
    # Convert each release to constant US$ of the latest year, and harmonize its country names.
    for index in range(len(tables)):
        tables[index] = convert_to_latest_usd(tb=tables[index], unit_pattern=LCOE_UNIT_PATTERN, us_deflator=us_deflator)
        tables[index] = paths.regions.harmonize_names(
            tb=tables[index],
            # Some countries appear only in some of the releases.
            warn_on_missing_countries=(index == len(tables) - 1),
            warn_on_unused_countries=False,
        )

    # Combine the three releases, prioritizing the newest wherever they overlap.
    tb = tables[-1]
    for tb_older in tables[-2::-1]:
        tb = combine_two_overlapping_dataframes(df1=tb, df2=tb_older, index_columns=["country", "year"])

    # Sanity check outputs.
    sanity_check_outputs(tb=tb)  # ty: ignore

    # Convert the module prices to constant US$ of the latest year.
    tb_solar_pv = convert_to_latest_usd(tb=tb_solar_pv, unit_pattern=MODULE_PRICE_UNIT_PATTERN, us_deflator=us_deflator)

    # Improve table formatting.
    tb = tb.format()
    tb_solar_pv = tb_solar_pv.format(short_name="solar_photovoltaic_module_prices")

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb, tb_solar_pv], yaml_params={"LATEST_YEAR": LATEST_YEAR})
    ds_garden.save()
