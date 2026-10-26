"""Combine IRENA's Renewable Power Generation Costs releases into one dataset.

Each release drops data that previous releases had (pre-2010 history, country series, the solar PV module price
index), so several meadow versions are kept as dependencies and combined, prioritizing the newest one.

Releases are expressed in constant US$ of different years. Older ones are converted to the latest release's dollars
with the US GDP deflator (one factor for all countries, since the values are already in US dollars).

NOTE: On the next update, add the new meadow version to the dag, and update LATEST_YEAR and MODULE_PRICES_VERSION.
"""

import re

from owid.catalog import Table
from owid.datautils.dataframes import combine_two_overlapping_dataframes

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Dollar year of the latest release.
LATEST_YEAR = 2025
# Last release that included the solar PV module price index (discontinued afterwards).
MODULE_PRICES_VERSION = "2025-08-22"
# Meadow units, with the dollar year of each release.
LCOE_UNIT_PATTERN = r"constant (\d{4}) US\$ per kilowatt-hour"
MODULE_PRICE_UNIT_PATTERN = r"constant (\d{4}) US\$ per watt"


def get_us_deflator() -> dict[int, float]:
    ds_deflator = paths.load_dataset("owid_deflator")
    tb = ds_deflator.read("owid_deflator")
    tb = tb[tb["country"] == "United States"].dropna(subset="gdp_deflator_linked")
    # Plain floats, so that the deflator's metadata does not propagate to the indicators.
    return {int(year): float(value) for year, value in zip(tb["year"], tb["gdp_deflator_linked"])}


def convert_to_latest_usd(tb: Table, unit_pattern: str, us_deflator: dict[int, float]) -> Table:
    columns = [column for column in tb.columns if column not in ["country", "year"]]
    years = set()
    for column in columns:
        match = re.fullmatch(unit_pattern, tb[column].metadata.unit)
        assert match, f"Unexpected unit for column '{column}': '{tb[column].metadata.unit}'."
        years.add(int(match.group(1)))
    assert len(years) == 1, "All columns of a release should be in the same dollar year."
    dollar_year = years.pop()

    factor = us_deflator[LATEST_YEAR] / us_deflator[dollar_year]
    assert 1.0 <= factor < 1.15, f"Unexpected deflator factor from {dollar_year} to {LATEST_YEAR}: {factor}."
    for column in columns:
        tb[column] *= factor
        tb[column].metadata.unit = re.sub(r"\d{4}", str(LATEST_YEAR), tb[column].metadata.unit)

    return tb


def sanity_check_outputs(tb: Table) -> None:
    assert (tb.drop(columns=["country", "year"]).fillna(0) >= 0).all().all(), "Negative costs."
    assert tb["year"].min() == 1984, "Pre-2010 history (from the oldest release) is missing."
    assert tb["year"].max() == LATEST_YEAR, "Latest year is missing."
    assert tb[(tb["country"] == "World") & (tb["year"] == LATEST_YEAR)].notna().all().all(), "World is missing data."


def run() -> None:
    #
    # Load inputs.
    #
    versions = sorted(
        [step.split("/")[-2] for step in paths.dependencies if step.endswith("renewable_power_generation_costs")]
    )
    tables = [
        paths.load_dataset("renewable_power_generation_costs", version=version).read(
            "renewable_power_generation_costs", safe_types=False
        )
        for version in versions
    ]
    tb_solar_pv = paths.load_dataset("renewable_power_generation_costs", version=MODULE_PRICES_VERSION).read(
        "solar_photovoltaic_module_prices", safe_types=False
    )
    us_deflator = get_us_deflator()

    #
    # Process data.
    #
    for i, tb in enumerate(tables):
        tb = convert_to_latest_usd(tb=tb, unit_pattern=LCOE_UNIT_PATTERN, us_deflator=us_deflator)
        tables[i] = paths.regions.harmonize_names(
            tb=tb, warn_on_missing_countries=(i == len(tables) - 1), warn_on_unused_countries=False
        )

    # Combine releases, newest first.
    tb = tables[-1]
    for tb_older in tables[-2::-1]:
        tb = combine_two_overlapping_dataframes(df1=tb, df2=tb_older, index_columns=["country", "year"])

    sanity_check_outputs(tb=tb)  # ty: ignore

    tb_solar_pv = convert_to_latest_usd(tb=tb_solar_pv, unit_pattern=MODULE_PRICE_UNIT_PATTERN, us_deflator=us_deflator)

    tb = tb.format()
    tb_solar_pv = tb_solar_pv.format(short_name="solar_photovoltaic_module_prices")

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb, tb_solar_pv], yaml_params={"LATEST_YEAR": LATEST_YEAR})
    ds_garden.save()
