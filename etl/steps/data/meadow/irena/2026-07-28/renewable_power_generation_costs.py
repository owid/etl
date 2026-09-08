"""Extract global (as well as at the country level for some countries) weighted-average levelized cost of electricity
(LCOE) for all energy sources from IRENA's Renewable Power Generation Costs dataset.

Extract solar photovoltaic module prices too.

NOTE: The original data is poorly formatted. The figures are renumbered on every release, and each one has its own
layout, so it is likely that on the next update this script will not work.

Changes in the "Renewable Power Generation Costs in 2025" release, compared to the previous one:

* LCOE is now given in USD/MWh (it used to be given in USD/kWh). We convert it to USD/kWh, which is the unit we
  publish, and assert the unit found in the file, so that a further change fails loudly.
* Global weighted-average LCOE for all technologies is now given in a single sheet (figure S.2), instead of one
  sheet per technology.
* Country-level LCOE is given in sheets whose titles mention only the last two years, but whose data actually
  covers the full period (figures 2.11 and 3.7).
* Solar PV module prices are now given yearly (they used to be monthly) and only from 2020 on, and they are
  broken down by module technology sold in Europe. The global price index that we used before is gone.

"""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Expected USD year.
# NOTE: We could get this from the version, but, if later on we create a minor upgrade with a different year, this will fail.
#  So, instead, hardcode the year and change it on next update.
EXPECTED_DOLLAR_YEAR = 2025
# Expected unit to be found in the LCOE sheets.
EXPECTED_LCOE_UNIT = f"{EXPECTED_DOLLAR_YEAR} USD/MWh"
# Expected unit to be found in the solar PV module prices sheet.
EXPECTED_SOLAR_PV_MODULE_COST_UNIT = f"{EXPECTED_DOLLAR_YEAR} USD/Wp"
# Conversion factor to go from the LCOE unit given by the producer to the unit we publish.
MWH_TO_KWH = 1000

# Sheet with the global weighted-average LCOE of all technologies.
GLOBAL_LCOE_SHEET = "Fig S.2"
# Technologies given in that sheet, and how to rename them.
GLOBAL_LCOE_TECHNOLOGIES = {
    "Bioenergy": "Bioenergy",
    "Geothermal": "Geothermal",
    "Hydro": "Hydropower",
    "Offshore wind": "Offshore wind",
    "Onshore wind": "Onshore wind",
    "Solar photovoltaic": "Solar photovoltaic",
    "Solar thermal": "Concentrated solar power",
}

# Sheets with country-level LCOE, and the rows to skip to reach their header.
# NOTE: Only onshore wind and solar photovoltaic have country-level data.
COUNTRY_LCOE_SHEETS = {
    "Onshore wind": {"sheet": "Fig 2.11", "skiprows": 6, "title_contains": "onshore wind"},
    "Solar photovoltaic": {"sheet": "Fig. 3.7", "skiprows": 2, "title_contains": "solar pv"},
}
# Factor by which the median country-level LCOE is allowed to differ from the global weighted average of the same
# technology, before we consider that the country-level data is not in the same unit as the global one.
# NOTE: We cannot check the unit stated in the country-level sheets: figure 3.7 states none, and figure 2.11 states
#  "USD/kWh" even though its values are in USD/MWh (the same kind of typo that affected sheet 6.1 of the previous
#  release). A magnitude check is what actually protects us from a unit change, since the two units differ by 1000.
MAX_COUNTRY_TO_GLOBAL_LCOE_RATIO = 5

# Sheet with the yearly solar PV module prices, and the rows to skip to reach its header.
SOLAR_PV_MODULE_PRICES_SHEET = "Fig B3.1b"
SOLAR_PV_MODULE_PRICES_SKIPROWS = 5
# Module technologies given in that sheet.
PV_TECHNOLOGIES = ["High efficiency", "Mainstream", "Low cost", "Bifacial", "Full black"]


def extract_global_cost_for_all_sources_from_excel_file(data: pr.ExcelFile) -> Table:
    """Extract global weighted-average LCOE of all energy sources from the excel file.

    Parameters
    ----------
    data : pr.ExcelFile
        Raw data.

    Returns
    -------
    tb : Table
        LCOE for different energy sources.
    """
    error = "The file format for the global weighted-average LCOE has changed."
    assert "LCOE of renewable power technologies" in str(data.parse(GLOBAL_LCOE_SHEET).columns[0]), error
    assert data.parse(GLOBAL_LCOE_SHEET, skiprows=2).columns[1] == f"LCOE ({EXPECTED_LCOE_UNIT})", error

    tb = data.parse(GLOBAL_LCOE_SHEET, skiprows=5).dropna(how="all", axis=1).dropna(how="all")
    assert tb.columns[0] == "Technology", error

    # Check that the technologies are the ones we expect, before renaming them.
    assert set(tb["Technology"]) == set(GLOBAL_LCOE_TECHNOLOGIES), error
    tb["Technology"] = tb["Technology"].astype(str).replace(GLOBAL_LCOE_TECHNOLOGIES)

    tb = tb.rename(columns={"Technology": "technology"}, errors="raise").melt(
        id_vars="technology", var_name="year", value_name="cost"
    )

    # Add country column.
    tb["country"] = "World"

    return tb


def extract_country_cost_from_excel_file(data: pr.ExcelFile) -> Table:
    """Extract weighted-average LCOE of certain countries and certain energy sources from the excel file.

    Only onshore wind and solar photovoltaic have this data, and only for specific countries.

    Parameters
    ----------
    data : pr.ExcelFile
        Raw data.

    Returns
    -------
    tb : Table
        LCOE for different energy sources.
    """
    tables = []
    for technology, spec in COUNTRY_LCOE_SHEETS.items():
        sheet = str(spec["sheet"])
        error = f"The file format for country-level {technology} LCOE has changed."
        # NOTE: The titles of these sheets mention only the last two years, but they contain the full time series.
        assert str(spec["title_contains"]) in str(data.parse(sheet).columns[0]).lower(), error

        tb = data.parse(sheet, skiprows=int(spec["skiprows"])).dropna(how="all", axis=1).dropna(how="all")
        # The first column contains the country names (it is sometimes unnamed).
        tb = tb.rename(columns={tb.columns[0]: "country"}, errors="raise")
        # Drop repeated country columns (the sheets repeat them next to the percentage-change columns).
        tb = tb.drop(columns=[column for column in tb.columns if str(column).startswith("Country")], errors="ignore")
        tb = tb.melt(id_vars="country", var_name="year", value_name="cost")
        # Keep only LCOE columns, dropping the percentage-change columns and empty rows.
        tb = tb[~tb["year"].astype(str).str.startswith("%")].dropna().reset_index(drop=True)
        tb["technology"] = technology
        tables.append(tb)

    combined = pr.concat(tables, ignore_index=True)

    # To avoid duplicate rows, fix country name (it's Türkiye in these sheets, but Turkey elsewhere).
    error = "Name of Turkey has changed."
    assert ("Türkiye" in set(combined["country"])) and "Turkey" not in set(combined["country"]), error
    combined["country"] = combined["country"].astype(str).replace({"Türkiye": "Turkey"})

    return combined


def sanity_check_country_lcoe_units(tb_costs_global: Table, tb_costs_national: Table) -> None:
    # The country-level sheets do not reliably state their unit (see MAX_COUNTRY_TO_GLOBAL_LCOE_RATIO), so check that
    # their values are of the same order of magnitude as the global weighted average of the same technology.
    for technology in set(tb_costs_national["technology"]):
        national = tb_costs_national[tb_costs_national["technology"] == technology]["cost"].median()
        global_ = tb_costs_global[tb_costs_global["technology"] == technology]["cost"].median()
        ratio = national / global_
        error = (
            f"Country-level {technology} LCOE (median {national:.2f}) is not of the same order of magnitude as the "
            f"global weighted average (median {global_:.2f}). The country-level sheet may have changed unit."
        )
        assert 1 / MAX_COUNTRY_TO_GLOBAL_LCOE_RATIO < ratio < MAX_COUNTRY_TO_GLOBAL_LCOE_RATIO, error


def combine_global_and_national_data(tb_costs_global: Table, tb_costs_national: Table) -> Table:
    # Combine global and national data.
    tb_combined = pr.concat([tb_costs_global, tb_costs_national], ignore_index=True).astype({"year": int})

    # Convert from the producer's unit to the unit we publish.
    tb_combined["cost"] /= MWH_TO_KWH

    # Convert from long to wide format.
    tb_combined = tb_combined.pivot(
        index=["country", "year"], columns="technology", values="cost", join_column_levels_with="_"
    )

    # Improve table format.
    tb_combined = tb_combined.format(sort_columns=True)

    # Add units.
    for column in tb_combined.columns:
        tb_combined[column].metadata.unit = f"constant {EXPECTED_DOLLAR_YEAR} US$ per kilowatt-hour"
        tb_combined[column].metadata.short_unit = "$/kWh"
        tb_combined[
            column
        ].metadata.description_short = "This data is expressed in US dollars per kilowatt-hour. It is adjusted for inflation but does not account for differences in living costs between countries."

    return tb_combined


def prepare_solar_pv_module_prices(data: pr.ExcelFile) -> Table:
    """Prepare yearly data on average solar photovoltaic module prices, by module technology.

    NOTE: Prices used to be given monthly, for a set of technologies that included a global price index. Since the
    "Renewable Power Generation Costs in 2025" release they are given yearly, only from 2020 on, and only for
    module technologies sold in Europe. The series is selected in the garden step.

    Parameters
    ----------
    data : pr.ExcelFile
        Raw data.

    Returns
    -------
    pv_prices : Table
        PV module prices.

    """
    error = "The file format for solar PV module prices has changed."
    assert "solar PV module prices by technology" in str(data.parse(SOLAR_PV_MODULE_PRICES_SHEET).columns[0]), error
    # The unit is given in a cell above the data block, whose column is not fixed.
    assert EXPECTED_SOLAR_PV_MODULE_COST_UNIT in [
        str(column) for column in data.parse(SOLAR_PV_MODULE_PRICES_SHEET, skiprows=2).columns
    ], error

    pv_prices = (
        data.parse(SOLAR_PV_MODULE_PRICES_SHEET, skiprows=SOLAR_PV_MODULE_PRICES_SKIPROWS)
        .dropna(how="all", axis=1)
        .dropna(how="all")
    )
    pv_prices = pv_prices.rename(columns={pv_prices.columns[0]: "technology"}, errors="raise")

    # The sheet also contains a second block, with percentage changes. Keep only the rows of the first block, namely
    # the ones for the module technologies.
    error = "Names of solar PV module technologies have changed."
    assert set(PV_TECHNOLOGIES) <= set(pv_prices["technology"].dropna()), error
    pv_prices = pv_prices[pv_prices["technology"].isin(PV_TECHNOLOGIES)].reset_index(drop=True)
    # The first block is followed by the percentage-change block, which repeats the technology names.
    pv_prices = pv_prices.iloc[: len(PV_TECHNOLOGIES)].reset_index(drop=True)

    pv_prices = pv_prices.melt(id_vars="technology", var_name="year", value_name="cost").dropna(subset="cost")

    # Drop incomplete years (the sheet ends with a partial year, e.g. "Q1 2026").
    error = "Expected only complete years, plus possibly a trailing incomplete quarter."
    n_before = len(pv_prices)
    pv_prices = pv_prices[pv_prices["year"].astype(str).str.fullmatch(r"\d{4}")].reset_index(drop=True)
    assert n_before - len(pv_prices) <= len(PV_TECHNOLOGIES), error
    pv_prices = pv_prices.astype({"year": int})

    # Add column for region.
    pv_prices = pv_prices.assign(**{"country": "World"})

    # Improve table formatting.
    pv_prices = pv_prices.format(
        keys=["country", "year", "technology"], sort_columns=True, short_name="solar_photovoltaic_module_prices"
    )

    # Add units.
    pv_prices["cost"].metadata.unit = f"constant {EXPECTED_DOLLAR_YEAR} US$ per watt"
    pv_prices["cost"].metadata.short_unit = "$/W"
    pv_prices[
        "cost"
    ].metadata.description_short = "This data is expressed in US dollars per watt, adjusted for inflation."

    return pv_prices


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("renewable_power_generation_costs.xlsx")
    data = snap.ExcelFile()

    # Extract global, weighted-average LCOE cost for all energy sources.
    tb_costs_global = extract_global_cost_for_all_sources_from_excel_file(data=data)

    # Extract national LCOE for specific countries and technologies.
    tb_costs_national = extract_country_cost_from_excel_file(data=data)

    # Check that global and national data are given in the same unit.
    sanity_check_country_lcoe_units(tb_costs_global=tb_costs_global, tb_costs_national=tb_costs_national)

    # Combine global and national data.
    # NOTE: For convenience, we will also add units and a short description here (instead of in the garden step).
    tb_combined = combine_global_and_national_data(tb_costs_global=tb_costs_global, tb_costs_national=tb_costs_national)

    # Extract global data on solar photovoltaic module prices.
    # NOTE: For convenience, we will also add units and a short description here (instead of in the garden step).
    tb_solar_pv_prices = prepare_solar_pv_module_prices(data=data)

    #
    # Save outputs.
    #
    # Create a new Meadow dataset.
    ds = paths.create_dataset(tables=[tb_combined, tb_solar_pv_prices], default_metadata=snap.metadata)
    ds.save()
