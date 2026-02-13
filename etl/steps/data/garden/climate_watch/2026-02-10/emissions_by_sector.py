"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table, utils
from owid.datautils.dataframes import map_series

from etl.helpers import PathFinder

from structlog import get_logger

# Initialize log.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# All sectors expected in the data, and how to rename them.
SECTORS = {
    # Independent sectors (they do not include each other):
    "Land Use, Land-Use Change and Forestry": "Land-use change and forestry emissions",
    "Agriculture": "Agriculture emissions",
    "Industrial Processes": "Industry emissions",
    "Waste": "Waste emissions",
    "Building": "Buildings emissions",
    "Electricity/Heat": "Electricity and heat emissions",
    "Manufacturing/Construction": "Manufacturing and construction emissions",
    "Fugitive Emissions": "Fugitive emissions",
    "Other Fuel Combustion": "Other fuel combustion emissions",
    # Transportation and Bunker fuels (aviation and shipping) are a bit tricky. According to CW's methodology:
    # "Please note that transport emissions for world total includes international marine bunkers and international aviation bunkers, which are not included in transportation at a national or regional level."
    # Therefore:
    # - For individual countries, Transport (and hence Energy, and Total including/excluding LUCF) DO NOT include bunker fuels.
    # - For World, Transport (and hence Energy, and Total including/excluding LUCF) DO include bunker fuels.
    "Transportation": "Transport emissions",
    "Bunker Fuels": "Aviation and shipping emissions",
    # Energy is the sum of:
    # - Building.
    # - Electricity/Heat.
    # - Manufacturing/Construction.
    # - Fugitive Emissions.
    # - Other Fuel Combustion.
    # - Transportation (which includes Bunker Fuels for World; for countries, it does not).
    "Energy": "Energy emissions",
    # Total excluding LULUCF is the sum of:
    # - Agriculture.
    # - Industrial Processes.
    # - Waste.
    # - Energy, which is the sum of:
    #   - Building.
    #   - Electricity/Heat.
    #   - Manufacturing/Construction.
    #   - Fugitive Emissions.
    #   - Other Fuel Combustion.
    #   - Transportation (which includes Bunker Fuels for World; for countries, it does not).
    "Total excluding LULUCF": "Total emissions excluding LUCF",
    # Total including LULUCF is the sum of:
    # - Agriculture.
    # - Industrial Processes.
    # - Waste.
    # - Energy, which is the sum of:
    #   - Building.
    #   - Electricity/Heat.
    #   - Manufacturing/Construction.
    #   - Fugitive Emissions.
    #   - Other Fuel Combustion.
    #   - Transportation (which includes Bunker Fuels for World; for countries, it does not).
    # - Land Use, Land-Use Change and Forestry.
    "Total including LULUCF": "Total emissions including LUCF",
}

# Suffix to add to the name of per capita variables.
PER_CAPITA_SUFFIX = "_per_capita"

# Mapping of gas name (as given in Climate Watch data) to the name of the corresponding output table.
TABLE_NAMES = {
    "All GHG": "Greenhouse gas emissions by sector",
    "CH4": "Methane emissions by sector",
    "CO2": "Carbon dioxide emissions by sector",
    "F-Gas": "Fluorinated gas emissions by sector",
    "N2O": "Nitrous oxide emissions by sector",
}

# Aggregate regions to add, following OWID definitions.
REGIONS = {
    # Continents.
    "Africa": {},
    "Asia": {},
    "Europe": {},
    # The EU27 is already included in the original data, and after inspection the data coincides with our aggregate.
    # So we simply keep the original data for EU27 given in the data.
    "North America": {},
    "Oceania": {},
    "South America": {},
    # Income groups.
    "Low-income countries": {},
    "Upper-middle-income countries": {},
    "Lower-middle-income countries": {},
    "High-income countries": {},
}

# Convert million tonnes to tonnes (for non per capita indicators).
MT_TO_T = 1e6

# Convert million tonnes to kilograms (for per capita indicators).
MT_TO_KG = 1e9

# List of countries for which the sum of sectorial emissions don't add up to the total.
# The sum of their emissions from energy sectors do not add up to the Energy emissions either.
EXPECTED_COUNTRIES_WITH_ISSUES = ["Andorra", "East Timor", "Liechtenstein"]


def create_table_for_gas(tb: Table, gas: str) -> Table:
    """Extract data for a particular gas and create a table with variables' metadata.

    Parameters
    ----------
    tb : Table
    gas : str
        Name of gas to consider (as called in "gas" column of the original data).

    Returns
    -------
    table_gas : Table
        Table with data for considered gas, and metadata for each variable.

    """
    # Select data for current gas.
    tb_gas = tb[tb["gas"] == gas].drop(columns="gas").reset_index(drop=True)

    # Pivot table to have a column for each sector.
    tb_gas = tb_gas.pivot(index=["country", "year"], columns="sector", values="value", join_column_levels_with="_")

    # Create region aggregates for all columns (with a simple sum) except for the column of efficiency factors.
    aggregations = {
        column: "sum" for column in tb_gas.columns if column not in ["country", "year", "efficiency_factor"]
    }
    tb_gas = paths.regions.add_aggregates(
        tb=tb_gas,
        aggregations=aggregations,
        regions=REGIONS,
        min_frac_countries_informed=0.7,
        min_num_values_per_year=1,
    )

    # Add population to data.
    tb_gas = paths.regions.add_per_capita(tb=tb_gas, regions=REGIONS, suffix=PER_CAPITA_SUFFIX)

    # List columns with emissions data.
    emissions_columns = [column for column in tb_gas.columns if column not in ["country", "year"]]

    for variable in emissions_columns:
        if PER_CAPITA_SUFFIX in variable:
            # Adapt units of per capita indicators from million tonnes to kilograms.
            tb_gas[variable] *= MT_TO_KG
        else:
            # Adapt non per capita indicators from million tonnes to tonnes.
            tb_gas[variable] *= MT_TO_T

    # Remove rows and columns that only have nans.
    tb_gas = tb_gas.dropna(how="all", axis=1)
    tb_gas = tb_gas.dropna(subset=emissions_columns, how="all").reset_index(drop=True)

    # Adapt table title and short name.
    tb_gas.metadata.title = TABLE_NAMES[gas]
    tb_gas.metadata.short_name = utils.underscore(TABLE_NAMES[gas])

    # Adapt column names.
    tb_gas = tb_gas.rename(
        columns={
            column: utils.underscore(column)
            .replace("emissions", f"{utils.underscore(gas)}_emissions")
            .replace("all_ghg", "ghg")
            .replace("f_gas", "fgas")
            for column in tb_gas.columns
            if column not in ["country", "year"]
        }
    )

    # Improve table format.
    tb_gas = tb_gas.format(sort_columns=True)

    return tb_gas


def sanity_check_outputs(tb):
    # For World, transport includes bunker fuels (for individual countries, it does not).
    _tb = tb[
        (tb["country"] == "World") & (tb["sector"].isin(["Transport emissions", "Aviation and shipping emissions"]))
    ].pivot(index=["country", "year", "gas"], columns=["sector"], join_column_levels_with="_")
    assert _tb[_tb["value_Transport emissions"] < _tb["value_Aviation and shipping emissions"]].empty, error
    error = "For World, transport should always be larger than bunker fuels."

    # Check that Total * are the sum of all individual sectors.
    sector_lucf = "Land-use change and forestry emissions"
    sector_total_excl_lucf = "Total emissions excluding LUCF"
    sector_total_incl_lucf = "Total emissions including LUCF"
    # List of sectors that should add up to total excluding LULUCF.
    sectors_all_excl_lucf = [
        "Agriculture emissions",
        # Note that bunker fuels should not be included. They are included as part of Transport for World, but for individual countries, they are not included either in Transport, or in Totals.
        # 'Aviation and shipping emissions',
        "Buildings emissions",
        "Electricity and heat emissions",
        "Fugitive emissions",
        "Industry emissions",
        "Manufacturing and construction emissions",
        "Other fuel combustion emissions",
        "Transport emissions",
        "Waste emissions",
    ]
    sectors_all_incl_lucf = sectors_all_excl_lucf + [sector_lucf]
    # Initialise a new list (to check that the list of countries with issues is as expected).
    found_countries_with_issues = []
    for gas in sorted(set(tb["gas"])):
        for country in sorted(set(tb["country"])):
            _tb = tb[(tb["country"] == country) & (tb["gas"] == gas)].reset_index(drop=True)
            for case in ["excl. lucf", "incl. lucf"]:
                group = sectors_all_excl_lucf if case == "excl. lucf" else sectors_all_incl_lucf
                total = sector_total_excl_lucf if case == "excl. lucf" else sector_total_incl_lucf
                _tb_sum = (
                    _tb[(_tb["sector"].isin(group))]
                    .groupby("year", as_index=False)
                    .agg({"value": "sum"})
                    .assign(**{"source": f"sum {case}"})
                )
                _tb_total = _tb[(_tb["sector"] == total)].reset_index(drop=True).assign(**{"source": f"total {case}"})
                _tb_compared = _tb_sum.merge(_tb_total, on="year", suffixes=("_sum", "_total"))
                _tb_compared["pct_diff"] = 100 * (
                    abs(_tb_compared["value_sum"] - _tb_compared["value_total"]) / _tb_compared["value_total"]
                )
                _tb_compared["abs_diff"] = abs(_tb_compared["value_sum"] - _tb_compared["value_total"])
                # Ignore large percentage deviations on small numbers.
                _tb_issues = _tb_compared[(_tb_compared["abs_diff"] > 0.1) & (_tb_compared["pct_diff"] > 5)]
                if not _tb_issues.empty:
                    # Uncomment to plot countries with identified issues.
                    # import owid.catalog.processing as pr
                    # import plotly.express as px
                    # px.line(pr.concat([_tb_sum, _tb_total]), x="year", y="value", color="source", markers=True, title=f"{gas} - {country}").show()
                    if country not in EXPECTED_COUNTRIES_WITH_ISSUES:
                        error = f"Expected sum of sectors to add up to total excluding LUCF ({gas} - {country}: {_tb_issues['pct_diff'].max()}). Consider adding it to the list of countries with known issues."
                        log.error(error)
                    found_countries_with_issues.append(country)
    error = "Update the list of countries with issues."
    assert set(found_countries_with_issues) == set(EXPECTED_COUNTRIES_WITH_ISSUES), error

    # Check that Energy is the sum of all individual energy sectors.
    sector_energy = "Energy emissions"
    # List of sectors that should add up to Energy.
    sectors_all_energy = [
        # Note that bunker fuels should not be included. They are included as part of Transport for World, but for individual countries, they are not included either in Transport, or in Totals.
        # 'Aviation and shipping emissions',
        "Buildings emissions",
        "Electricity and heat emissions",
        "Fugitive emissions",
        "Manufacturing and construction emissions",
        "Other fuel combustion emissions",
        "Transport emissions",
    ]
    # Initialise a new list (to check that the list of countries with issues is as expected).
    found_countries_with_issues = []
    for gas in sorted(set(tb["gas"])):
        for country in sorted(set(tb["country"])):
            _tb = tb[(tb["country"] == country) & (tb["gas"] == gas)].reset_index(drop=True)
            _tb_sum = (
                _tb[(_tb["sector"].isin(sectors_all_energy))]
                .groupby("year", as_index=False)
                .agg({"value": "sum"})
                .assign(**{"source": f"sum"})
            )
            _tb_total = _tb[(_tb["sector"] == sector_energy)].reset_index(drop=True).assign(**{"source": f"total"})
            _tb_compared = _tb_sum.merge(_tb_total, on="year", suffixes=("_sum", "_total"))
            _tb_compared["pct_diff"] = 100 * (
                abs(_tb_compared["value_sum"] - _tb_compared["value_total"]) / _tb_compared["value_total"]
            )
            _tb_compared["abs_diff"] = abs(_tb_compared["value_sum"] - _tb_compared["value_total"])
            # Ignore large percentage deviations on small numbers.
            _tb_issues = _tb_compared[(_tb_compared["abs_diff"] > 0.1) & (_tb_compared["pct_diff"] > 5)]
            if not _tb_issues.empty:
                # Uncomment to plot countries with identified issues.
                # import owid.catalog.processing as pr
                # import plotly.express as px
                # px.line(pr.concat([_tb_sum, _tb_total]), x="year", y="value", color="source", markers=True, title=f"{gas} - {country}").show()
                if country not in EXPECTED_COUNTRIES_WITH_ISSUES:
                    error = f"Expected sum of energy sectors to add up to Energy ({gas} - {country}: {_tb_issues['pct_diff'].max()}). Consider adding it to the list of countries with known issues."
                    log.error(error)
                found_countries_with_issues.append(country)
    error = "Update the list of countries with issues."
    assert set(found_countries_with_issues) == set(EXPECTED_COUNTRIES_WITH_ISSUES), error


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("emissions_by_sector")
    tb = ds_meadow.read("emissions_by_sector")

    #
    # Process data.
    #
    # Select only one data source (Climate Watch).
    tb = tb[tb["data_source"] == "Climate Watch"].reset_index(drop=True)

    # Check that there is only one unit in dataset.
    assert set(tb["unit"]) == {"MtCO₂e"}, "Unknown units in dataset"

    # Remove unnecessary columns.
    tb = tb.drop(columns=["unit", "id", "data_source", "iso_code3"], errors="raise")

    # Rename sectors.
    tb["sector"] = map_series(
        series=tb["sector"],
        mapping=SECTORS,
        warn_on_missing_mappings=True,
        warn_on_unused_mappings=True,
        show_full_warning=True,
    )

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Sanity check outputs.
    sanity_check_outputs(tb=tb)

    # Create one table for each gas, and one for all gases combined.
    tables = [create_table_for_gas(tb=tb, gas=gas) for gas in tb["gas"].unique()]

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=tables)
    ds_garden.save()
