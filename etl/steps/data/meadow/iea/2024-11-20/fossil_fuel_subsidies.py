"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Assumed USD year.
DOLLAR_YEAR = 2023


def prepare_subsidies_by_country_table(tb_subsidies: Table) -> Table:
    # The table is split into two subtables: Above, global data, and below, country data. They are separated by an empty row.
    columns_total = tb_subsidies.loc[0].tolist()
    table_country_start_index = tb_subsidies[tb_subsidies[tb_subsidies.columns[0]] == "Country"].index[0]
    columns_countries = tb_subsidies.loc[table_country_start_index].tolist()
    error = "Subsidies by country sheet has changed."
    assert tb_subsidies.columns[0] == f"Unit: Real {DOLLAR_YEAR} million USD", error
    # Check that tables are aligned.
    assert columns_total[2:] == columns_countries[2:], error
    # Rename columns.
    columns = columns_countries[0:2] + [str(int(year)) for year in columns_countries[2:]]
    # Extract global subtable and assign country "World".
    tb_global = tb_subsidies.loc[1 : table_country_start_index - 1].dropna(how="all").reset_index(drop=True)
    tb_global = tb_global.rename(
        columns={old_column: new_column for old_column, new_column in zip(tb_global.columns, columns)}, errors="raise"
    )
    tb_global["Country"] = "World"
    tb_global["Product"] = tb_global["Product"].replace({"All Products": "Total", "Natural Gas": "Gas"})
    # Extract countries subtable.
    tb_countries = tb_subsidies.loc[table_country_start_index + 1 :].reset_index(drop=True)
    tb_countries = tb_countries.rename(
        columns={old_column: new_column for old_column, new_column in zip(tb_countries.columns, columns)},
        errors="raise",
    )
    # Combine both tables.
    tb = pr.concat([tb_global, tb_countries], ignore_index=True)
    # Transpose table.
    tb = tb.melt(id_vars=["Country", "Product"], var_name="Year", value_name="subsidy")

    # Improve format.
    tb = tb.format(["country", "year", "product"])

    return tb


def prepare_indicators_by_country_table(tb_indicators: Table) -> Table:
    # The year of the data is given in the very first cell. The actual table starts a few rows below.
    error = "Indicators by country sheet has changed."
    assert tb_indicators.columns[0] == f"Indicators for year {DOLLAR_YEAR}", error
    columns = {
        "Country": "country",
        "Average subsidisation rate (%)": "subsidization_rate",
        "Subsidy per capita ($/person)": "subsidy_per_capita",
        "Total subsidy as share of GDP (%)": "subsidy_as_share_of_gdp",
    }
    assert tb_indicators.loc[2].tolist() == list(columns), error
    tb_indicators = tb_indicators.loc[3:].reset_index(drop=True)
    tb_indicators = tb_indicators.rename(
        columns={old_column: new_column for old_column, new_column in zip(tb_indicators.columns, columns.values())},
        errors="raise",
    )
    # Add a year column.
    tb_indicators = tb_indicators.assign(**{"year": DOLLAR_YEAR})
    # Improve format.
    tb_indicators = tb_indicators.format(short_name="fossil_fuel_subsidies_indicators")

    return tb_indicators


def prepare_transport_oil_table(tb_transport: Table) -> Table:
    columns = ["country"] + [str(int(year)) for year in tb_transport.loc[0][1:].tolist()]
    error = "Transport Oil Subsidies sheet has changed."
    assert tb_transport.columns[0] == f"Unit: Real {DOLLAR_YEAR} million USD", error
    assert [column.isdigit() for column in columns[1:]], error
    tb_transport = tb_transport.loc[1:].reset_index(drop=True)
    tb_transport = tb_transport.rename(
        columns={old_column: new_column for old_column, new_column in zip(tb_transport.columns, columns)},
        errors="raise",
    )
    # Transpose table.
    tb_transport = tb_transport.melt(id_vars=["country"], var_name="year", value_name="transport_oil_subsidy")
    # Improve format.
    tb_transport = tb_transport.format(short_name="fossil_fuel_subsidies_transport_oil")

    return tb_transport


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("fossil_fuel_subsidies.xlsx")

    # Load data from all relevant sheets in the snapshot file.
    tb_subsidies = snap.read(sheet_name="Subsidies by country", skiprows=3)
    tb_indicators = snap.read(sheet_name="Indicators by country")
    tb_transport = snap.read(sheet_name="Transport Oil Subsidies", skiprows=3)

    #
    # Process data.
    #
    # Prepare "Subsidies by country" table.
    tb_subsidies = prepare_subsidies_by_country_table(tb_subsidies=tb_subsidies)

    # Prepare "Indicators by country" table.
    tb_indicators = prepare_indicators_by_country_table(tb_indicators=tb_indicators)

    # Prepare "Transport Oil Subsidies" table.
    tb_transport = prepare_transport_oil_table(tb_transport=tb_transport)

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb_subsidies, tb_indicators, tb_transport], default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
