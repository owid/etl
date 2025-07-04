"""Load snapshots of data files of the Statistical Review of World Energy, and create a meadow dataset."""

import re

import owid.catalog.processing as pr
from owid.catalog import License, Origin, Table, VariableMeta
from owid.datautils.dataframes import map_series

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# The following list will contain the (sheet name, indicator name, unit) for each relevant sheet in the main excel file.
# Here, indicator name is the name of the indicator given in that sheet, as it appears in the consolidated datasets.
# And unit is the one as it appears in the corresponding sheet.
SHEET_INDICATOR_UNITS = [
    # The EI used to provide primary energy consumption in their consolidated dataset.
    # But now they have moved to using Total Energy Supply.
    # They still have primary energy consumption, but only in the excel file, so I'll get it from there.
    ("Total Energy Supply (TES) -EJ", "tes_ej", "Exajoules"),
    # ('TES by fuel', 'UNKNOWN', 'Exajoules'),
    # ('TES per Capita', 'tes_gj_pc', 'Gigajoule per capita'),
    ("Natural Gas Flaring", "gasflared_bcm", "Billion cubic metres"),
    ("Oil Production - barrels", "oilprod_kbd", "Thousand barrels daily"),
    ("Oil Production - tonnes", "oilprod_mt", "Million tonnes"),
    ("Crude+cond production - barrels", "oilprod_crudecond_kbd", "Thousand barrels daily"),
    ("NGLs production - barrels", "oilprod_ngl_kbd", "Thousand barrels daily"),
    ("Liquids Consumption - barrels", "liqcons_kbd", "Thousand barrels daily"),
    ("Oil Consumption - barrels", "oilcons_kbd", "Thousand barrels daily"),
    ("Oil Consumption - Tonnes", "oilcons_mt", "Million tonnes"),
    ("Oil Consumption - EJ", "oilcons_ej", "Exajoules"),
    ("Oil refinery - throughput", "refthru_kbd", "Thousand barrels daily*"),
    ("Oil refinery - capacity", "refcap_kbd", "Thousand barrels daily*"),
    ("Gas Production - Bcm", "gasprod_bcm", "Billion cubic metres"),
    ("Gas Production - bcf", "gasprod_bcfd", "Billion cubic feet per day"),
    ("Gas Production - EJ", "gasprod_ej", "Exajoules"),
    ("Gas Consumption - Bcm", "gascons_bcm", "Billion cubic metres"),
    ("Gas Consumption - Bcf", "gascons_bcfd", "Billion cubic feet per day"),
    ("Gas Consumption - EJ", "gascons_ej", "Exajoules"),
    # ('Gas - H2 Production Capacity', 'hydrogen', 'Production Capacity (Thousand tonnes/yr)'),
    ("Coal Production - mt", "coalprod_mt", "Million tonnes "),
    ("Coal Production - EJ", "coalprod_ej", "Exajoules"),
    ("Coal Consumption - EJ", "coalcons_ej", "Exajoules"),
    ("Nuclear Generation - TWh", "nuclear_twh", "Terawatt-hours"),
    ("Nuclear Consumption - EJ", "nuclear_ej", "Exajoules"),
    ("Hydro Generation - TWh", "hydro_twh", "Terawatt-hours"),
    ("Hydro Consumption - EJ", "hydro_ej", "Exajoules"),
    ("Renewables Consumption -EJ", "renewables_ej", "Exajoules"),
    ("Renewable Power (inc hydro) -EJ", "ren_power_ej", "Exajoules"),
    ("Renewable Power (inc hydro)-TWh", "ren_power_twh", "Terawatt-hours"),
    # ("Renewables Generation by Source", "electbyfuel_ren_power", "Terawatt-hours"),
    ("Solar Generation - TWh", "solar_twh", "Terawatt-hours"),
    ("Solar Consumption - EJ", "solar_ej", "Exajoules"),
    # ('Solar Installed Capacity', 'UNKNOWN', 'nan'),
    ("Wind Generation - TWh", "wind_twh", "Terawatt-hours"),
    ("Wind Consumption - EJ", "wind_ej", "Exajoules "),
    # ('Wind Installed Capacity', 'UNKNOWN', 'nan'),
    ("Geo Biomass Other - TWh", "biogeo_twh", "Terawatt-hours"),
    ("Geo Biomass Other - EJ", "biogeo_ej", "Exajoules"),
    # NOTE: Biofuels sheets contain total, biogasoline and biodiesel one above the other. So they need to be handled separately.
    # ('Biofuels production - kboed', 'biofuels_prod_kboed', 'Thousand barrels of oil equivalent per day'),
    # ('Biofuels production - PJ', 'biofuels_prod_pj', 'Petajoules'),
    # ('Biofuels Consumption - kboed', 'biofuels_cons_kboed', 'Thousand barrels of oil equivalent per day'),
    # ('Biofuels consumption - PJ', 'biofuels_cons_pj', 'Petajoules'),
    ("Electricity Generation - TWh", "elect_twh", "Terawatt-hours"),
    # ("Elec generation by fuel", "electbyfuel_total", "Terawatt-hours"),
    ("Oil inputs - Elec generation ", "electbyfuel_oil", "Terawatt-hours"),
    ("Gas inputs - Elec generation", "electbyfuel_gas", "Terawatt-hours"),
    ("Coal inputs - Elec generation ", "electbyfuel_coal", "Terawatt-hours"),
    ("Other inputs - Elec generation", "electbyfuel_other", "Terawatt-hours"),
    # ('Grid Scale BESS Capacity', 'UNKNOWN', 'Installed Capacity (Gigawatts)'),
    ("Cobalt P-R", "cobalt_kt", "Thousand tonnes"),
    ("Lithium P-R", "lithium_kt", "Thousand tonnes of Lithium content"),
    ("Natural Graphite P-R", "graphite_kt", "Thousand tonnes"),
    ("Rare Earth metals P-R", "rareearths_kt", "Thousand tonnes1"),
    # ('Copper P-R', 'UNKNOWN', 'Thousand tonnes'),
    # ('Manganese P-R', 'UNKNOWN', 'Thousand tonnes'),
    # ('Nickel P-R', 'UNKNOWN', 'Thousand tonnes'),
    # ('Zinc P-R', 'UNKNOWN', 'Thousand tonnes'),
    # ('Platinum Group Metals P-R', 'UNKNOWN', 'Thousand tonnes'),
    # ('Bauxite P-R', 'UNKNOWN', 'Thousand tonnes'),
    # ('Aluminium P-R', 'UNKNOWN', 'Thousand tonnes'),
    # ('Tin P-R', 'UNKNOWN', 'Thousand tonnes'),
    # ('Vanadium P-R', 'UNKNOWN', 'Thousand tonnes'),
    # ('Mineral Commodity Prices', 'UNKNOWN', 'nan'),
    ("Primary Energy Cons (old meth)", "primary_ej", "Exajoules"),
    # ('PE Cons by fuel (old meth) ', 'UNKNOWN', 'Exajoules'),
]


def parse_sheet(data_additional: pr.ExcelFile, sheet: str, indicator: str, unit: str) -> Table:
    tb = data_additional.parse(sheet, skiprows=2)

    # Remove empty rows and rows of footnotes.
    tb = tb.dropna(subset=tb.columns[1:], how="all").reset_index(drop=True)

    # Sanity check.
    assert tb.columns[0] == unit, f"Unit {unit} or format of sheet '{sheet}' has changed"

    # Transpose table.
    tb = tb.rename(columns={unit: "country"})

    # Drop columns of growth and share.
    tb = tb[[column for column in tb.columns if isinstance(column, int) or (column == "country")]]

    # Clean country names.
    tb["country"] = (
        tb["country"]
        .str.replace(" #", "")
        .str.replace("of which: ", "")
        .str.replace(r"\d+$", "", regex=True)
        .str.strip()
    )

    # Transpose table.
    tb = tb.melt(id_vars=["country"], var_name="year", value_name=indicator)

    # Fix dtypes.
    tb = tb.astype({"year": "Int64", indicator: "Float64"})

    # For consistency with all other tables, rename some countries to their most common names in the dataset.
    # Their names will be properly harmonized in the garden step.
    country_mapping = {
        "Central America": "Total Central America",
        "Eastern Africa": "Total Eastern Africa",
        "European Union": "Total EU",
        "Middle Africa": "Total Middle Africa",
        "Non-OECD": "Total Non-OECD",
        "OECD": "Total OECD",
        "OPEC": "Total OPEC",
        "Non-OPEC": "Total Non-OPEC",
        "Turkey": "Turkiye",
        "Western Africa": "Total Western Africa",
        "DR Congo": "Democratic Republic of Congo",
        "Other North Africa": "Other Northern Africa",
    }
    tb["country"] = map_series(
        tb["country"],
        country_mapping,
        warn_on_missing_mappings=False,
        warn_on_unused_mappings=False,
        show_full_warning=True,
    )

    # Drop any remaining rows with no data.
    tb = tb.dropna(subset=[indicator]).reset_index(drop=True)

    return tb


def parse_coal_reserves(data: pr.ExcelFile) -> Table:
    sheet_name = "Coal - Reserves"
    # Unfortunately, using header=[...] doesn't work, so the header must be extracted in a different way.
    tb = data.parse(sheet_name, skiprows=3)

    # The year of the data is written in the header of the sheet.
    # Extract it using a regular expression.
    _year = re.findall(r"\d{4}", tb.columns[0])  # type: ignore
    assert len(_year) == 1, f"Year could not be extracted from the header of the sheet {sheet_name}."
    year = int(_year[0])

    # Re-create the original column names, assuming the zeroth column is for countries.
    new_columns = ["country"] + [
        "Coal reserves - " + " ".join(tb[column].iloc[0:2].fillna("").astype(str).tolist()).strip()
        for column in tb.columns[1:]
    ]
    tb = tb.rename(columns={column: new_columns[i] for i, column in enumerate(tb.columns)}, errors="raise")

    # The units should be written in the header of the first column.
    assert tb.iloc[1, 0] == "Million tonnes", f"Units (or sheet format) may have changed in sheet {sheet_name}"
    # Zeroth column should correspond to countries.
    assert "Total World" in tb["country"].tolist()

    # Drop header rows.
    tb = tb.drop([0, 1, 2]).reset_index(drop=True)

    # Drop empty columns and rows.
    tb = tb.dropna(axis=1, how="all")
    tb = tb.dropna(how="all").reset_index(drop=True)

    # There are many rows of footers at the end, occupying values of the zeroth column.
    # Remove all those rows, for which all columns are nan except the country column.
    tb = tb.dropna(subset=tb.drop(columns="country").columns, how="all")

    # Clean country names (remove spurious spaces and "of which: " in one of the last rows).
    tb["country"] = tb["country"].str.replace("of which:", "").str.strip()

    # For consistency with all other tables, rename some countries to their most common names in the dataset.
    # Their names will be properly harmonized in the garden step.
    # For consistency with all other tables, rename some countries to their most common names in the dataset.
    # Their names will be properly harmonized in the garden step.
    country_mapping = {
        "European Union": "Total EU",
        "Middle East": "Total Middle East",
        "Non-OECD": "Total Non-OECD",
        "OECD": "Total OECD",
        "Turkey": "Turkiye",
    }
    tb["country"] = map_series(tb["country"], country_mapping)

    # Add a column for the year of the data.
    tb = tb.assign(**{"year": year})

    # Ensure index columns are not repeated, and sort rows and columns conveniently.
    tb = tb.format()

    return tb


def parse_oil_reserves(data: pr.ExcelFile) -> Table:
    sheet_name = "Oil - Proved reserves history"
    # Unfortunately, using header=[...] doesn't work, so the header must be extracted in a different way.
    tb = data.parse(sheet_name, skiprows=4)

    # Check that units are the expected ones.
    assert (
        tb.columns[0] == "Thousand million barrels"
    ), f"Units (or sheet format) may have changed in sheet {sheet_name}"
    # Check that zeroth column should correspond to countries.
    assert "Total World" in tb[tb.columns[0]].tolist()

    # Rename country column.
    tb.columns = ["country"] + tb.columns[1:].tolist()

    # The last few columns show growth rate and share of reserves; they will be parsed with a name that does not correspond to a year.
    # To remove them, drop all columns that are not either "country" or a year.
    tb = tb.drop(
        columns=[column for column in tb.columns if re.match(r"^\d{4}$", str(column)) is None if column != "country"]
    )

    # Drop empty columns and rows.
    tb = tb.dropna(axis=1, how="all")
    tb = tb.dropna(how="all").reset_index(drop=True)

    # There are many rows of footers at the end, occupying values of the zeroth column.
    # Remove all those rows, for which all columns are nan except the country column.
    tb = tb.dropna(subset=tb.drop(columns="country").columns, how="all")

    # Clean country names (remove spurious spaces and "of which: " in one of the last rows).
    tb["country"] = tb["country"].str.replace("of which:", "").str.strip()

    # For consistency with all other tables, rename some countries to their most common names in the dataset.
    # Their names will be properly harmonized in the garden step.
    # For consistency with all other tables, rename some countries to their most common names in the dataset.
    # Their names will be properly harmonized in the garden step.
    country_mapping = {
        "European Union#": "Total EU",
        "Non-OECD": "Total Non-OECD",
        "Non-OPEC": "Total Non-OPEC",
        "OECD": "Total OECD",
        "OPEC": "Total OPEC",
    }
    tb["country"] = map_series(tb["country"], country_mapping, warn_on_unused_mappings=True, show_full_warning=True)

    # Transpose table to have a year column.
    tb = tb.melt(id_vars=["country"], var_name="year", value_name="oil_reserves_bbl")

    # Ensure numeric column has the right format.
    tb = tb.astype({"oil_reserves_bbl": float})

    # Ensure index columns are not repeated, and sort rows and columns conveniently.
    tb = tb.format()

    return tb


def parse_gas_reserves(data: pr.ExcelFile) -> Table:
    sheet_name = "Gas - Proved reserves history "
    # Unfortunately, using header=[...] doesn't work, so the header must be extracted in a different way.
    tb = data.parse(sheet_name, skiprows=4)

    # Check that units are the expected ones.
    assert tb.columns[0] == "Trillion cubic metres", f"Units (or sheet format) may have changed in sheet {sheet_name}"
    # Check that zeroth column should correspond to countries.
    assert "Total World" in tb[tb.columns[0]].tolist()

    # Rename country column.
    tb.columns = ["country"] + tb.columns[1:].tolist()

    # The last few columns show growth rate and share of reserves; they will be parsed with a name that does not correspond to a year.
    # To remove them, drop all columns that are not either "country" or a year.
    tb = tb.drop(
        columns=[column for column in tb.columns if re.match(r"^\d{4}$", str(column)) is None if column != "country"]
    )

    # Drop empty columns and rows.
    tb = tb.dropna(axis=1, how="all")
    tb = tb.dropna(how="all").reset_index(drop=True)

    # There are many rows of footers at the end, occupying values of the zeroth column.
    # Remove all those rows, for which all columns are nan except the country column.
    tb = tb.dropna(subset=tb.drop(columns="country").columns, how="all")

    # Clean country names (remove spurious spaces and "of which: " in one of the last rows).
    tb["country"] = tb["country"].str.replace("of which:", "").str.strip()

    # For consistency with all other tables, rename some countries to their most common names in the dataset.
    # Their names will be properly harmonized in the garden step.
    # For consistency with all other tables, rename some countries to their most common names in the dataset.
    # Their names will be properly harmonized in the garden step.
    country_mapping = {
        "European Union": "Total EU",
        "Non-OECD": "Total Non-OECD",
        "OECD": "Total OECD",
    }
    tb["country"] = map_series(tb["country"], country_mapping, warn_on_unused_mappings=True, show_full_warning=True)

    # Transpose table to have a year column.
    tb = tb.melt(id_vars=["country"], var_name="year", value_name="gas_reserves_tcm")

    # Ensure numeric column has the right format.
    tb = tb.astype({"gas_reserves_tcm": float})

    # Ensure index columns are not repeated, and sort rows and columns conveniently.
    tb = tb.format()

    return tb


def parse_oil_spot_crude_prices(data: pr.ExcelFile):
    sheet_name = "Spot crude prices"
    # Unfortunately, using header=[...] doesn't work, so the header must be extracted in a different way.
    tb = data.parse(sheet_name, skiprows=0, na_values=["-"])

    # Re-create the original column names, assuming the zeroth column is for years.
    tb.columns = ["year"] + [
        "Oil spot crude prices - " + " ".join(tb[column].iloc[0:2].fillna("").astype(str).tolist()).strip()
        for column in tb.columns[1:]
    ]

    # The units should be written in the header of the first column.
    assert tb.iloc[2, 0] == "US dollars per barrel", f"Units (or sheet format) may have changed in sheet {sheet_name}"

    # Drop header rows.
    tb = tb.drop([0, 1, 2, 3]).reset_index(drop=True)

    # Drop empty columns and rows.
    tb = tb.dropna(axis=1, how="all")
    tb = tb.dropna(how="all").reset_index(drop=True)

    # There are many rows of footers at the end, occupying values of the zeroth column.
    # Remove all those rows, for which all columns are nan except the country column.
    tb = tb.dropna(subset=tb.drop(columns=tb.columns[0]).columns, how="all")

    # Ensure index columns are not repeated, and sort rows and columns conveniently.
    tb = tb.format(keys=["year"])

    return tb


def parse_oil_crude_prices(data: pr.ExcelFile):
    sheet_name = "Oil crude prices since 1861"
    # Unfortunately, using header=[...] doesn't work, so the header must be extracted in a different way.
    tb = data.parse(sheet_name, skiprows=3, na_values=["-"])

    # Rename columns.
    tb.columns = ["year"] + ["Oil crude prices - " + column for column in tb.columns[1:]]

    # Drop empty columns and rows.
    tb = tb.dropna(axis=1, how="all")
    tb = tb.dropna(how="all").reset_index(drop=True)

    # There are many rows of footers at the end, occupying values of the zeroth column.
    # Remove all those rows, for which all columns are nan except the country column.
    tb = tb.dropna(subset=tb.drop(columns=tb.columns[0]).columns, how="all")

    # Ensure index columns are not repeated, and sort rows and columns conveniently.
    tb = tb.format(keys=["year"])

    return tb


def parse_gas_prices(data: pr.ExcelFile) -> Table:
    sheet_name = "Gas Prices "
    # Unfortunately, using header=[...] doesn't work, so the header must be extracted in a different way.
    tb = data.parse(sheet_name, skiprows=0, na_values="-")

    # Re-create the original column names, assuming the zeroth column is for countries.
    tb.iloc[0] = tb.iloc[0].ffill()
    units = tb.iloc[1].ffill()
    error = f"Units (or sheet format) may have changed in sheet {sheet_name}"
    assert set(units.dropna()) == {"$/kg", "$/mt", "US dollars per million Btu"}, error
    tb = tb.drop(1)
    tb.columns = ["year"] + [
        "  - ".join(tb[column].iloc[0:3].fillna("").astype(str).tolist()).strip() for column in tb.columns[1:]
    ]
    # Remove numbers from column names (they are references to footnotes).
    tb.columns = [re.sub(r"\d", "", column).strip().strip(" -").replace("\n", " ") for column in tb.columns]
    # Remove spurious spaces.
    tb.columns = [re.sub(r"\s{2,3}", " ", column) for column in tb.columns]

    # Drop header rows.
    tb = tb.drop([0, 2, 3]).reset_index(drop=True)

    # Drop empty columns and rows.
    tb = tb.dropna(axis=1, how="all")
    tb = tb.dropna(how="all").reset_index(drop=True)

    # There are many rows of footers at the end, occupying values of the zeroth column.
    # Remove all those rows, for which all columns are nan except the country column.
    tb = tb.dropna(subset=tb.drop(columns=tb.columns[0]).columns, how="all")

    # Ensure index columns are not repeated, and sort rows and columns conveniently.
    tb = tb.format(keys=["year"])

    return tb


def parse_coal_prices(data: pr.ExcelFile) -> Table:
    sheet_name = "Coal & Uranium - Prices"
    # Unfortunately, using header=[...] doesn't work, so the header must be extracted in a different way.
    tb = data.parse(sheet_name, skiprows=0, na_values=["-"])

    # Re-create the original column names, assuming the zeroth column is for countries.
    tb.iloc[0] = tb.iloc[0].ffill()
    units = tb.iloc[1].ffill()
    error = f"Units (or sheet format) may have changed in sheet {sheet_name}"
    assert set(units.dropna()) == {"$/lb", "US dollars per tonne"}, error
    tb = tb.drop(1)
    tb.columns = ["year"] + [
        "  - ".join(tb[column].iloc[0:2].fillna("").astype(str).tolist()).strip() for column in tb.columns[1:]
    ]
    # Remove numbers from column names (they are references to footnotes).
    tb.columns = [re.sub(r"\d", "", column).strip().strip(" -").replace("\n", " ") for column in tb.columns]
    # Remove spurious spaces.
    tb.columns = [re.sub(r"\s{2,3}", " ", column) for column in tb.columns]

    # Drop unnecessary rows.
    tb = tb.drop([0, 2])

    # There are many rows of footers at the end, occupying values of the zeroth column.
    # Remove all those rows, for which all columns are nan except the country column.
    tb = tb.dropna(subset=tb.columns[1:], how="all").reset_index(drop=True)

    # Ensure index columns are not repeated, and sort rows and columns conveniently.
    tb = tb.format(keys=["year"])

    return tb


def parse_thermal_equivalent_efficiency(data: pr.ExcelFile, origin: Origin, license: License) -> Table:
    sheet_name = "Approximate conversion factors"
    # Unfortunately, using header=[...] doesn't work, so the header must be extracted in a different way.
    tb = data.parse(sheet_name, skiprows=1, na_values=["-"])

    # In the "Approximate conversion factors" sheet, there is a table, called
    # "Thermal equivalent efficiency factors used to convert non-fossil electricity to primary energy."
    # Find the row where that table starts.
    table_start_row = tb[tb.iloc[:, 0].astype(str).str.startswith("Year")].index

    assert (
        len(table_start_row) == 1
    ), "Table of thermal equivalent efficiency factors not found, format may have changed."

    # Select relevant rows and drop empty columns.
    tb = tb.iloc[table_start_row[0] :].dropna(axis=1, how="all").reset_index(drop=True)

    # Use the first row to name columns, and clean column names.
    tb.columns = [column.replace("*", "").strip() for column in tb.iloc[0].values]
    tb = tb.drop(0).reset_index(drop=True)

    # Attempt to get the efficiency of the range of years 1965 - 2000 from the last row.
    older_efficiency_value = tb.iloc[-1].dropna().values[0]
    year_start, year_end, efficiency_factor = re.findall(r"(\d{4}).*(\d{4}).*([\d\.]{4})%.*", older_efficiency_value)[0]

    # Create a table of old efficiencies.
    years = list(range(int(year_start), int(year_end) + 1))
    older_efficiency = Table({"year": years, "efficiency_factor": [float(efficiency_factor) * 0.01] * len(years)})

    # Drop the final row, that includes the efficiency of older years.
    tb = tb.drop(tb.iloc[-1].name).reset_index(drop=True)

    # The data is given in two columns, extract both and put together into one column.
    efficiency = tb[["Year"]].melt()[["value"]].rename(columns={"value": "year"})
    efficiency["efficiency_factor"] = tb[["Efficiency Factor"]].melt()["value"]

    # Concatenate older and curren values of efficiency.
    efficiency = pr.concat([older_efficiency, efficiency], ignore_index=True).dropna().reset_index(drop=True)

    # Sanity checks.
    assert efficiency["year"].diff().dropna().unique().tolist() == [
        1
    ], "Year columns for efficiency factors was not well extracted."
    assert (
        efficiency["efficiency_factor"].diff().dropna() >= 0
    ).all(), "Efficiency is expected to be monotonically increasing."
    assert (efficiency["efficiency_factor"] > 0.35).all() & (
        efficiency["efficiency_factor"] < 0.5
    ).all(), "Efficiency out of expected range."

    # Set an appropriate index and sort conveniently.
    efficiency = efficiency.format(keys=["year"], short_name="statistical_review_of_world_energy_efficiency_factors")

    # Prepare table and variable metadata.
    efficiency["efficiency_factor"].metadata = VariableMeta(
        title="Thermal equivalent efficiency factors",
        description="Thermal equivalent efficiency factors used to convert non-fossil electricity to primary energy.",
        origins=[origin],
        licenses=[license],
    )

    return efficiency


def combine_consolidated_dataset_and_spreadsheet(tb_consolidated, data_additional):
    tables = []
    for sheet, indicator, unit in SHEET_INDICATOR_UNITS:
        _table = parse_sheet(data_additional=data_additional, sheet=sheet, indicator=indicator, unit=unit)
        assert list(_table.columns) == ["country", "year", indicator]
        assert not _table.empty
        tables.append(_table)
    tb_additional = pr.multi_merge(tables, on=["country", "year"], how="outer")
    # Sanity checks.
    error = "Duplicated rows found when combining data extracted from sheets. Sheets format may have changed."
    assert tb_additional[tb_additional.duplicated(subset=["country", "year"])].empty, error
    error = (
        "Unexpected mismatch between entities extracted from the consolidated dataset and those from the spreadsheet."
    )
    assert set(tb_additional["country"]) - set(tb_consolidated["country"]) == set(), error
    # However, there are entities in the consolidated dataset which do have nonzero data, which is not present in the spreadsheet.
    # But they are all "Other*" regions, so we can also ignore them.
    # tb[tb["country"] == "Other Eastern Africa"].loc[:, subset.ne(0).any()]
    assert set(tb_consolidated["country"]) - set(tb_additional["country"]) == {
        "Other Eastern Africa",
        "Other Middle Africa",
        "Other North America",
        "Other Western Africa",
    }

    # Check that the only indicator that is in the spreadsheet and not in the consolidated dataset is primary energy consumption.
    error = "Expected only primary energy consumption to be in the spreadsheet but not in the consolidated dataset."
    assert set(tb_additional.columns) - set(tb_consolidated.columns) == {"primary_ej"}, error

    # Combine data from the consolidated dataset with the one from the spreadsheet.
    # If an indicator is in the spreadsheet, take that one, otherwise, take it from the consolidated dataset.
    tb_combined = tb_consolidated[
        [
            column
            for column in tb_consolidated.columns
            if column not in tb_additional.drop(columns=["country", "year"]).columns
        ]
    ].merge(tb_additional, on=["country", "year"], how="outer")

    return tb_combined


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshots.
    snap = paths.load_snapshot("statistical_review_of_world_energy.csv")
    snap_additional = paths.load_snapshot("statistical_review_of_world_energy.xlsx")

    # Most data comes from the wide-format csv file, and some additional variables from the excel file.
    tb_consolidated = snap.read(underscore=True)
    data_additional = snap_additional.ExcelFile()

    #
    # Process data.
    #
    # Extract most of the data from the main spreadsheet, and some from the consolidated dataset.
    # NOTE: It used to be the other way around, and we usted to rely mostly on the consolidated dataset.
    # But several important issues have been detected in the consolidated dataset (see docstring of snapshot).
    tb = combine_consolidated_dataset_and_spreadsheet(tb_consolidated=tb_consolidated, data_additional=data_additional)

    # Parse coal reserves sheet.
    tb_coal_reserves = parse_coal_reserves(data=data_additional)

    # Parse gas reserves sheet.
    tb_gas_reserves = parse_gas_reserves(data=data_additional)

    # Parse oil reserves sheet.
    tb_oil_reserves = parse_oil_reserves(data=data_additional)

    # Parse oil spot crude prices.
    tb_oil_spot_crude_prices = parse_oil_spot_crude_prices(data=data_additional)

    # Parse oil crude prices.
    tb_oil_crude_prices = parse_oil_crude_prices(data=data_additional)

    # Parse gas prices.
    tb_gas_prices = parse_gas_prices(data=data_additional)

    # Parse coal prices.
    tb_coal_prices = parse_coal_prices(data=data_additional)

    # Parse thermal equivalent efficiency factors.
    tb_efficiency_factors = parse_thermal_equivalent_efficiency(
        data=data_additional, origin=snap_additional.metadata.origin, license=snap_additional.metadata.license
    )

    # Combine main table and coal, gas, and oil reserves.
    tb = pr.multi_merge(
        [
            tb,
            tb_coal_reserves.reset_index(),
            tb_gas_reserves.reset_index(),
            tb_oil_reserves.reset_index(),
        ],
        how="outer",
        on=["country", "year"],
    )

    # Set an appropriate index and sort conveniently.
    tb = tb.format(sort_columns=True)

    # Create combined table of fossil fuel prices.
    tb_prices = pr.multi_merge(
        [
            tb_oil_spot_crude_prices.reset_index(),
            tb_oil_crude_prices.reset_index(),
            tb_gas_prices.reset_index(),
            tb_coal_prices.reset_index(),
        ],
        how="outer",
        on=["year"],
    )

    # Set an appropriate index and sort conveniently.
    tb_prices = tb_prices.format(
        keys=["year"], sort_columns=True, short_name="statistical_review_of_world_energy_prices"
    )

    # Prices variables need to cite S&P Global Platts, and include their license.
    year_published = tb_prices[tb_prices.columns[0]].metadata.origins[0].date_published[0:4]
    for column in tb_prices.columns:
        assert len(tb_prices[column].metadata.origins) == 1, f"Unexpected origins in column {column}"
        tb_prices[column].metadata.origins[
            0
        ].attribution = (
            f"Energy Institute based on S&P Global Platts - Statistical Review of World Energy ({year_published})"
        )
        tb_prices[column].metadata.origins[
            0
        ].citation_full = (
            f"Energy Institute based on S&P Global Platts - Statistical Review of World Energy ({year_published})"
        )
        tb_prices[column].metadata.licenses.append(License(name=f"© S&P Global Inc. {year_published}"))

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb, tb_prices, tb_efficiency_factors], default_metadata=snap.metadata)
    ds_meadow.save()
