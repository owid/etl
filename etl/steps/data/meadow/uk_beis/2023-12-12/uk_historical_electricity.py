"""Load a snapshot and create a meadow dataset."""

import warnings
from typing import Dict

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Ignore unnecessary warnings when loading the files.
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def prepare_data(tb: Table, columns: Dict[int, str], table_name: str) -> Table:
    """Prepare raw content of a specific sheet in the the BEIS excel file.

    It contains some sanity checks due to the poor formatting of the original file, and some basic processing (like
    removing footnote marks from the years, e.g. "2000 (5)" -> 2000). Duplicate rows are not removed.

    Parameters
    ----------
    tb : Table
        Input data from a specific sheet.
    columns : dict
        Columns to select from data, and how to rename them. The dictionary key should be the column number, and the
        value should be the new name for that column.
    table_name : str
        Short name of the output table.

    Returns
    -------
    tb : Table
        Clean data extracted, with proper column names.

    """
    tb = tb.copy()

    # Sanity checks.
    error = "File structure has changed."
    assert (tb.iloc[0]["Year"] == 1920) and (tb.iloc[-1]["Year"] >= 2022), error

    # Select columns and how to rename them.
    tb = tb[list(columns)].rename(columns=columns, errors="raise")

    # Remove all rows for which the year column does not start with an integer of 4 digits.
    tb = tb.loc[tb["year"].astype(str).str.contains(r"^\d{4}", regex=True, na=False)].reset_index(drop=True)
    # Remove annotations from years (e.g. replace "1987 (5)" by 1987).
    tb["year"] = tb["year"].astype(str).str[0:4].astype(int)

    # Make all columns float (except year column).
    error = "Dtypes of columns have changed."
    assert all(tb.drop(columns="year").dtypes == "float64"), error

    # Set an appropriate index and sort conveniently.
    # NOTE: We do not verify integrity because there are duplicated rows, that will be handled in the garden step.
    tb = tb.set_index(["year"], verify_integrity=False).sort_index().sort_index(axis=1)

    # Update table's short name.
    tb.metadata.short_name = table_name

    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("uk_historical_electricity.xls")

    # Load data from the two relevant sheets of the excel file.
    # The original excel file is poorly formatted and will be hard to parse automatically.
    data = snap.ExcelFile()
    tb_fuel_input = data.parse(sheet_name="Fuel Input", skiprows=5, na_values=["[x]"])
    tb_supply = data.parse(sheet_name="Supply, Availability & Consump", skiprows=5, na_values=["[x]"])
    tb_efficiency = data.parse(sheet_name="Generated and Supplied", skiprows=6, na_values=["[x]"])

    #
    # Process data.
    #
    # Process data from the sheet about fuels input for electricity generation.
    tb_fuel_input = prepare_data(
        tb=tb_fuel_input,
        columns={
            "Year": "year",
            "Total all fuels": "all_sources",
            "Coal": "coal",
            "Oil [note 2]": "oil",
            "Natural gas [note 3]": "gas",
            "Nuclear": "nuclear",
            "Natural flow hydro [note 4, note 5]": "hydro",
            "Wind and solar [note 4]": "wind_and_solar",
            # 'Coke and breeze [note 6]': "coke_and_breeze",
            "Other fuels [note 7]": "other",
            # 'Source notes': "notes",
        },
        table_name="fuel_input",
    )

    # Prepare data from the sheet about electricity supply, availability and consumption.
    tb_supply = prepare_data(
        tb=tb_supply,
        columns={
            "Year": "year",
            "Electricity supplied (net)": "electricity_generation",
            # "Purchases from other producers": "purchases_from_other_producers",
            "Net imports [note 1]": "net_imports",
            # "Electricity available": "electricity_available",
            # "Losses [note 2]": "losses__note_2",
            # "Total electricity consumed": "total_electricity_consumed",
            # "Fuel industries consumption": "fuel_industries_consumption",
            # "Industrial consumption [note 3]": "industrial_consumption__note_3",
            # "Domestic consumption": "domestic_consumption",
            # "Other users consumption [note 4]": "other_users_consumption__note_4",
            # "Total consumption by final users [note 5]": "total_consumption_by_final_users__note_5",
            # "Domestic and farm premises": "domestic_and_farm_premises",
            # "Shops, offices and other commercial premises": "shops__offices_and_other_commercial_premises",
            # "Factories and other industrial premises": "factories_and_other_industrial_premises",
            # "Public lighting": "public_lighting",
            # "Traction": "traction",
            # "Total": "total",
            # "Number of consumers [Note 6]": "number_of_consumers__note_6",
            # "Source notes": "source_notes",
        },
        table_name="supply",
    )

    # Prepare data from the sheet about electricity generated and supplied.
    tb_efficiency = prepare_data(
        tb=tb_efficiency,
        columns={
            "Year": "year",
            # "Electricity generated (major power producers)": "electricity_generated__major_power_producers",
            # "Electricity used on works (major power producers)": "electricity_used_on_works__major_power_producers",
            # "Total electricity supplied (gross, major power producers) [note 2]": "total_electricity_supplied__gross__major_power_producers__note_2",
            # "Supplied from conventional thermal and other (gross, major power producers) [note 3]": "supplied_from_conventional_thermal_and_other__gross__major_power_producers__note_3",
            # "Supplied from CCGT (gross, major power producers)": "supplied_from_ccgt__gross__major_power_producers",
            # "Supplied from nuclear (gross, major power producers)": "supplied_from_nuclear__gross__major_power_producers",
            # "Supplied from hydro natural flow (gross, major power producers)": "supplied_from_hydro_natural_flow__gross__major_power_producers",
            # "Supplied from hydro (gross, major power producers)": "supplied_from_hydro__gross__major_power_producers",
            # "Supplied from hydro pumped storage (gross, major power producers)": "supplied_from_hydro_pumped_storage__gross__major_power_producers",
            # "Supplied from wind and solar (gross, major power producers)": "supplied_from_wind_and_solar__gross__major_power_producers",
            # "Electricity used in pumping at pumped storage stations (major power producers)": "electricity_used_in_pumping_at_pumped_storage_stations__major_power_producers",
            # "Electricity supplied (net, major power producers) [note 4]": "electricity_supplied__net__major_power_producers__note_4",
            # "Electricity generated (other generators)": "electricity_generated__other_generators",
            # "Electricity used on works (other generators)": "electricity_used_on_works__other_generators",
            # "Total electricity supplied (gross, other generators) [note 2]": "total_electricity_supplied__gross__other_generators__note_2",
            # "Supplied from conventional thermal and other (gross, other generators) [note 3]": "supplied_from_conventional_thermal_and_other__gross__other_generators__note_3",
            # "Supplied from CCGT (gross, other generators)": "supplied_from_ccgt__gross__other_generators",
            # "Supplied from non-thermal renewables (gross, other generators) [note 5]": "supplied_from_non_thermal_renewables__gross__other_generators__note_5",
            # "Electricity generated (all generators)": "electricity_generated__all_generators",
            # "Electricity used on works (all generators)": "electricity_used_on_works__all_generators",
            # "Total electricity supplied (gross, all generators) [note 2]": "total_electricity_supplied__gross__all_generators__note_2",
            # "Supplied from conventional thermal and other (gross, all generators) [note 3]": "supplied_from_conventional_thermal_and_other__gross__all_generators__note_3",
            # "Supplied from CCGT (gross, all generators)": "supplied_from_ccgt__gross__all_generators",
            # "Supplied from nuclear (gross, all generators)": "supplied_from_nuclear__gross__all_generators",
            # "Supplied from non-thermal renewables (gross, all generators) [note 5]": "supplied_from_non_thermal_renewables__gross__all_generators__note_5",
            # "Supplied from hydro pumped storage (gross, all generators)": "supplied_from_hydro_pumped_storage__gross__all_generators",
            # "Electricity supplied (net, all generators) [note 4]": "electricity_supplied__net__all_generators__note_4",
            # "Fuel input for electricity generation (Mtoe)": "fuel_input_for_electricity_generation__mtoe",
            # "Fuel input for electricity generation (GWh)": "fuel_input_for_electricity_generation__gwh",
            "Implied efficiency": "implied_efficiency",
            # "Source notes": "source_notes",
        },
        table_name="efficiency",
    )

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=[tb_fuel_input, tb_supply, tb_efficiency],
        default_metadata=snap.metadata,
        check_variables_metadata=True,
    )

    # Save changes in the new garden dataset.
    ds_meadow.save()
