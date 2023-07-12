"""Load a snapshot and create a meadow dataset."""

from typing import Dict, List, cast

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def prepare_data(tb: Table, expected_content: Dict[int, List[str]], columns: Dict[int, str], table_name: str) -> Table:
    """Prepare raw content of a specific sheet in the the BEIS excel file (loaded with a simple pd.read_excel(...)).

    It contains some sanity checks due to the poor formatting of the original file, and some basic processing (like
    removing footnote marks from the years, e.g. "2000 (5)" -> 2000). Duplicate rows are not removed.

    Parameters
    ----------
    tb : Table
        Input data from a specific sheet.
    expected_content : dict
        Words that are expected to be found in any row of specific columns in the data. The dictionary key should be the
        column number (where the 0th column is expected to be the years variable), and the value should be a list of
        words (for example, a column that contains data for natural gas may have the words "Natural" and "gas" spread
        across two different rows). This is used to check that the columns are in the order we expected.
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

    # Check that certain words are contained in specific columns, to ensure that they contain the data we expect.
    for column in expected_content:
        expected_elements = expected_content[column]
        for element in expected_elements:
            error = f"Excel file may have changed structure (expected {element} in column {column})."
            assert tb[tb.columns[column]].str.contains(element, regex=False).any(), error

    # Select columns and how to rename them.
    tb = tb.loc[:, tb.columns[list(columns)]].rename(columns={tb.columns[i]: columns[i] for i in columns})

    # Remove all rows for which the year column does not start with an integer of 4 digits.
    tb = tb.loc[tb["year"].astype(str).str.contains(r"^\d{4}", regex=True, na=False)].reset_index(drop=True)
    # Remove annotations from years (e.g. replace "1987 (5)" by 1987).
    tb["year"] = tb["year"].astype(str).str[0:4].astype(int)

    # Make all columns float (except year column).
    tb.astype({column: float for column in tb.columns if column != "year"})

    # Update table short name.
    tb.metadata.short_name = table_name

    # Set an appropriate index and sort conveniently.
    # NOTE: We do not verify integrity because there are duplicated rows, that will be handled in the garden step.
    tb = tb.set_index(["year"], verify_integrity=False).sort_index().sort_index(axis=1)

    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = cast(Snapshot, paths.load_dependency("uk_historical_electricity.xls"))

    # Load data from the two relevant sheets of the excel file.
    # The original excel file is poorly formatted and will be hard to parse automatically.
    data = pr.ExcelFile(snap.path)
    tb_fuel_input = data.parse(sheet_name="Fuel input", metadata=snap.to_table_metadata())
    tb_supply = data.parse(sheet_name="Supply, availability & consump", metadata=snap.to_table_metadata())
    tb_efficiency = data.parse(sheet_name="Generated and supplied", metadata=snap.to_table_metadata())

    #
    # Process data.
    #
    # Process data from the sheet about fuels input for electricity generation.
    tb_fuel_input = prepare_data(
        tb=tb_fuel_input,
        expected_content={
            1: ["Total", "all", "fuels"],
            2: ["Coal"],
            3: ["Oil"],
            4: ["Natural", "gas"],
            5: ["Nuclear"],
            6: ["Natural", "flow hydro"],
            7: ["Wind", "and solar"],
            9: ["Other", "fuels"],
        },
        columns={
            0: "year",
            1: "all_sources",
            2: "coal",
            3: "oil",
            4: "gas",
            5: "nuclear",
            6: "hydro",
            7: "wind_and_solar",
            9: "other",
        },
        table_name="fuel_input",
    )

    # Prepare data from the sheet about electricity supply, availability and consumption.
    tb_supply = prepare_data(
        tb=tb_supply,
        expected_content={
            1: ["Electricity", "supplied"],
            3: ["Net", "Imports"],
        },
        columns={
            0: "year",
            1: "electricity_generation",
            3: "net_imports",
        },
        table_name="supply",
    )

    # Prepare data from the sheet about electricity generated and supplied.
    tb_efficiency = prepare_data(
        tb=tb_efficiency,
        expected_content={
            33: ["Implied", "Efficiency"],
        },
        columns={
            0: "year",
            33: "implied_efficiency",
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
