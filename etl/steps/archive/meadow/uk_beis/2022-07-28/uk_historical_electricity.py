from typing import Dict, List

import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from owid.walden import Catalog as WaldenCatalog

from etl.helpers import PathFinder
from etl.steps.data.converters import convert_walden_metadata

# Get relevant paths for current file.
paths = PathFinder(__file__)


def prepare_data(df: pd.DataFrame, expected_content: Dict[int, List[str]], columns: Dict[int, str]) -> pd.DataFrame:
    """Prepare raw content of a specific sheet in the the BEIS excel file (loaded with a simple pd.read_excel(...)).

    It contains some sanity checks due to the poor formatting of the original file, and some basic processing (like
    removing footnote marks from the years, e.g. "2000 (5)" -> 2000). Duplicate rows are not removed.

    Parameters
    ----------
    df : pd.DataFrame
    expected_content : dict
        Words that are expected to be found in any row of specific columns in the data. The dictionary key should be the
        column number (where the 0th column is expected to be the years variable), and the value should be a list of
        words (for example, a column that contains data for natural gas may have the words "Natural" and "gas" spread
        across two different rows). This is used to check that the columns are in the order we expected.
    columns : dict
        Columns to select from data, and how to rename them. The dictionary key should be the column number, and the
        value should be the new name for that column.

    Returns
    -------
    df : pd.DataFrame
        Clean data extracted, with proper column names.

    """
    df = df.copy()

    # Check that certain words are contained in specific columns, to ensure that they contain the data we expect.
    for column in expected_content:
        expected_elements = expected_content[column]
        for element in expected_elements:
            error = f"Excel file may have changed structure (expected {element} in column {column})."
            assert df[df.columns[column]].str.contains(element, regex=False).any(), error

    # Select columns and how to rename them.
    df = df.loc[:, df.columns[list(columns)]].rename(columns={df.columns[i]: columns[i] for i in columns})

    # Remove all rows for which the year column does not start with an integer of 4 digits.
    df = df.loc[df["year"].astype(str).str.contains(r"^\d{4}", regex=True, na=False)].reset_index(drop=True)
    # Remove annotations from years (e.g. replace "1987 (5)" by 1987).
    df["year"] = df["year"].astype(str).str[0:4].astype(int)

    # Make all columns float (except year column).
    df.astype({column: float for column in df.columns if column != "year"})

    return df


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Retrieve raw data from walden.
    walden_ds: WaldenCatalog = paths.load_dependency("uk_historical_electricity")
    local_file = walden_ds.ensure_downloaded()

    # Load data from the two relevant sheets of the excel file.
    # The original excel file is poorly formatted and will be hard to parse automatically.
    fuel_input = pd.read_excel(local_file, sheet_name="Fuel input")
    supply = pd.read_excel(local_file, sheet_name="Supply, availability & consump")
    efficiency = pd.read_excel(local_file, sheet_name="Generated and supplied")

    #
    # Process data.
    #
    # Process data from the sheet about fuels input for electricity generation.
    fuel_input = prepare_data(
        df=fuel_input,
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
    )

    # Prepare data from the sheet about electricity supply, availability and consumption.
    supply = prepare_data(
        df=supply,
        expected_content={
            1: ["Electricity", "supplied"],
            3: ["Net", "Imports"],
        },
        columns={
            0: "year",
            1: "electricity_generation",
            3: "net_imports",
        },
    )

    # Prepare data from the sheet about electricity generated and supplied.
    efficiency = prepare_data(
        df=efficiency,
        expected_content={
            33: ["Implied", "Efficiency"],
        },
        columns={
            0: "year",
            33: "implied_efficiency",
        },
    )

    #
    # Save outputs.
    #
    # Create new dataset and reuse walden metadata.
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(walden_ds)
    ds.metadata.version = "2022-07-28"

    # Create tables using metadata from walden.
    tb_fuel_input = Table(
        fuel_input,
        metadata=TableMeta(
            short_name="fuel_input",
            title="Fuel input for electricity generation",
            description=walden_ds.description,
        ),
    )
    tb_supply = Table(
        supply,
        metadata=TableMeta(
            short_name="supply",
            title="Electricity supply, availability and consumption",
            description=walden_ds.description,
        ),
    )
    tb_efficiency = Table(
        efficiency,
        metadata=TableMeta(
            short_name="efficiency",
            title="Electricity generated and supplied",
            description=walden_ds.description,
        ),
    )

    # Underscore all table columns.
    tb_fuel_input = underscore_table(tb_fuel_input)
    tb_supply = underscore_table(tb_supply)

    # Add tables to a dataset.
    ds.add(tb_fuel_input)
    ds.add(tb_supply)
    ds.add(tb_efficiency)

    # Save the dataset.
    ds.save()
