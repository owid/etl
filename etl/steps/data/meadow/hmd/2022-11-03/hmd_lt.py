import os
import re
import tempfile
from glob import glob
from io import StringIO
from typing import List

import pandas as pd
from owid import catalog
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from owid.datautils.io import decompress_file
from owid.walden import Catalog as WaldenCatalog
from structlog import get_logger

from etl.helpers import Names
from etl.steps.data.converters import convert_walden_metadata

log = get_logger()

# naming conventions
N = Names(__file__)


# Files expected once Walden file is uncompressed
FILES_EXPECTED = ["bltper_1x1", "bltper_1x10", "bltper_1x5", "bltper_5x1", "bltper_5x10", "bltper_5x5"]
# Regular expression to extract relevant fields from file (used in make_df)
FILE_REGEX = (
    r"([a-zA-Z\-\s,]+), Life tables \(period {table}\), Total\tLast modified: (\d+ [a-zA-Z]{{3}} \d+);  Methods"
    r" Protocol: v\d+ \(\d+\)\n\n((?s:.)*)"
)
# Dataset details from Walden
NAMESPACE = "hmd"
SHORT_NAME = "hmd_lt"
VERSION_WALDEN = "2022-11-01"
# Meadow version
VERSION_MEADOW = "2022-11-01"


def run(dest_dir: str) -> None:
    """Run step."""
    log.info("hmd_lt.start")

    # Retrieve raw data from walden
    walden_ds = WaldenCatalog().find_one(namespace=NAMESPACE, short_name=SHORT_NAME, version=VERSION_WALDEN)
    local_file = walden_ds.ensure_downloaded()

    # Create new dataset and reuse walden metadata
    ds = init_meadow_dataset(dest_dir, walden_ds)

    # Create and add tables to dataset
    ds = create_and_add_tables_to_dataset(local_file, ds, walden_ds)

    # Save the dataset
    ds.save()
    log.info("hmd_lt.end")


def init_meadow_dataset(dest_dir: str, walden_ds: WaldenCatalog) -> Dataset:
    """Initialize meadow dataset."""
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(walden_ds)
    ds.metadata.version = VERSION_MEADOW
    return ds


def create_and_add_tables_to_dataset(local_file: str, ds: Dataset, walden_ds: WaldenCatalog) -> Dataset:
    """Create and add tables to dataset.

    This method creates tables for all the folders found once `local_file` is uncompressed. Then,
    it cleans and ads them to the dataset `ds`. It uses the metadata from `walden_ds` to create the tables.

    Parameters
    ----------
    local_file : str
        File to walden raw file.
    ds : Dataset
        Dataset where tables should be added.
    walden_ds : WaldenCatalog
        Walden dataset.

    Returns
    -------
    Dataset
        Dataset with tables
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Decompress files
        decompress_file(local_file, tmp_dir)

        # Check available files
        _sanity_check_files(tmp_dir)

        # Load data
        for i in [1, 5]:
            for j in [1, 5, 10]:
                # Create table and folder name
                table_name = f"{i}x{j}"
                # Create table
                log.info(f"Creating table '{table_name}'...")
                table = make_table(tmp_dir, table_name, walden_ds)
                # add table to a dataset
                log.info("Adding table to dataset...")
                ds.add(table)
    return ds


def _sanity_check_files(path: str) -> None:
    """Checks that all required files are present once zip is uncompressed."""
    files_found = sorted(os.listdir(path))
    assert (
        files_found == FILES_EXPECTED
    ), f"Files found are not the ones expected! Check that {FILES_EXPECTED} are actually there!"


def make_table(input_folder: str, table_name: str, walden_ds: WaldenCatalog) -> catalog.Table:
    """Create table.

    Loads data from `input_folder` and creates a table with the name `table_name`. It uses the metadata from `walden_ds`.

    Parameters
    ----------
    walden_ds : WaldenCatalog
        Walden dataset.
    input_folder : str
        Folder containing uncompressed data from Walden.
    table_name : str
        Name of the table.

    Returns
    -------
    catalog.Table
        Table with data.
    """
    # Load files
    f = f"bltper_{table_name}"
    files = glob(os.path.join(input_folder, f"{f}/*.txt"))
    log.info(f"Looking for available files in {f}...")
    # Create df
    df = make_df(files, table_name)
    # Clean df
    df = clean_df(df)
    # df to table
    table = df_to_table(walden_ds, table_name, df)
    # underscore all table columns
    table = underscore_table(table)
    return table


def make_df(files: List[str], table_name: str) -> pd.DataFrame:
    """Create dataframe.

    Parameters
    ----------
    files : List[str]
        Files to load and extract data from. There is a file per country. Within the file, data is found
        along with country name. Note that sometimes, the country name can be like "New Zealand - Non-Maori".
    table_name : str
        Name of the table.

    Returns
    -------
    pd.DataFrame
        _description_
    """
    log.info("Creating dataframe from files...")
    regex = FILE_REGEX.format(table=table_name)
    dfs = []
    # Load each file
    for f in files:
        with open(f, "r") as f:
            text = f.read()
        # Get relevant fields
        match = re.search(regex, text)
        if match is not None:
            country, _, table_str = match.group(1, 2, 3)
        else:
            raise ValueError("No match found!")
        # Build country df
        df_ = _make_df_country(country, table_str)
        dfs.append(df_)
    # Concatenate all country dfs
    df = pd.concat(dfs, ignore_index=True)
    return df  # type: ignore


def _make_df_country(country: str, table: str) -> pd.DataFrame:
    """Create dataframe for individual country."""
    # Remove starting/ending spaces
    table = table.strip()
    # Remove spacing after newline
    table = re.sub(r"\n\s+", "\n", table)
    # Replace spacing with tabs
    table = re.sub(r"[^\S\r\n]+", "\t", table)
    # Build df
    df = pd.read_csv(StringIO(table), sep="\t")
    # Assign country
    df = df.assign(Country=country)
    # # Filter columns
    # df = df[["Country", "Year", "ex"]].rename(columns={"ex": "life_expectancy"})
    return df


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """Clean dataframe.

    Orders columns, renames columns, checks for missing values and sets dtypes.

    Parameters
    ----------
    df : pd.DataFrame
        Initial dataframe.

    Returns
    -------
    pd.DataFrame
        Cleaned dataframe.
    """
    log.info("Cleaning dataframe...")
    # Order columns
    cols_first = ["Country", "Year"]
    cols = cols_first + [col for col in df.columns if col not in cols_first]
    df = df[cols]
    # Rename columns
    df = _clean_rename_columns_df(df)
    # Correct missing data
    df = _clean_correct_missing_data(df)
    # Set dtypes
    df = _clean_set_dtypes_df(df)
    return df


def _clean_rename_columns_df(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns."""
    return df.rename(
        columns={
            "Country": "country",
            "Year": "year",
            "Age": "age",
            "mx": "central_death_rate",
            "qx": "probability_of_death",
            "ax": "avg_survival_length",
            "lx": "num_survivors",
            "dx": "num_deaths",
            "Lx": "num_person_years_lived",
            "Tx": "num_person_years_remaining",
            "ex": "life_expectancy",
        }
    )


def _clean_correct_missing_data(df: pd.DataFrame) -> pd.DataFrame:
    """Checks on missing data."""
    # Expected missing data
    num_rows_missing_expected = 0.01
    countries_missing_data_expected = 1
    # Find missing data
    rows_missing = df[df.central_death_rate == "."]
    num_rows_missing = len(rows_missing) / len(df)
    countries_missing_data = rows_missing.country.unique()
    # Run checks
    assert num_rows_missing < num_rows_missing_expected, (
        f"More missing data than expected was found! {round(num_rows_missing*100, 2)} rows missing, but"
        f" {round(num_rows_missing_expected*100,2)} were expected."
    )
    assert len(countries_missing_data) <= countries_missing_data_expected, (
        f"More missing data than expected was found! Found {len(countries_missing_data)} countries, expected is"
        f" {countries_missing_data_expected}. Check {countries_missing_data}!"
    )
    # Correct
    df = df.replace(".", pd.NA)
    return df


def _clean_set_dtypes_df(df: pd.DataFrame) -> pd.DataFrame:
    """Set dtypes."""
    # dtypes
    dtypes = {
        "central_death_rate": "Float64",
        "probability_of_death": "Float64",
        "avg_survival_length": "Float64",
        "num_survivors": "Int64",
        "num_deaths": "Int64",
        "num_person_years_lived": "Int64",
        "num_person_years_remaining": "Int64",
        "life_expectancy": "Float64",
        "age": "category",
        "country": "category",
        "year": "category",
    }
    # Numeric
    cols_numeric = [col_name for col_name, dtype in dtypes.items() if dtype in ["Int64", "Float64"]]
    for col in cols_numeric:
        df[col] = pd.to_numeric(df[col])
    return df.astype(dtypes)


def df_to_table(walden_ds: WaldenCatalog, table_name: str, df: pd.DataFrame) -> catalog.Table:
    """Convert plain pandas.DataFrame into table.

    Parameters
    ----------
    walden_ds : WaldenCatalog
        Raw Walden dataset.
    table_name : str
        Table name.
    df : pd.DataFrame
        Dataframe.

    Returns
    -------
    catalog.Table
        Table created from dataframe, walden metadata and table name.
    """
    # create table with metadata from dataframe
    table_metadata = TableMeta(
        short_name=f"agexyear_{table_name}",
        title=f"{walden_ds.name} [{table_name}]",
        description=walden_ds.description,
    )
    tb = Table(df, metadata=table_metadata)
    return tb
