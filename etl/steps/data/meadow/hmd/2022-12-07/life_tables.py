"""Imports Life Tables dataset to Meadow.

This dataset is from Human Mortality Database.

The source data provides a zip with several folders. From these, only three are of interest: female, male and both sexes.
These folders of interest contain 6 folders. Each folder contains a TXT file per country (not great).
This step generates a dataset with 6 tables, one for each folder. Each table contains the data from all TXT files.

Structure would be as follows:


    lt_female
        ├── fltper_1x1
        │       ├── Albania.txt
        │       ├── Algeria.txt
        ...
        ├── fltper_1x10
        ...
    lt_male
        ├── mltper_1x1
        │   ├── Albania.txt
        │   ├── Algeria.txt
        ...
    lt_both
        ├── bltper_1x1
        │   ├── Albania.txt
        │   ├── Algeria.txt
        ...

"""
import os
import re
import tempfile
from glob import glob
from io import StringIO
from pathlib import Path
from typing import List, Tuple, cast

import numpy as np
import pandas as pd
from owid import catalog
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from owid.datautils.io import decompress_file
from structlog import get_logger

from etl.helpers import PathFinder
from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata

log = get_logger()

# naming conventions
N = PathFinder(__file__)
NAMESPACE = "hmd"
SHORT_NAME = "hmd"
VERSION_SNAPSHOT = "2022-12-07"
DATASET_SNAPSHOT = f"hmd/{VERSION_SNAPSHOT}/{SHORT_NAME}.zip"
VERSION_MEADOW = "2022-12-07"
TNAME = "Human Mortality Database"

# Folder structure
FOLDER_STRUCTURE = [
    {
        "name": "lt_both",
        "files": ["bltper_1x1", "bltper_1x10", "bltper_1x5", "bltper_5x1", "bltper_5x10", "bltper_5x5"],
        "sex": "both",
        "regex": (
            r"([a-zA-Z\-\s,]+), Life tables \(period {table}\), Total\tLast modified: (\d+ [a-zA-Z]{{3}} \d+);  Methods"
            r" Protocol: v\d+ \(\d+\)\n\n((?s:.)*)"
        ),
    },
    {
        "name": "lt_female",
        "files": ["fltper_1x1", "fltper_1x10", "fltper_1x5", "fltper_5x1", "fltper_5x10", "fltper_5x5"],
        "sex": "female",
        "regex": (
            r"([a-zA-Z\-\s,]+), Life tables \(period {table}\), Females\tLast modified: (\d+ [a-zA-Z]{{3}} \d+); "
            r" Methods"
            r" Protocol: v\d+ \(\d+\)\n\n((?s:.)*)"
        ),
    },
    {
        "name": "lt_male",
        "files": ["mltper_1x1", "mltper_1x10", "mltper_1x5", "mltper_5x1", "mltper_5x10", "mltper_5x5"],
        "sex": "male",
        "regex": (
            r"([a-zA-Z\-\s,]+), Life tables \(period {table}\), Males\tLast modified: (\d+ [a-zA-Z]{{3}} \d+);  Methods"
            r" Protocol: v\d+ \(\d+\)\n\n((?s:.)*)"
        ),
    },
]
# Folders expected
FOLDERS_EXPECTED = [folder["name"] for folder in FOLDER_STRUCTURE]

# Column renaming
COLUMNS_RENAME = {
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
# Column dtypes
DTYPES = {
    "central_death_rate": "float64",
    "probability_of_death": "float64",
    "avg_survival_length": "float64",
    "num_survivors": "float64",
    "num_deaths": "float64",
    "num_person_years_lived": "float64",
    "num_person_years_remaining": "float64",
    "life_expectancy": "float64",
    "age": "category",
    "country": "category",
    "year": "category",
}
# missing data: this are the countries where data is missing for a specific table.
# these checks are done in function _clean_correct_missing_data.
MISSING_DATA_COUNTRIES = {
    "bltper_1x1": ["Belgium"],
    "bltper_1x5": ["Belgium"],
    "bltper_1x10": [],
    "bltper_5x1": ["Belgium"],
    "bltper_5x5": ["Belgium"],
    "bltper_5x10": [],
    "fltper_1x1": ["Belgium"],
    "fltper_1x5": ["Belgium"],
    "fltper_1x10": [],
    "fltper_5x1": ["Belgium"],
    "fltper_5x5": ["Belgium"],
    "fltper_5x10": [],
    "mltper_1x1": ["Belgium"],
    "mltper_1x5": ["Belgium"],
    "mltper_1x10": [],
    "mltper_5x1": ["Belgium"],
    "mltper_5x5": ["Belgium"],
    "mltper_5x10": [],
}


def run(dest_dir: str) -> None:
    """Run step."""
    log.info("hmd_lt.start")

    # retrieve data snapshot
    log.info("hmd_lt: loading snapshot")
    snap = Snapshot(DATASET_SNAPSHOT)

    # Create new dataset and reuse snapshot metadata
    log.info("hmd_lt: initiating meadow dataset")
    ds = init_dataset(dest_dir, snap)

    # Create and add tables to dataset
    log.info("hmd_lt: creating and adding new table...")
    ds = create_and_add_tables_to_dataset(snap.path, ds)

    ds.metadata.short_name = "life_tables"

    # Save the dataset
    ds.save()
    log.info("hmd_lt.end")


def init_dataset(dest_dir: str, snap: Snapshot) -> Dataset:
    """Initialize meadow dataset."""
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_snapshot_metadata(snap.metadata)
    ds.metadata.version = VERSION_MEADOW
    return ds


def create_and_add_tables_to_dataset(local_file: Path, ds: Dataset) -> Dataset:
    """Create and add tables to dataset.

    This method creates tables for all the folders found once `local_file` is uncompressed. Then,
    it cleans and adds them to the dataset `ds`. It uses the metadata from `walden_ds` to create the tables.

    Parameters
    ----------
    local_file : str
        File to walden raw file.
    ds : Dataset
        Dataset where tables should be added.

    Returns
    -------
    Dataset
        Dataset with tables
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Decompress files
        log.info("hmd_lt: decompressing files...")
        decompress_file(local_file, tmp_dir)

        # Check available files
        log.info("hmd_lt: sanity checking folders...")
        _sanity_check_relevant_folders(tmp_dir)

        for folder in FOLDER_STRUCTURE:
            log.info(f"hmd_lt: processing folder {folder['name']}...")
            # Check that all required subolders are present in folder
            path_folder = os.path.join(tmp_dir, folder["name"])
            _sanity_check_files(path_folder, folder["files"])
            # Load data
            for f in folder["files"]:
                log.info(f"hmd_lt: loading data from file {folder['name']}/{f}...")
                path_subfolder = os.path.join(path_folder, f)
                # Create table
                table = make_table(path_subfolder, folder)
                # add table to a dataset
                log.info("hmd_lt: adding table to dataset...")
                ds.add(table)
    return ds


def _sanity_check_relevant_folders(path: str) -> None:
    folders = os.listdir(path)
    assert all(
        folder in folders for folder in FOLDERS_EXPECTED
    ), f"Some of the life table ({FOLDERS_EXPECTED}) folders are not present!"


def _sanity_check_files(path: str, files_expected: List[str]) -> None:
    """Checks that all required files are present once zip is uncompressed."""
    files_found = sorted(os.listdir(path))
    assert (
        files_found == files_expected
    ), f"Files found are not the ones expected! Check that {files_expected} are actually there!"


def make_table(input_folder: str, folder: dict) -> catalog.Table:
    """Create table.

    Loads data from `input_folder` and creates a table with the name `table_name`. It uses the metadata from `walden_ds`.

    Parameters
    ----------
    input_folder : str
        Folder containing uncompressed data from Walden.
    folder : dict
        Structure details from folder.

    Returns
    -------
    catalog.Table
        Table with data.
    """
    # List all files (each file corresponds to a country roughly)
    files = glob(os.path.join(input_folder, "*.txt"))
    log.info(f"hmd_lt: looking for available files in {input_folder}: {len(files)} found.")
    # create df
    age, year = _age_year(input_folder)
    regex_header = folder["regex"].format(table=f"{age}x{year}")
    df = make_df(files, regex_header)
    # Clean df
    df = clean_df(df, input_folder)
    # Set index
    df = df.set_index(["country", "year", "age"], verify_integrity=True).sort_index()
    # df to table
    table = df_to_table(df, age, year, folder["sex"])
    # underscore all table columns
    table = underscore_table(table)
    return table


def _age_year(input_folder: str) -> Tuple[str, str]:
    ageyear = input_folder.split("_")[-1]
    age, year = ageyear.split("x")
    assert age.isdecimal() and year.isdecimal(), f"Age and year should be integers! Check {age} and {year} is not!"
    return age, year


def make_df(files: List[str], regex_header: str) -> pd.DataFrame:
    """Create dataframe.

    Parameters
    ----------
    files : List[str]
        Files to load and extract data from. There is a file per country. Within the file, data is found
        along with country name. Note that sometimes, the country name can be like "New Zealand - Non-Maori".
    regex_header: str
        Regex of the file header text line.

    Returns
    -------
    pd.DataFrame
        _description_
    """
    log.info("hmd_lt: creating dataframe from files...")
    dfs = []
    # Load each file
    for f in files:
        with open(f, "r") as f:
            text = f.read()
        # Get relevant fields
        match = re.search(regex_header, text)
        if match is not None:
            country, _, table_str = match.group(1, 2, 3)
        else:
            raise ValueError(f"No match found! Please revise that source files' content matches {regex_header}.")
        # Build country df
        df_ = _make_df_country(country, table_str)
        dfs.append(df_)
    # Concatenate all country dfs
    df = pd.concat(dfs, ignore_index=True)
    return cast(pd.DataFrame, df)


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
    return df


def clean_df(df: pd.DataFrame, folder_path: str) -> pd.DataFrame:
    """Clean dataframe.

    Orders columns, renames columns, checks for missing values and sets dtypes.

    Parameters
    ----------
    df : pd.DataFrame
        Initial dataframe.
    folder_path: str
        Path to the folder from which the dataframe was created.

    Returns
    -------
    pd.DataFrame
        Cleaned dataframe.
    """
    log.info("hmd_lt: cleaning dataframe...")
    # Order columns
    cols_first = ["Country", "Year"]
    cols = cols_first + [col for col in df.columns if col not in cols_first]
    df = df[cols]
    # Rename columns
    df = _clean_rename_columns_df(df)
    # Correct missing data
    df = _clean_correct_missing_data(df, folder_path)
    # Set dtypes
    df = _clean_set_dtypes_df(df)
    return df


def _clean_rename_columns_df(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns."""
    return df.rename(columns=COLUMNS_RENAME, errors="raise")


def _clean_correct_missing_data(df: pd.DataFrame, folder_path: str) -> pd.DataFrame:
    """Checks on missing data."""
    # Number of rows expected to be missing at most (percentage )
    perc_rows_missing_expected = 0.01
    # Find missing data
    rows_missing = df[df.central_death_rate == "."]
    perc_rows_missing = len(rows_missing) / len(df)
    countries_missing_data = sorted(set(rows_missing.country))
    # Run checks
    assert perc_rows_missing < perc_rows_missing_expected, (
        f"More missing data than expected was found! {round(perc_rows_missing*100, 2)} rows missing, but"
        f" {round(perc_rows_missing_expected*100,2)} were expected."
    )
    fname = folder_path.split("/")[-1]
    assert countries_missing_data == MISSING_DATA_COUNTRIES[fname], (
        f"More missing data than expected was found! Found {len(countries_missing_data)} countries, expected is"
        f" {MISSING_DATA_COUNTRIES[fname]}. Check {countries_missing_data}!"
    )
    # Correct
    df = df.replace(".", np.nan)
    return df


def _clean_set_dtypes_df(df: pd.DataFrame) -> pd.DataFrame:
    """Set dtypes."""
    # Numeric
    cols_numeric = [col_name for col_name, dtype in DTYPES.items() if dtype in ["int64", "float64"]]
    for col in cols_numeric:
        df[col] = pd.to_numeric(df[col])
    return df.astype(DTYPES)


def df_to_table(df: pd.DataFrame, age: str, year: str, sex: str) -> catalog.Table:
    """Convert plain pandas.DataFrame into table.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe.
    age: int
        Age group size (1 or 5).
    year: int
        Year interval size (1, 5 or 10).
    sex: str
        Sex (both, female or male).

    Returns
    -------
    catalog.Table
        Table created from dataframe, walden metadata and table name.
    """
    description = f"Contains data in {age}-year age groups grouped in {year}-year intervals."
    if sex != "both":
        description += f"Consideres only {sex} population."
    # create table with metadata from dataframe
    table_metadata = TableMeta(
        short_name=f"{sex}_{age}x{year}",
        title=f"{TNAME} [age={age}, year={year}, sex={sex}]",
        description=description,
    )
    tb = Table(df, metadata=table_metadata)
    return tb
