"""Load a snapshot and create a meadow dataset."""

import os
import re
import tempfile
from glob import glob
from io import StringIO
from pathlib import Path
from typing import Callable, List

import numpy as np
import owid.catalog.processing as pr
from owid.catalog import Table
from owid.datautils.io import decompress_file
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Logger
log = get_logger()
# Column rename
COLUMNS_RENAME = {
    "mx": "central_death_rate",
    "qx": "probability_of_death",
    "ax": "average_survival_length",
    "lx": "number_survivors",
    "dx": "number_deaths",
    "Lx": "number_person_years_lived",
    "Tx": "number_person_years_remaining",
    "ex": "life_expectancy",
}
# There are some missing values, filled with '.'. Used in `proces_missing_data_lt`
FRAC_ROWS_MISSING_EXPECTED_LT = 0.01
COUNTRIES_MISSING_DATA_EXPECTED_LT = 14
# There are some missing values, filled with '.'. Used in `proces_missing_data_exp`
FRAC_ROWS_MISSING_EXPECTED_EXP = 0.23
COUNTRIES_MISSING_DATA_EXPECTED_EXP = 47
# There are some missing values, filled with '.'. Used in `proces_missing_data_de`
FRAC_ROWS_MISSING_EXPECTED_DE = 0.001
COUNTRIES_MISSING_DATA_EXPECTED_DE = 1


def run(dest_dir: str) -> None:
    """Run script."""
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("hmd.zip")

    # Load data from snapshot.
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Decompress files
        decompress_file(snap.path, tmp_dir)

        # Parse Life tables
        folders_names = [
            "lt_male",
            "lt_female",
            "lt_both",
            "c_lt_male",
            "c_lt_female",
            "c_lt_both",
        ]
        tb_lt = _make_tb(
            path=Path(tmp_dir), folders_names=folders_names, make_tb_from_txt_callable=_make_tb_from_txt_lt
        )

        # Parse Exposures
        folders_names = [
            "c_exposures",
            "exposures",
        ]
        tb_exp = _make_tb(
            path=Path(tmp_dir), folders_names=folders_names, make_tb_from_txt_callable=_make_tb_from_txt_exp
        )

        # Parse Deaths
        folders_names = [
            "deaths",
        ]
        tb_de = _make_tb(
            path=Path(tmp_dir), folders_names=folders_names, make_tb_from_txt_callable=_make_tb_from_txt_de
        )

    #
    # Process data.
    #
    # LIFE TABLES
    ## While this is not necessary at this step, there are some columns that, when underscored, are mapped to the same name;
    ## e.g. "Lx -> lx" and "lx -> lx". This will cause an error when setting the index.
    tb_lt = tb_lt.rename(columns=COLUMNS_RENAME)
    ## Process missing data: discard, replace symbols for NaNs, etc.
    tb_lt = proces_missing_data_lt(tb_lt)
    ## Dtypes
    tb_lt["Year"] = tb_lt["Year"].astype(str)

    ## EXPOSURES
    ## Process missing data
    tb_exp = proces_missing_data_exp(tb_exp)
    ## Dtypes
    tb_exp["Year"] = tb_exp["Year"].astype(str)

    ## DEATHS
    ## Process missing data
    tb_de = proces_missing_data_de(tb_de)
    # Dtypes
    tb_de["Year"] = tb_de["Year"].astype(str)

    # Add metadata (table + indicators)
    ## life exp
    tb_lt.metadata = snap.to_table_metadata()
    tb_lt.metadata.short_name = "life_tables"
    for column in tb_lt.columns:
        tb_lt[column].metadata.origins = [snap.metadata.origin]
    ## exposure to risk
    tb_exp.metadata = snap.to_table_metadata()
    tb_exp.metadata.short_name = "exposures"
    for column in tb_exp.all_columns:
        tb_exp[column].metadata.origins = [snap.metadata.origin]
    ## deaths
    tb_de.metadata = snap.to_table_metadata()
    tb_de.metadata.short_name = "deaths"
    for column in tb_de.columns:
        tb_de[column].metadata.origins = [snap.metadata.origin]

    # Set index
    ## Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    columns_primary = ["format", "type", "country", "year", "sex", "age"]
    ## Actual index-setting
    tb_lt = tb_lt.underscore().set_index(columns_primary, verify_integrity=True).sort_index()
    tb_exp = tb_exp.underscore().set_index(columns_primary, verify_integrity=True).sort_index()
    tb_de = tb_de.underscore().set_index(columns_primary, verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=[tb_lt, tb_exp, tb_de],
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def _make_tb(path: Path, folders_names: List[str], make_tb_from_txt_callable: Callable) -> Table:
    """Create tables from multiple folders.

    It uses `make_tb_from_txt_callable` to load and parse each txt file into a table.

    While `make_tb` obtains a general table, `_make_tb` obtains a table for a specific category (e.g. life tables).
    """
    # List with all tables
    tbs = []
    # Iterate over each top-level folder
    for folder_name in folders_names:
        # Iterate over each subfolder, map content in subfolder into a table
        folders = os.listdir(path / folder_name)
        for folder in folders:
            path_subfolder = path / folder_name / folder
            if "lexis" in str(path_subfolder):
                continue
            if os.path.isdir(path):
                tb = _make_tb_from_subfolder(path=path_subfolder, make_tb_from_txt_callable=make_tb_from_txt_callable)
                tbs.append(tb)
        # Concatenate all dataframes
    tb = pr.concat(tbs, ignore_index=True)
    return tb


def _make_tb_from_subfolder(path: Path | str, make_tb_from_txt_callable: Callable) -> Table:
    """Create a single table from all txt files in a folder."""
    # Initiate empty list of tables
    tbs = []
    # Get files in folder
    files = glob(os.path.join(path, "*.txt"))
    log.info(f"Creating dataframe from available files in {path}...")
    # Load and parse each txt file
    for f in files:
        # Parse txt -> table
        tb = make_tb_from_txt_callable(f)
        tbs.append(tb)
    tb = pr.concat(tbs, ignore_index=True)
    return tb


def _make_tb_from_txt_lt(txt_path: Path | str) -> Table:
    """Load and parse a txt file into a Table."""
    # Regex to parse TXT file
    FILE_REGEX = (
        r"(?P<country>[a-zA-Z\-\s,]+), Life tables \((?P<type>[a-zA-Z]+) (?P<format>\d+x\d+)\), (?P<sex>[a-zA-Z]+)"
        r"\tLast modified: (?P<last_modified>\d+ [a-zA-Z]{3} \d+);  Methods Protocol: v\d+ \(\d+\)\n\n(?P<data>(?s:.)*)"
    )
    # Read single file
    with open(txt_path, "r") as f:
        text = f.read()
    # Get relevant fields
    match = re.search(FILE_REGEX, text)
    if match is not None:
        groups = match.groupdict()
    else:
        raise ValueError(f"No match found in {f}! Please revise that source files' content matches FILE_REGEX.")
    # Build df
    tb_str = groups["data"].strip()
    tb_str = re.sub(r"\n\s+", "\n", tb_str)
    tb_str = re.sub(r"[^\S\r\n]+", "\t", string=tb_str)
    tb = pr.read_csv(StringIO(tb_str), sep="\t")
    # Add dimensions
    tb = tb.assign(
        country=groups["country"],
        sex=groups["sex"],
        type=groups["type"],
        format=groups["format"],
    )
    return tb


def _make_tb_from_txt_exp(txt_path: Path | str) -> Table:
    """Load and parse a txt file into a Table."""
    # Regex to parse TXT file
    FILE_REGEX = (
        r"(?P<country>[a-zA-Z\-\s,]+), Exposure to risk \((?P<type>[a-zA-Z]+) (?P<format>\d+x\d+)\),\s\tLast modified: "
        r"(?P<last_modified>\d+ [a-zA-Z]{3} \d+);  Methods Protocol: v\d+ \(\d+\)\n\n(?P<data>(?s:.)*)"
    )
    # Read single file
    with open(txt_path, "r") as f:
        text = f.read()
    # Get relevant fields
    match = re.search(FILE_REGEX, text)
    if match is not None:
        groups = match.groupdict()
    else:
        raise ValueError(f"No match found in {f}! Please revise that source files' content matches FILE_REGEX.")
    # Build df
    tb_str = groups["data"].strip()
    tb_str = re.sub(r"\n\s+", "\n", tb_str)
    tb_str = re.sub(r"[^\S\r\n]+", "\t", string=tb_str)
    tb = pr.read_csv(StringIO(tb_str), sep="\t")
    # Melt
    tb = tb.melt(id_vars=["Age", "Year"], var_name="sex", value_name="exposure")
    # Add dimensions
    tb = tb.assign(
        country=groups["country"],
        type=groups["type"],
        format=groups["format"],
    )
    return tb


def _make_tb_from_txt_de(txt_path: Path | str) -> Table:
    """Load and parse a txt file into a Table."""
    # Regex to parse TXT file
    FILE_REGEX = (
        r"(?P<country>[a-zA-Z\-\s,]+), Deaths \((?P<type>[a-zA-Z]+) (?P<format>\d+x\d+|Lexis triangle)\),\s\tLast modified: "
        r"(?P<last_modified>\d+ [a-zA-Z]{3} \d+);  Methods Protocol: v\d+ \(\d+\)\n\n(?P<data>(?s:.)*)"
    )
    # Read single file
    with open(txt_path, "r") as f:
        text = f.read()
    # Get relevant fields
    match = re.search(FILE_REGEX, text)
    if match is not None:
        groups = match.groupdict()
    else:
        raise ValueError(f"No match found in {f}! Please revise that source files' content matches FILE_REGEX.")
    # Build df
    tb_str = groups["data"].strip()
    tb_str = re.sub(r"\n\s+", "\n", tb_str)
    tb_str = re.sub(r"[^\S\r\n]+", "\t", string=tb_str)
    tb = pr.read_csv(StringIO(tb_str), sep="\t")
    # Melt
    tb = tb.melt(id_vars=["Age", "Year"], var_name="sex", value_name="deaths")
    # Add dimensions
    tb = tb.assign(
        country=groups["country"],
        type=groups["type"],
        format=groups["format"],
    )
    return tb


def proces_missing_data_lt(tb: Table) -> Table:
    """Check and process missing data.

    Missing data comes as '.', instead replace these with NaNs.
    """
    # Find missing data
    rows_missing = tb[tb["central_death_rate"] == "."]
    num_rows_missing = len(rows_missing) / len(tb)
    countries_missing_data = rows_missing["country"].unique()
    # Run checks
    assert num_rows_missing < FRAC_ROWS_MISSING_EXPECTED_LT, (
        f"More missing data than expected was found! {round(num_rows_missing*100, 2)} rows missing, but"
        f" {round(FRAC_ROWS_MISSING_EXPECTED_LT*100, 2)}% were expected."
    )
    assert len(countries_missing_data) <= COUNTRIES_MISSING_DATA_EXPECTED_LT, (
        f"More missing data than expected was found! Found {len(countries_missing_data)} countries, expected is"
        f" {COUNTRIES_MISSING_DATA_EXPECTED_LT}. Check {countries_missing_data}!"
    )
    # Correct
    tb = tb.replace(".", np.nan)
    return tb


def proces_missing_data_de(tb: Table) -> Table:
    """Check and process missing data.

    Missing data comes as '.', instead replace these with NaNs.
    """
    # Find missing data
    rows_missing = tb[tb["deaths"] == "."]
    num_rows_missing = len(rows_missing) / len(tb)
    countries_missing_data = rows_missing["country"].unique()
    # Run checks
    assert num_rows_missing < FRAC_ROWS_MISSING_EXPECTED_DE, (
        f"More missing data than expected was found! {round(num_rows_missing*100, 2)} rows missing, but"
        f" {round(FRAC_ROWS_MISSING_EXPECTED_DE*100,2)}% were expected."
    )
    assert len(countries_missing_data) <= COUNTRIES_MISSING_DATA_EXPECTED_DE, (
        f"More missing data than expected was found! Found {len(countries_missing_data)} countries, expected is"
        f" {COUNTRIES_MISSING_DATA_EXPECTED_DE}. Check {countries_missing_data}!"
    )
    # Correct
    tb = tb.replace(".", np.nan)
    return tb


def proces_missing_data_exp(tb: Table) -> Table:
    """Check and process missing data.

    Missing data comes as '.', instead replace these with NaNs.
    """
    # Find missing data
    rows_missing = tb[tb["exposure"] == "."]
    num_rows_missing = len(rows_missing) / len(tb)
    countries_missing_data = rows_missing["country"].unique()
    # Run checks
    assert num_rows_missing < FRAC_ROWS_MISSING_EXPECTED_EXP, (
        f"More missing data than expected was found! {round(num_rows_missing*100, 2)} rows missing, but"
        f" {round(FRAC_ROWS_MISSING_EXPECTED_EXP*100,2)}% were expected."
    )
    assert len(countries_missing_data) <= COUNTRIES_MISSING_DATA_EXPECTED_EXP, (
        f"More missing data than expected was found! Found {len(countries_missing_data)} countries, expected is"
        f" {COUNTRIES_MISSING_DATA_EXPECTED_EXP}. Check {countries_missing_data}!"
    )
    # Correct
    tb = tb.replace(".", np.nan)
    return tb
