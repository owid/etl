"""Load a snapshot and create a meadow dataset."""

import os
import re
import tempfile
from glob import glob
from io import StringIO
from pathlib import Path

import numpy as np
import owid.catalog.processing as pr
from owid.catalog import Table
from owid.datautils.io import decompress_file
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Expected zip structure
folders_top = [
    "lt_male",
    "lt_female",
    "lt_both",
    "c_lt_male",
    "c_lt_female",
    "c_lt_both",
]
# Logger
log = get_logger()
# Regular expression to extract relevant fields from a txt file (Life tables)
FILE_REGEX_LT = (
    r"(?P<country>[a-zA-Z\-\s,]+), Life tables \((?P<type>[a-zA-Z]+) (?P<format>\d+x\d+)\), (?P<sex>[a-zA-Z]+)"
    r"\tLast modified: (?P<last_modified>\d+ [a-zA-Z]{3} \d+);  Methods Protocol: v\d+ \(\d+\)\n\n(?P<data>(?s:.)*)"
)
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


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("hmd.zip")

    # Load data from snapshot.
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Decompress files
        decompress_file(snap.path, tmp_dir)

        # Parse life tables table
        tb_lt = make_tb_lt(Path(tmp_dir))

    #
    # Process data.
    #
    # Set short_name
    tb_lt.metadata.short_name = "life_tables"

    # Rename columns
    # While this is not necessary at this step, there are some columns that, when underscored, are mapped to the same name;
    # e.g. "Lx -> lx" and "lx -> lx". This will cause an error when setting the index.
    tb_lt = tb_lt.rename(columns=COLUMNS_RENAME)

    # Process missing data
    tb_lt = proces_missing_data(tb_lt)

    # Dtypes
    tb_lt["Year"] = tb_lt["Year"].astype(str)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    columns_primary = ["format", "type", "country", "year", "sex", "age"]
    tb_lt = tb_lt.underscore().set_index(columns_primary, verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb_lt], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def make_tb_lt(path: Path) -> Table:
    """Create Life-tables table from multiple folders."""
    tbs = []
    # Iterate over each top-level folder
    for folder_top in folders_top:
        # Iterate over each subfolder, map content in subfolder into a table
        folders = os.listdir(path / folder_top)
        for folder in folders:
            tb = _make_tb_lt_from_subfolder(path=path / folder_top / folder)
            tbs.append(tb)
        # Concatenate all dataframes
    tb = pr.concat(tbs, ignore_index=True)
    return tb


def _make_tb_lt_from_subfolder(path: Path | str) -> Table:
    """Create a single table from all txt files in a folder."""
    # Initiate empty list of tables
    tbs = []
    # Get files in folder
    files = glob(os.path.join(path, "*.txt"))
    log.info(f"Creating dataframe from available files in {path}...")
    # Load and parse each txt file
    for f in files:
        # Parse txt -> table
        tb = _make_tb_lt_from_txt(f)
        tbs.append(tb)
    tb = pr.concat(tbs, ignore_index=True)
    return tb


def _make_tb_lt_from_txt(txt_path: Path | str) -> Table:
    """Load and parse a txt file into a Table."""
    # Read single file
    with open(txt_path, "r") as f:
        text = f.read()
    # Get relevant fields
    match = re.search(FILE_REGEX_LT, text)
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


def proces_missing_data(tb: Table) -> Table:
    """Check and process missing data.

    Missing data comes as '.', instead replace these with NaNs.
    """
    # Expected missing data
    frac_rows_missing_expected = 0.01
    countries_missing_data_expected = 14
    # Find missing data
    rows_missing = tb[tb.central_death_rate == "."]
    num_rows_missing = len(rows_missing) / len(tb)
    countries_missing_data = rows_missing.country.unique()
    # Run checks
    assert num_rows_missing < frac_rows_missing_expected, (
        f"More missing data than expected was found! {round(num_rows_missing*100, 2)} rows missing, but"
        f" {round(frac_rows_missing_expected*100,2)} were expected."
    )
    assert len(countries_missing_data) <= countries_missing_data_expected, (
        f"More missing data than expected was found! Found {len(countries_missing_data)} countries, expected is"
        f" {countries_missing_data_expected}. Check {countries_missing_data}!"
    )
    # Correct
    tb = tb.replace(".", np.nan)
    return tb
