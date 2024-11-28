"""Load a snapshot and create a meadow dataset."""

import os
import re
from io import StringIO
from pathlib import Path
from typing import Callable, List

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Life tables
TABLES_LIFE_TABLES = [
    "lt_male",
    "lt_female",
    "lt_both",
    "c_lt_male",
    "c_lt_female",
    "c_lt_both",
]
REGEX_LT = (
    r"(?P<country>[a-zA-Z\-\s,]+), Life tables \((?P<type>[a-zA-Z]+) (?P<format>\d+x\d+)\), (?P<sex>[a-zA-Z]+)"
    r"\tLast modified: (?P<last_modified>\d+ [a-zA-Z]{3} \d+);  Methods Protocol: v\d+ \(\d+\)\n\n(?P<data>(?s:.)*)"
)
COLUMNS_RENAME_LT = {
    "mx": "central_death_rate",
    "qx": "probability_of_death",
    "ax": "average_survival_length",
    "lx": "number_survivors",
    "dx": "number_deaths",
    "Lx": "number_person_years_lived",
    "Tx": "number_person_years_remaining",
    "ex": "life_expectancy",
}

# Exposures
TABLES_EXPOSURES = [
    "c_exposures",
    "exposures",
]
REGEX_EXP = (
    r"(?P<country>[a-zA-Z\-\s,]+), Exposure to risk \((?P<type>[a-zA-Z]+) (?P<format>\d+x\d+)\),\s\tLast modified: "
    r"(?P<last_modified>\d+ [a-zA-Z]{3} \d+);  Methods Protocol: v\d+ \(\d+\)\n\n(?P<data>(?s:.)*)"
)

# Mortality
TABLES_M = [
    "deaths",
]
REGEX_M = (
    r"(?P<country>[a-zA-Z\-\s,]+), Deaths \((?P<type>[a-zA-Z]+) (?P<format>\d+x\d+|Lexis triangle)\),\s\tLast modified: "
    r"(?P<last_modified>\d+ [a-zA-Z]{3} \d+);  Methods Protocol: v\d+ \(\d+\)\n\n(?P<data>(?s:.)*)"
)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("hmd.zip")

    # Load data from snapshot.
    with snap.extract_to_tempdir() as tmpdir:
        # Life tables
        tb_lt = make_tb(
            path=Path(tmpdir),
            main_folders=TABLES_LIFE_TABLES,
            regex=REGEX_LT,
        )
        # Exposure
        tb_exp = make_tb(
            path=Path(tmpdir),
            main_folders=TABLES_EXPOSURES,
            regex=REGEX_EXP,
        )
        # Mortality
        tb_m = make_tb(
            path=Path(tmpdir),
            main_folders=TABLES_M,
            regex=REGEX_M,
        )
    #
    # Process data.
    #
    # Column rename
    ## e.g. "Lx -> lx" and "lx -> lx". This will cause an error when setting the index.
    tb_lt = tb_lt.rename(columns=COLUMNS_RENAME_LT)

    # Check missing values
    def _check_missing(tb, missing_row_max, missing_countries_max):
        row_nans = tb.isna().any(axis=1)
        assert (
            row_nans.sum() / len(tb) < missing_row_max
        ), f"Too many missing values in life tables: {row_nans.sum()/len(tb)}"

        # Countries missing
        countries_missing_data = tb.loc[row_nans, "country"].unique()
        assert (
            len(countries_missing_data) / len(tb) < missing_countries_max
        ), f"Too many missing values in life tables: {len(countries_missing_data)}"

    _check_missing(tb_lt, 0.01, 14)
    _check_missing(tb_exp, 0.23, 47)
    _check_missing(tb_m, 0.001, 1)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tables = [
        tb_lt.format(["country", "year", "sex", "age", "type", "format"]),
        tb_exp.format(["country", "year", "sex", "age", "type", "format"]),
        tb_m.format(["country", "year", "sex", "age", "type", "format"]),
    ]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def make_tb(path: Path, main_folders: List[str], regex: str) -> Table:
    """Create table from multiple category folders.

    It inspects the content in `main_folders` (should be in `path`), and looks for TXT files to parse into tables.

    The output is a table with the relevant indicators and dimensions for all the categories.

    Arguments:
        path: Path where the HMD export is located.
        main_folders: List of folders to consider in `path`. These should typically be categories, which
                        group different individual indicators
        regex: Regex to extract the metadata for a set of TXTs file found in main_folders. We need this
                because the structure of the header in the TXT files slightly varies depending on
                the indicator.
    """
    # List with all relevant tables
    tbs = []
    # Iterate over each top-level folder
    for category_folder in main_folders:
        main_folder_path = path / category_folder
        if not main_folder_path.is_dir():
            raise FileNotFoundError(f"Folder {main_folder_path} not found in {path}")
        # Iterate over each indicator folder
        for indicator_path in main_folder_path.iterdir():
            if "lexis" in indicator_path.name:
                continue
            if indicator_path.is_dir():
                # Read all TXT files in the indicator folder, and put them as a single table
                paths.log.info(f"Creating list of tables from available files in {path}...")
                files = list(indicator_path.glob("*.txt"))
                tbs_ = [make_tb_from_txt(f, regex) for f in files]
                tbs.extend(tbs_)
    # Concatenate all dataframes
    tb = pr.concat(tbs, ignore_index=True)
    return tb


def make_tb_from_txt(text_path: Path, regex: str) -> Table:
    """Create a table from a TXT file."""
    # Extract fields
    groups = extract_fields(regex, text_path)

    # Build df
    tb = parse_table(groups["data"])

    # Optional melt
    if ("Female" in tb.columns) & ("Male" in tb.columns):
        tb = tb.melt(id_vars=["Age", "Year"], var_name="sex", value_name="deaths")

    # Add dimensions
    tb = tb.assign(
        country=groups["country"],
        type=groups["type"],
        format=groups["format"],
    )

    # Optional sex column
    if "sex" in groups:
        tb["sex"] = groups["sex"]

    return tb


def extract_fields(regex: str, path: Path) -> dict:
    """Structure the fields in the raw TXT file."""
    # Read single file
    with open(path, "r") as f:
        text = f.read()
    # Get relevant fields
    match = re.search(regex, text)
    if match is not None:
        groups = match.groupdict()
    else:
        raise ValueError(f"No match found in {f}! Please revise that source files' content matches FILE_REGEX.")
    return groups


def parse_table(data_raw: str):
    """Given the raw data from the TXT file (as string) map it to a table."""
    tb_str = data_raw.strip()
    tb_str = re.sub(r"\n\s+", "\n", tb_str)
    tb_str = re.sub(r"[^\S\r\n]+", "\t", string=tb_str)
    tb = pr.read_csv(
        StringIO(tb_str),
        sep="\t",
        na_values=["."],
    )

    return tb
