"""Load a snapshot and create a meadow dataset."""

import re
from io import StringIO
from pathlib import Path
from typing import List

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Life tables
FOLDERS_LT = [
    "lt_male",
    "lt_female",
    "lt_both",
    "c_lt_male",
    "c_lt_female",
    "c_lt_both",
]
REGEX_LT = (
    r"Life tables \((?P<type>[a-zA-Z]+) (?P<format>\d+x\d+)\), (?P<sex>[a-zA-Z]+)"
    r"\s*\tLast modified: (?P<last_modified>\d+ [a-zA-Z]{3} \d+); Methods Protocol: v\d+ \(\d+\)\n\n(?P<data>(?s:.)*)"
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

# Population
FOLDERS_POP = [
    "population",
]
REGEX_POP = (
    r"(?P<name>Population) size \((?P<format>1\-year|abridged)\)"
    r"\s*\tLast modified: (?P<last_modified>\d+ [a-zA-Z]{3} \d+); Methods Protocol: v\d+ \(\d+\)\n\n(?P<data>(?s:.)*)"
)

# Exposures
FOLDERS_EXPOSURES = [
    "c_exposures",
    "exposures",
]
REGEX_EXP = (
    r"(?P<name>Exposure) to risk \((?P<type>[a-zA-Z]+) (?P<format>\d+x\d+)\),\s\tLast modified: "
    r"(?P<last_modified>\d+ [a-zA-Z]{3} \d+);\s*Methods Protocol: v\d+ \(\d+\)\n\n(?P<data>(?s:.)*)"
)

# Mortality
FOLDERS_MOR = [
    "deaths",
]
REGEX_MOR = (
    r"(?P<name>Deaths) \((?P<type>[a-zA-Z]+) (?P<format>\d+x\d+|Lexis triangle)\),\s\tLast modified: "
    r"(?P<last_modified>\d+ [a-zA-Z]{3} \d+);\s*Methods Protocol: v\d+ \(\d+\)\n\n(?P<data>(?s:.)*)"
)


# Births
FOLDERS_BIRTHS = [
    "births",
]
REGEX_BIRTHS = (
    r"(?P<name>Births) \((?P<format>1\-year)\)\s+Last modified: "
    r"(?P<last_modified>\d+ [a-zA-Z]{3} \d+);\s+Methods Protocol: v\d+ \(\d+\)\n\n(?P<data>(?s:.)*)"
)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("hmd.zip")

    # Load data from snapshot.
    with snap.extract_to_tempdir() as tmpdir:
        # Navigate into the hmd_statistics subdirectory (new structure in 2025 snapshot)
        hmd_data_dir = Path(tmpdir) / "hmd_statistics_20250925"
        if not hmd_data_dir.exists():
            # Fallback to old structure (directly in tmpdir)
            hmd_data_dir = Path(tmpdir)

        # Life tables
        tb_lt = make_tb(
            path=hmd_data_dir,
            main_folders=FOLDERS_LT,
            regex=REGEX_LT,
            snap=snap,
        )

        # Population
        tb_pop = make_tb(
            path=hmd_data_dir,
            main_folders=FOLDERS_POP,
            regex=REGEX_POP,
            snap=snap,
        )

        # Exposure
        tb_exp = make_tb(
            path=hmd_data_dir,
            main_folders=FOLDERS_EXPOSURES,
            regex=REGEX_EXP,
            snap=snap,
        )

        # Mortality
        tb_m = make_tb(
            path=hmd_data_dir,
            main_folders=FOLDERS_MOR,
            regex=REGEX_MOR,
            snap=snap,
        )

        # Births
        tb_bi = make_tb(
            path=hmd_data_dir,
            main_folders=FOLDERS_BIRTHS,
            regex=REGEX_BIRTHS,
            snap=snap,
        )

    # Life tables
    ## Column rename
    ## e.g. "Lx -> lx" and "lx -> lx". This will cause an error when setting the index.
    tb_lt = tb_lt.rename(columns=COLUMNS_RENAME_LT)

    # Check missing values
    _check_nas(tb_lt, 0.01, 14)
    _check_nas(tb_exp, 0.23, 47)
    _check_nas(tb_m, 0.001, 1)
    _check_nas(tb_pop, 0.001, 1)

    # Ensure correct year dtype
    tb_lt = _clean_year(tb_lt)
    tb_exp = _clean_year(tb_exp)
    tb_m = _clean_year(tb_m)
    tb_bi = _clean_year(tb_bi)
    tb_pop = _clean_population_type(tb_pop)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tables = [
        tb_lt.format(["popname", "year", "sex", "age", "type", "format"], short_name="life_tables"),
        tb_exp.format(["popname", "year", "sex", "age", "type", "format"], short_name="exposures"),
        tb_m.format(["popname", "year", "sex", "age", "type", "format"], short_name="deaths"),
        tb_pop.format(["popname", "year", "sex", "age", "format"], short_name="population"),
        tb_bi.format(["popname", "year", "sex", "format"], short_name="births"),
    ]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(
        tables=tables,
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def make_tb(path: Path, main_folders: List[str], regex: str, snap) -> Table:
    """Create table from multiple category folders.

    It inspects the content in `main_folders` (should be in `path`), and looks for TXT files to parse into tables.

    The output is a table with the relevant indicators and dimensions for all the categories.

    Arguments:
        path: Path where the HMD export is located.
        main_folders: List of folders to consider in `path`. These should typically be categories, which group different individual indicators
    """
    # Get relevant paths
    filepaths = _get_relevant_paths(path, main_folders)
    paths.log.info(f"Got {len(filepaths)} relevant files")

    # Parse txt files
    tbs = []
    for filepath in filepaths:
        tb_ = make_tb_from_txt(filepath, regex, snap)
        tbs.append(tb_)
    paths.log.info(f"Parsed {len(tbs)} tables")

    # Concatenate all dataframes
    tb = pr.concat(tbs, ignore_index=True)
    paths.log.info(f"Concatenated table has {len(tb)} rows")

    return tb


def _get_relevant_paths(path: Path, main_folders: List[str]):
    """Get list with all relevant TXT file paths in the HMD export structure."""
    filepaths = []
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
                # Check directory, there should only be one TXT file (format change since 2025)
                files = list(indicator_path.glob("*.txt"))
                assert len(files) == 1, f"Expected only one TXT file in {indicator_path}, found {len(files)}"
                filepaths.append(files[0])
    return filepaths


def make_tb_from_txt(filepath: Path, regex: str, snap) -> Table:
    """Create a table from a TXT file."""
    # print(text_path)
    # Extract fields
    groups = extract_fields(regex, filepath)

    # Build df
    tb = parse_table(groups["data"], snap)

    # Optional melt
    if ("Female" in tb.columns) and ("Male" in tb.columns):
        id_vars = [col for col in ["Age", "Year", "PopName"] if col in tb.columns]
        if "name" not in groups:
            raise ValueError(
                f"Indicator name not found in {filepath}! Please revise that source files' content matches FILE_REGEX."
            )
        tb = tb.melt(id_vars=id_vars, var_name="sex", value_name=groups["name"])

    # Optional sex column
    if "sex" in groups:
        tb["sex"] = groups["sex"]
    if "format" in groups:
        tb["format"] = groups["format"]
    if "type" in groups:
        tb["type"] = groups["type"]
    return tb


def extract_fields(regex: str, filepath: Path) -> dict:
    """Structure the fields in the raw TXT file."""
    # Read single file
    with open(filepath, "r") as f:
        text = f.read()

    # Clean (strip leading/trailing spaces)
    text = "\n".join(line.rstrip() for line in text.splitlines())

    # Get relevant fields
    matched = re.search(regex, text)
    if matched is not None:
        groups = matched.groupdict()
    else:
        raise ValueError(f"No match found in {f}! Please revise that source files' content matches FILE_REGEX.")
    return groups


def parse_table(data_raw: str, snap):
    """Given the raw data from the TXT file (as string) map it to a table."""
    tb_str = data_raw.strip()
    tb_str = re.sub(r"\n\s+", "\n", tb_str)
    tb_str = re.sub(r"[^\S\r\n]+", "\t", string=tb_str)
    tb = pr.read_csv(
        StringIO(tb_str),
        sep="\t",
        na_values=["."],
        metadata=snap.to_table_metadata(),
        origin=snap.m.origin,
    )

    return tb


def _check_nas(tb, missing_row_max, missing_countries_max):
    """Check missing values & countries in data."""
    row_nans = tb.isna().any(axis=1)
    assert (
        row_nans.sum() / len(tb) < missing_row_max
    ), f"Too many missing values in life tables: {row_nans.sum()/len(tb)}"

    # Countries missing
    countries_missing_data = tb.loc[row_nans, "PopName"].unique()
    assert (
        len(countries_missing_data) / len(tb) < missing_countries_max
    ), f"Too many missing values in life tables: {len(countries_missing_data)}"


def _clean_population_type(tb):
    """Data provider notes the following:

    For populations with territorial changes, two sets of population estimates are given for years in which a territorial change occurred. The first set of estimates (identified as year "19xx-") refers to the population just before the territorial change, whereas the second set (identified as year "19xx+") refers to the population just after the change. For example, in France, the data for "1914-" cover the previous territory (i.e., as of December 31, 1913), whereas the data for "1914+" reflect the territorial boundaries as of January 1, 1914.

    To avoid confusion and duplicity, whenever there are multiple entries for a year, we keep YYYY+ definition for the year (e.g. country with new territorial changes).
    """
    # Create new column with the year.
    regex = r"\b\d{4}\b"
    tb["year"] = tb["Year"].astype("string").str.extract(f"({regex})", expand=False)
    assert tb["year"].notna().all(), "Year extraction was successful!"
    tb["year"] = tb["year"].astype(int)

    # Ensure raw year is as expected
    assert (
        tb.groupby(["PopName", "year", "Age", "sex", "format"]).Year.nunique().max() == 2
    ), "Unexpected number of years (+/-)"

    # Drop duplicate years, keeping YYYY+.
    tb["Year"] = tb["Year"].astype("string")
    tb = tb.sort_values("Year")
    tb = tb.drop_duplicates(subset=["year", "Age", "sex", "PopName", "format"], keep="first").drop(columns="Year")

    tb = tb.rename(columns={"year": "Year"})

    # Additionally, remove year periods
    tb = _clean_year(tb)

    return tb


def _clean_year(tb):
    # Remove year ranges, and convert to int
    flag = tb["Year"].astype("string").str.contains("-")
    tb = tb.loc[~flag]
    tb.loc[:, "Year"] = tb.loc[:, "Year"].astype("int")
    return tb
