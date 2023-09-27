"""Load a meadow dataset and create a garden dataset."""

from datetime import datetime as dt

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table
from structlog import get_logger

# from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
log = get_logger()

# There are some errors in reported start/end years
## To fix this we either go to file `peacedatav31dymon.csv` and check the years there of
## check the data itself, in case there are previous or following year periods.
START_YEAR_FIXES = {
    "19620086": "19620806",  # dyad code 51365
}
END_YEAR_FIXES = {
    "19699025": "19690925",  # dyad code 435600
    "19971913": "19971013",  # dyad code 484540
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("peace_diehl")

    # Read table from meadow dataset.
    log.info("peace_diehl: Reading table from meadow dataset.")
    tb = ds_meadow["peace_diehl"].reset_index()

    #
    # Process data.
    #
    # Set time fields as strings
    log.info("peace_diehl: Ensure correct types.")
    tb[["time_start", "time_end"]] = tb[["time_start", "time_end"]].astype(str)

    # Times (YYYYMMDD) -> Years (YYYY)
    log.info("peace_diehl: Get year periods.")
    tb = set_years_in_table(tb)

    # Expand observations
    log.info("peace_diehl: Expand observations.")
    tb = expand_observations(tb)

    # Replace NaNs with zeroes
    tb["peace_scale_level"] = tb["peace_scale_level"].fillna(0)

    # Harmonize countries
    # tb = geo.harmonize_countries(
    #     df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    # )

    # Get aggregate table (counts)
    log.info("peace_diehl: Get aggregate table.")
    tb_agg = make_aggregate_table(tb)
    tb_agg.metadata.short_name = f"{tb.metadata.short_name}_agg"

    # Set index
    log.info("peace_diehl: Set indexes.")
    tb = tb.set_index(["code_1", "code_2", "year"], verify_integrity=True)[["peace_scale_level"]].sort_index()
    tb_agg = tb_agg.set_index(["country", "year"], verify_integrity=True).sort_index()

    # Define list of tables
    tables = [
        tb,
        tb_agg,
    ]

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=tables, check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def set_years_in_table(tb: Table) -> Table:
    """Get start and end years for each entry.

    Each entry in the table describes a period of time and the relationship of a country pair. The period of time is defined by fields `time_start` and `time_end`, which are in the format of YYYYMMDD. This function maps these to a year.
    """
    # Small assertions
    assert set(tb[tb["time_start"].str.contains("99$")]["time_start"]) == {
        "19009999"
    }, "Unexpected time_start ending with 99, other than 19009999"
    assert set(tb[tb["time_end"].str.contains("99$")]["time_end"]) == {
        "20209999"
    }, "Unexpected time_end ending with 99, other than 20209999"

    # Known bugs, fix
    tb["time_start"] = tb["time_start"].replace(START_YEAR_FIXES)
    tb["time_end"] = tb["time_end"].replace(END_YEAR_FIXES)

    # Wrong end year (if not fixed, there are different peace levels in overlapping year periods)
    tb.loc[(tb["code_1"] == 370) & (tb["code_2"] == 701) & (tb["peace_scale_level"] == 0.5), "time_end"] = "20150101"

    # Create groups
    tbg = tb.groupby(["code_1", "code_2"])

    # Build table
    tbs = []
    for _, tb in tbg:
        tb.loc[:, "time_start"] = tb.loc[:, "time_start"].apply(time_to_year, start=True)
        tb.loc[:, "time_end"] = tb.loc[:, "time_end"].apply(time_to_year)
        tbs.append(tb)
    tb = pr.concat(tbs, ignore_index=True)
    return tb


def expand_observations(tb: Table) -> Table:
    """Transform table into long format.

    Originally, the table is expected to have an entry per year period. E.g.:

        code_1, code_2, time_start, time_end, peace_scale_level
            02      20        2000      2003                0.5

    This function expands the table to have an entry per year. E.g.:

        code_1, code_2, year, peace_scale_level
            02      20  2000                0.5
            02      20  2001                0.5
            02      20  2002                0.5
            02      20  2003                0.5

    """
    # Get code-year observation
    YEAR_MIN = tb["time_start"].min()
    YEAR_MAX = tb["time_end"].max()
    tb_all_years = Table(pd.DataFrame(pd.RangeIndex(YEAR_MIN, YEAR_MAX + 1), columns=["year"]))
    # tb = pd.DataFrame(tb)  # to prevent error "AttributeError: 'DataFrame' object has no attribute 'all_columns'"
    tb = tb.merge(tb_all_years, how="cross")
    ## Filter only entries that actually existed
    tb = tb[(tb["year"] >= tb["time_start"]) & (tb["year"] < tb["time_end"])]
    return tb


def make_aggregate_table(tb: Table) -> Table:
    """Aggregate table.

    Obtain the global number of each type of peace scale level relationship per year.
    """
    tb_agg = tb.copy()
    # Map peace scale levels to readable labels
    tb_agg["peace_scale_level"] = tb_agg["peace_scale_level"].map(
        {
            0: "severe_rivalry",
            0.25: "lesser_rivalry",
            0.5: "negative_peace",
            0.75: "positive_peace",
            1: "security_community",
        }
    )

    # Format table
    tb_agg = tb_agg.groupby(["year", "peace_scale_level"]).size().unstack()

    # Set country to 'World'
    tb_agg["country"] = "World"

    # Reset index
    tb_agg = tb_agg.reset_index()

    # Propagate metadata
    for column in tb_agg.all_columns:
        tb_agg[column].metadata.origins = tb["peace_scale_level"].metadata.origins

    # Replace NaNs with zeroes
    tb_agg = tb_agg.fillna(0)

    return tb_agg


def time_to_year(t: str, start: bool = False) -> int:
    """Map raw time in dataset to year.

    The original table contains time in the format YYYYMMDD. This function maps it to a year. Given a time "YYYYMMDD", it is assigned
    to can be assigned to year YYYY, or YYYY + 1. The rule is:

        - If it concerns a start time (`start=True`), then we map YYYYMMDD -> YYYY
        - If it concerns an end time (`start=False`), then we map:
            - YYYYMMDD -> YYYY
            - YYYY1231 -> YYYY + 1

    Note that sometimes, the original time is not a valid date:

        - "20209999": Mapped to 2021.
        - "YYYY0000": Unclear what it is denoting. We assume "YYYY0101".
        - "YYYYMM00": Unclear what it is denoting. We assume "YYYYMM01".

    All of this assumes that `expand_observations` uses:
        - observation year >= time_start
        - observation year < time_end (strict)
    """
    if len(t) != 8:
        raise ValueError(f"Invalid time: {t}. Format should be YYYYMMDD.")
    # Quick fixes
    if t == "19009999":
        return 1900
    elif t == "20209999":
        return 2021
    elif t[4:] == "0000":
        t = t[:4] + "0101"
    elif t[6:] == "00":
        t = t[:6] + "01"

    date = dt.strptime(t, "%Y%m%d")
    year = date.year
    if start:
        return year
    else:
        if (date.month == 12) & (date.day == 31):
            return year + 1
        return year
