"""Load a meadow dataset and create a garden dataset."""

from datetime import datetime as dt

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Dataset, Table
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
    paths.log.info("Reading table from meadow dataset.")
    ds_meadow = paths.load_dataset("peace_diehl")
    # Read table from meadow dataset.
    tb = ds_meadow["peace_diehl"].reset_index()
    # Load COW state system
    ds_cow_ssm = paths.load_dataset("cow_ssm")

    #
    # Process data.
    #
    # Fix codes
    codes_to_fix = {
        "936": "946",
        "937": "947",
    }
    columns_codes = ["code_1", "code_2"]
    for col in columns_codes:
        tb[col] = tb[col].replace(codes_to_fix)

    # Set time fields as strings
    paths.log.info("ensure correct types.")
    tb[["time_start", "time_end"]] = tb[["time_start", "time_end"]].astype(str)

    # Times (YYYYMMDD) -> Years (YYYY)
    paths.log.info("get year periods.")
    tb = set_years_in_table(tb)

    # Expand observations
    paths.log.info("expand observations.")
    tb = expand_observations(tb)

    # Replace NaNs with zeroes
    tb["peace_scale_level"] = tb["peace_scale_level"].fillna(0)

    # Region table
    tb_regions = build_tb_regions(tb, ds_cow_ssm)

    # Add region of the relationship
    tb = add_region(tb)

    # Get aggregate table (counts)
    paths.log.info("get aggregate table")
    tb_agg = make_aggregate_table(tb)

    # Add 'no relationship'
    paths.log.info("adding indicator 'no relationship'")
    tb_agg = add_no_relationship(tb_agg, tb_regions)

    # Add short_name
    tb_agg.metadata.short_name = f"{tb.metadata.short_name}_agg"

    # Set index
    paths.log.info("set indexes")
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
            0.75: "warm_peace",
            1: "security_community",
        }
    )

    # World
    ## Format table
    tb_agg_world = tb_agg.groupby(["year", "peace_scale_level"]).size().unstack()
    ## Set country to 'World'
    tb_agg_world["country"] = "World"
    # Reset index
    tb_agg_world = tb_agg_world.reset_index()

    # Regions
    ## Format table
    tb_agg = tb_agg.groupby(["year", "country", "peace_scale_level"]).size().unstack()
    # Reset index
    tb_agg = tb_agg.reset_index()

    tb_agg = pr.concat([tb_agg, tb_agg_world], ignore_index=True)

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


def build_tb_regions(tb: Table, ds_cow_ssm: Dataset) -> Table:
    """Build table with number of countries per region per year (using Diehl's data).

    We only consider countries listed in the dataset. Hence if a country existed but did not have any relationship, it is not counted. We sanity-checked this, and only country with code 260 was missing! This is West Germany. Instead, Diehl uses 255, which stands for Germany. I.e. they consider Germany to exist back then (not as West Germany but as modern Germany already).
    """
    tb_1 = tb[["code_1", "year"]].drop_duplicates().rename(columns={"code_1": "code"})
    tb_2 = tb[["code_2", "year"]].drop_duplicates().rename(columns={"code_2": "code"})
    tb_regions = pr.concat([tb_1, tb_2])
    tb_regions["region"] = tb_regions["code"].apply(code_to_region)
    tb_regions_regions = tb_regions.groupby(["region", "year"], as_index=False)[["code"]].nunique()
    tb_regions_world = tb_regions.groupby(["year"], as_index=False)[["code"]].nunique()
    tb_regions_world["region"] = "World"
    tb_regions = pr.concat([tb_regions_regions, tb_regions_world]).rename(columns={"code": "number_countries"})

    # COW table
    tb_regions_cow = ds_cow_ssm["cow_ssm_system"].reset_index()
    tb_regions_cow = tb_regions_cow[tb_regions_cow["year"] >= 1900]

    missing_codes = set(tb_regions_cow["ccode"]) - (set(tb["code_2"]) | set(tb["code_1"]))
    assert missing_codes == {
        260
    }, "Country codes missing in Diehl, when comparing with COW. Only code expected to miss is 260."

    # Sanity check
    assert (
        tb_regions["year"].max() == tb["year"].max()
    ), "Maximum year does not match between DIEHL and DIEHL-based country in region table."
    assert (
        tb_regions["year"].min() == tb["year"].min()
    ), "Minimum year does not match between DIEHL and DIEHL-based country in region table."
    return tb_regions


def code_to_region(cow_code: int) -> str:
    """Convert code to region name."""
    match cow_code:
        case c if 2 <= c <= 165:
            return "Americas"
        case c if 200 <= c <= 399:
            return "Europe"
        case c if 402 <= c <= 626:
            return "Africa"
        case c if 630 <= c <= 698:
            return "Middle East"
        case c if 700 <= c <= 999:
            return "Asia and Oceania"
        case _:
            raise ValueError(f"Invalid COW code: {cow_code}")


def add_region(tb: Table) -> Table:
    """Add region of the relationship.

    If both countries belong to region A, then the region of the relationship is A. Otherwise, the region is 'Inter-continental'.
    """
    tb["region_1"] = tb["code_1"].apply(code_to_region)
    tb["region_2"] = tb["code_2"].apply(code_to_region)
    assert tb["region_1"].notna().all(), "Some regions are NaN"
    assert tb["region_2"].notna().all(), "Some regions are NaN"
    mask = tb["region_1"] == tb["region_2"]
    tb.loc[mask, "country"] = tb.loc[mask, "region_1"]
    tb.loc[~mask, "country"] = "Inter-continental"
    tb = tb.drop(columns=["region_1", "region_2"])
    return tb


def add_no_relationship(tb: Table, tb_regions: Table) -> Table:
    """Add new column with number of country-pairs with no relationship."""
    # Column types
    columns_index = ["country", "year"]
    columns_indicators = [col for col in tb.columns if col not in columns_index]
    column_new = "no_relation"
    ## Load region numbers, add number of country-pairs in inter-continental
    tb_regions["number_countries"] = tb_regions["number_countries"].astype(float)
    tb_regions["number_country_pairs"] = (
        tb_regions["number_countries"] * (tb_regions["number_countries"] - 1) / 2
    ).astype(int)
    tb_no_rel = tb_regions.groupby("year", as_index=False).apply(_get_intercontinental_pairs)
    tb_no_rel = Table(tb_no_rel).rename(columns={None: "number_country_pairs"})
    tb_no_rel["region"] = "Inter-continental"
    tb_regions = pr.concat([tb_regions, tb_no_rel]).rename(columns={"region": "country"})

    ## Merge with main table
    ## Note that we merge with `outer` mode. This is because `tb` sometimes is missing year entries
    ## that `tb_regions` does have. We need to perform a subtraction later, hence we need all columns!
    tb = tb.merge(tb_regions, on=["year", "country"], how="outer")

    ## Sanity check
    assert not tb["number_country_pairs"].isna().any(), "Some NaNs detected in `number_country_pairs` column!"

    ## Fill NaNs
    tb[columns_indicators] = tb[columns_indicators].fillna(0)

    ## Estimate no-relationship
    tb[column_new] = tb["number_country_pairs"] - tb[columns_indicators].sum(axis=1)

    ## Remove unused columns
    tb = tb[columns_index + columns_indicators + [column_new]]
    return tb


def _get_intercontinental_pairs(tb_group: Table):
    # expected_regions = {
    #     "Africa",
    #     "Americas",
    #     "Asia and Oceania",
    #     "Europe",
    #     "Middle East",
    #     "World",
    # }
    mask = tb_group["region"] == "World"
    value = tb_group.loc[mask, "number_country_pairs"] - tb_group.loc[~mask, "number_country_pairs"].sum()
    return value.item()
    # if set(tb_group["region"]) == expected_regions:
    #     mask = tb_group["region"] == "World"
    #     value = (tb_group.loc[mask, "number_country_pairs"] - tb_group.loc[~mask, "number_country_pairs"].sum())
    #     return value.item()
    # return np.nan
