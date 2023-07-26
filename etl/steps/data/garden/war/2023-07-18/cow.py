"""Load a meadow dataset and create a garden dataset."""

from typing import List, Set, cast

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from pandas.api.types import is_integer_dtype  # type: ignore
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Logger
log = get_logger()
# Region mapping (extra- and non-state, inter after de-aggregating)
REGIONS_RENAME = {
    1: "Americas",
    2: "Europe",
    4: "Africa",
    6: "Middle East",
    7: "Asia",
    9: "Oceania",
}
# Conflict types rename (this is very granular, and currently not in use)
CONFLICT_TYPES_RENAME = {
    2: "colonial war",
    3: "imperial war",
    8: "non-state (in non-state territory)",
    9: "non-state (accross borders)",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("cow"))

    # Read data (there are four different tables)
    # Extra-state
    log.info("war.cow.extra: read data")
    tb_extra = load_cow_table(
        ds=ds_meadow,
        table_name="extra_state",
        column_start_year="startyear1",
        column_end_year="endyear1",
        column_location="wherefought",
        columns_deaths=["batdeath", "nonstatedeaths"],
        values_exp_wartype={2, 3},
    )

    # Non-state
    log.info("war.cow.non: read data")
    tb_non = load_cow_table(
        ds=ds_meadow,
        table_name="non_state",
        column_start_year="startyear",
        column_end_year="endyear",
        column_location="wherefought",
        columns_deaths=["totalcombatdeaths"],
        values_exp_wartype={8, 9},
    )

    # Inter-state
    log.info("war.cow.inter: read data")
    tb_inter = load_cow_table(
        ds=ds_meadow,
        table_name="inter_state",
        column_start_year="startyear1",
        column_end_year="endyear1",
        column_location="wherefought",
        columns_deaths=["batdeath"],
        values_exp_wartype={1},
        check_unique_for_location=False,
    )
    # Rename death-related metric columns
    tb_inter = tb_inter.rename(columns={"batdeath": "number_deaths_ongoing_conflicts"})
    # Assign conflict_type
    tb_inter["conflict_type"] = "inter-state"

    # Intra-state
    log.info("war.cow.intra: read data")
    tb_intra = load_cow_table(
        ds=ds_meadow,
        table_name="intra_state",
        column_start_year="startyr1",
        column_end_year="endyr1",
        column_location="v5regionnum",
        columns_deaths=["totalbdeaths"],
        values_exp_wartype={4, 5, 6, 7},
    )
    # Rename death-related metric columns
    tb_intra = tb_intra.rename(columns={"totalbdeaths": "number_deaths_ongoing_conflicts"})
    # Assign conflict_type
    tb_intra["conflict_type"] = "intra-state"

    # Check that there are no overlapping warnums between tables
    log.info("war.cow: check overlapping warnum in tables")
    _check_overlapping_warnum(tb_extra, tb_non, tb_inter, tb_intra)

    #
    # Process data.
    #
    tb_extra = make_table_extra(tb_extra)
    tb_non = make_table_nonstate(tb_non)
    tb_inter = make_table_inter(tb_inter)
    # tb_intra = make_table_intra(tb_intra)

    # Combine data
    tb = combine_types(
        tb_extra=tb_extra,
        tb_nonstate=tb_non,
        tb_inter=tb_inter,
        tb_intra=None,
    )

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def combine_types(tb_extra: Table, tb_nonstate: Table, tb_inter: Table, tb_intra: Table) -> Table:
    """Combine the tables from all four different conflict types.

    The original data comes in separate tables: extra-, non-, inter- and intra-state.
    """
    tb = pd.concat(
        [
            tb_extra,
            tb_nonstate,
            tb_inter,
            # tb_intra,
        ],
        ignore_index=True,
    )

    # TODO: Add values for conflict_type='all'

    # Replace missing values with zeroes
    log.info("war.cow: replace missing values with zeroes")
    tb = replace_missing_data_with_zeros(tb)

    # Set index
    log.info("war.cow: set index")
    tb = tb.set_index(["year", "region", "conflict_type"], verify_integrity=True)

    # Create table
    tb = Table(tb, short_name="cow")

    return tb


########################################################################
## EXTRA-STATE #########################################################
########################################################################
def make_table_extra(tb: Table) -> Table:
    """Generate extra-state table."""
    # Rename death-related metric columns
    tb = tb.rename(columns={"batdeath": "battle_deaths", "nonstatedeaths": "nonstate_deaths"})
    # Assign conflict_type
    tb["conflict_type"] = "extra-state"

    log.info("war.cow.extra: Sanity checks")
    _sanity_checks_extra(tb)

    log.info("war.cow.extra: Replace negative values where applicable")
    tb = replace_negative_values_extra(tb)

    log.info("war.cow.extra: obtain total number of deaths")
    tb["number_deaths_ongoing_conflicts"] = tb["battle_deaths"] + tb["nonstate_deaths"]

    log.info("war.cow.extra: keep relevant columns")
    COLUMNS_RELEVANT = [
        "warnum",
        "year_start",
        "year_end",
        "conflict_type",
        "region",
        "number_deaths_ongoing_conflicts",
    ]
    tb = tb[COLUMNS_RELEVANT]

    log.info("war.cow.extra: expand observations")
    tb = expand_observations(tb, column_metrics=["number_deaths_ongoing_conflicts"])  # type: ignore

    log.info("war.cow.extra: estimate metrics")
    tb = estimate_metrics(tb)

    # Rename regions
    log.info("war.cow.extra: rename regions")
    tb["region"] = tb["region"].map(REGIONS_RENAME | {"World": "World"})
    assert tb["region"].isna().sum() == 0, "Unmapped regions!"

    return tb


def _sanity_checks_extra(tb: Table) -> None:
    # Sanity checks
    ## Only one instance per (warnum, ccode1, ccode2) triplet
    assert (
        tb.groupby(["warnum", "ccode1", "ccode2"]).size().max() == 1
    ), "There should only be one instance for each (warnum, ccode1, ccode2) triplet."
    # A conflict only occurs in one region
    assert tb.groupby("warnum")["region"].nunique().max() == 1, "More than one region for some conflicts!"
    # A conflict only is of one type
    assert tb.groupby("warnum")["conflict_type"].nunique().max() == 1, "More than one conflict type for some conflicts!"
    # Check negative values in metric deaths
    col = "battle_deaths"
    assert set(tb.loc[tb[col] < 0, col]) == {-9}, f"Negative values other than -9 found for {col}"
    col = "nonstate_deaths"
    assert set(tb.loc[tb[col] < 0, col]) == {-9, -8}, f"Negative values other than -9 or -8 found for {col}"
    col = "year_end"
    assert set(tb.loc[tb[col] < 0, col]) == {-7}, f"Negative values other than -7 found for {col}"


def replace_negative_values_extra(tb: Table) -> Table:
    """Replace negative values for missing value where applicable.

    Some fields use negative values to encode missing values.
    """
    # Replace endyear1=-7 for 2007
    tb.loc[tb["year_end"] == -7, "year_end"] = 2007
    # Replace missing values for deaths
    tb[["battle_deaths", "nonstate_deaths"]] = tb[["battle_deaths", "nonstate_deaths"]].replace(-9, np.nan)
    tb[["nonstate_deaths"]] = tb[["nonstate_deaths"]].replace(-8, 0)

    return tb


########################################################################
## NON-STATE ###########################################################
########################################################################


def make_table_nonstate(tb: Table) -> Table:
    """Generate non-state table."""
    # Rename death-related metric columns
    tb = tb.rename(columns={"totalcombatdeaths": "number_deaths_ongoing_conflicts"})
    # Assign conflict_type
    tb["conflict_type"] = "non-state"

    log.info("war.cow.non_state: sanity checks")
    _sanity_check_nonstate(tb)

    log.info("war.cow.non_state: replace -9 for NaN")
    tb[["number_deaths_ongoing_conflicts"]] = tb[["number_deaths_ongoing_conflicts"]].replace(-9, np.nan)

    log.info("war.cow.non_state: keep relevant columns")
    COLUMNS_RELEVANT = [
        "warnum",
        "year_start",
        "year_end",
        "conflict_type",
        "region",
        "number_deaths_ongoing_conflicts",
    ]
    tb = tb[COLUMNS_RELEVANT]

    log.info("war.cow.non_state: expand observations")
    tb = expand_observations(tb, column_metrics=["number_deaths_ongoing_conflicts"])  # type: ignore

    log.info("war.cow.non_state: estimate metrics")
    tb = estimate_metrics(tb)

    # Rename regions
    log.info("war.cow.non_state: rename regions")
    tb["region"] = tb["region"].map(REGIONS_RENAME | {"World": "World"})
    assert tb["region"].isna().sum() == 0, "Unmapped regions!"

    return tb


def _sanity_check_nonstate(tb: Table) -> None:
    # Sanity checks
    ## Only one instance per (warnum)
    assert tb.groupby(["warnum"]).size().max() == 1, "There should only be one instance for each (warnum)."
    # A conflict only occurs in one region
    assert tb.groupby("warnum")["region"].nunique().max() == 1, "More than one region for some conflicts!"
    # A conflict only is of one type
    assert tb.groupby("warnum")["conflict_type"].nunique().max() == 1, "More than one conflict type for some conflicts!"
    # Check negative values in metric deaths
    col = "number_deaths_ongoing_conflicts"
    assert set(tb.loc[tb[col] < 0, col]) == {-9}, f"Negative values other than -9 found for {col}"
    # Check year end
    assert (tb["year_end"] > 1819).all(), "Unexpected value for `year_end`!"


########################################################################
## INTER-STATE #########################################################
########################################################################


def make_table_inter(tb: Table) -> Table:
    # Rename death-related metric columns
    tb = tb.rename(columns={"batdeath": "number_deaths_ongoing_conflicts"})
    # Assign conflict type
    tb["conflict_type"] = "inter-state"

    log.info("war.cow.inter: sanity checks")
    _sanity_checks_inter(tb)

    log.info("war.cow.inter: -9 -> NaN")
    tb[["number_deaths_ongoing_conflicts"]] = tb[["number_deaths_ongoing_conflicts"]].replace(-9, np.nan)

    log.info("war.cow.inter: keep relevant columns")
    COLUMNS_RELEVANT = [
        "warnum",
        "year_start",
        "year_end",
        "conflict_type",
        "region",
        "number_deaths_ongoing_conflicts",
    ]
    tb = tb[COLUMNS_RELEVANT]

    log.info("war.cow.inter: expand observations")
    tb = expand_observations(tb, column_metrics=["number_deaths_ongoing_conflicts"])  # type: ignore

    log.info("war.cow.inter: split region composites")
    tb = split_regions_composites(tb)

    log.info("war.cow.inter: estimate metrics")
    tb = estimate_metrics(tb)

    # Rename regions
    log.info("war.cow.non_state: rename regions")
    tb["region"] = tb["region"].map(REGIONS_RENAME | {"World": "World"})
    assert tb["region"].isna().sum() == 0, "Unmapped regions!"

    return tb


def _sanity_checks_inter(tb: Table) -> Table:
    # Sanity checks
    ## Only one instance per (warnum)
    assert (
        tb.groupby(["warnum", "ccode", "side"]).size().max() == 1
    ), "There should only be one instance for each (warnum, ccode, side)."
    # A conflict only is of one type
    assert tb.groupby("warnum")["conflict_type"].nunique().max() == 1, "More than one conflict type for some conflicts!"
    # Check negative values in metric deaths
    col = "number_deaths_ongoing_conflicts"
    assert set(tb.loc[tb[col] < 0, col]) == {-9}, f"Negative values other than -9 found for {col}"
    # Check year end
    assert not (tb["year_end"].isna().any()), "Unexpected NaN value for `year_end`!"


def split_regions_composites(tb: Table) -> Table:
    """Split regions that are region composites.

    The inter-state COW war data dataset contains regions that are region composites, e.g. "Europe & Asia". This function
    deaggregates these region composites so that we only have individual regions (regions as defined by extra- and non-state datasets).
    We allocate the number of deaths evenly across the regions. That is, if a conflict in "Europe & Asia" has 4 deaths, we assign 2 deaths
    to Europe and Asia, respectively.
    """
    REGIONS_INTER_SPLIT = {
        # Europe & Middle East
        11: [2, 6],
        # Europe & Asia
        12: [2, 7],
        # Americas & Asia
        13: [1, 7],
        # Europe & Africa & Middle East
        14: [2, 4, 6],
        # Europe & Africa & Middle East & Asia
        15: [2, 4, 6, 7],
        # Africa, Middle East, Asia & Oceania
        16: [4, 6, 7, 9],
        # Asia & Oceania
        17: [7, 9],
        # Africa & Middle East
        18: [4, 6],
        # Europe, Africa, Middle East, Asia & Oceania
        19: [2, 4, 6, 7, 9],
    }
    tb["region"] = tb["region"].map(REGIONS_INTER_SPLIT).fillna(tb["region"])

    # Scale metric values accordingly
    ## E.g. if there are 10 deaths in a 5-region composite, we assign 10/5 deaths to each region (we assume uniform distribution)
    tb["number_deaths_ongoing_conflicts"] = (
        tb["number_deaths_ongoing_conflicts"] / tb["region"].apply(lambda x: len(x) if isinstance(x, list) else 1)
    ).round()

    # Explode
    tb = tb.explode("region")

    return tb


########################################################################
## INTRA-STATE #########################################################
########################################################################


def make_table_intra(tb: Table) -> Table:
    pass


########################################################################
## GENERIC #############################################################
########################################################################


def _check_overlapping_warnum(tb_extra: Table, tb_non: Table, tb_inter: Table, tb_intra: Table) -> None:
    # CHECK no intersection!
    # EXTRA u NON
    tb_ = tb_extra.merge(tb_non, how="inner", on="warnum")
    assert tb_.empty

    # EXTRA u INTER
    tb_ = tb_extra.merge(tb_inter, how="inner", on="warnum")
    assert tb_.empty

    # EXTRA u INTRA
    tb_ = tb_extra.merge(tb_intra, how="inner", on="warnum")
    assert tb_.empty

    # NON u INTER
    tb_ = tb_non.merge(tb_inter, how="inner", on="warnum")
    assert tb_.empty

    # NON u INTRA
    tb_ = tb_non.merge(tb_intra, how="inner", on="warnum")
    assert tb_.empty

    # INTER u INTRA
    tb_ = tb_inter.merge(tb_intra, how="inner", on="warnum")
    assert tb_.empty


def load_cow_table(
    ds: Dataset,
    table_name: str,
    column_start_year: str,
    column_end_year: str,
    column_location: str,
    columns_deaths: List[str],
    values_exp_wartype: Set[int],
    check_unique_for_location: bool = True,
):
    """Read table from dataset."""
    tb = ds[table_name]

    # Check year start/end (missing?)
    assert (tb[column_start_year] != -9).all(), "There is at least one entry with unknown `column_start_year`"
    assert (tb[column_end_year] != -9).all(), "There is at least one entry with unknown `column_end_year`"

    # Check uniqueness
    ## For each `warnum`, there is only one `warname` and `wartype`. Optionally, also test there is only one `location` (in inter-state, we can have multiple locations).
    cols = ["warname", "wartype"]
    if check_unique_for_location:
        cols += [column_location]
    assert (tb.groupby("warnum")[cols].nunique().sort_values(cols) == 1).all(
        axis=None
    ), "There is multiple (`warname`, `wartype`, `wherefought`) triplets for some `warnum`"

    # Check war types are as expected
    assert is_integer_dtype(tb["wartype"]), "Non integer types found in columnd 'wartype'!"
    assert set(tb["wartype"]) == values_exp_wartype, "Unexpected war type identifiers!"

    # Check location values
    assert is_integer_dtype(tb[column_location]), "Non integer types found in column given by `column_location`!"

    # Keep only relevant columns
    COLUMNS_RELEVANT = [
        "warnum",
        "warname",
        column_start_year,
        column_end_year,
        "wartype",
        column_location,
    ] + columns_deaths
    tb = tb[COLUMNS_RELEVANT + [col for col in tb.columns if col not in COLUMNS_RELEVANT]]

    # Rename
    tb = tb.rename(
        columns={
            column_start_year: "year_start",
            column_end_year: "year_end",
            "wartype": "conflict_type",
            column_location: "region",
        }
    )

    # Dtypes
    for col in columns_deaths:
        tb[col] = tb[col].astype(str).str.replace(",", "").astype("Int64")

    return tb


def expand_observations(tb: Table, column_metrics: List[int]) -> Table:
    """Add year per observation."""
    # Expand to all observation years
    # Add years
    YEAR_MIN = tb["year_start"].min()
    YEAR_MAX = tb["year_end"].max()
    tb_all_years = pd.DataFrame(pd.RangeIndex(YEAR_MIN, YEAR_MAX + 1), columns=["year"])
    tb = tb_all_years.merge(tb, how="cross")  # type: ignore
    ## Filter only entries that actually existed
    tb = tb[(tb["year"] >= tb["year_start"]) & (tb["year"] <= tb["year_end"])]

    # Scale number of deaths
    tb[column_metrics] = tb[column_metrics].div(tb["year_end"] - tb["year_start"] + 1, "index").round()

    return tb


def estimate_metrics(tb: Table) -> Table:
    """Remix table to have the desired metrics.

    These metrics are:
        - number_ongoing_conflicts
        - number_new_conflicts
        - number_deaths_ongoing_conflicts_battle
        - number_deaths_ongoing_conflicts_nonstate

    Parameters
    ----------
    tb : Table
        Table with a row per conflict and year of observation.
    Returns
    -------
    Table
        Table with a row per year, and the corresponding metrics of interest.
    """
    # Get metrics (ongoing and new)
    tb_ongoing = _get_ongoing_metrics(tb)
    tb_new = _get_new_metrics(tb)

    # Combine
    columns_idx = ["year", "region", "conflict_type"]
    tb = tb_ongoing.merge(tb_new, on=columns_idx, how="outer").sort_values(columns_idx)

    return tb


def _get_ongoing_metrics(tb: Table) -> Table:
    # Get ongoing #conflicts and deaths, by region and conflict type.

    def sum_nan(x: pd.Series):
        # Perform summation if all numbers are notna; otherwise return NaN.
        if not x.isna().any():
            return x.sum()
        return np.nan

    ops = {
        "warnum": "nunique",
        "number_deaths_ongoing_conflicts": sum_nan,
    }
    ## By region and conflict_type
    tb_ongoing = tb.groupby(["year", "region", "conflict_type"], as_index=False).agg(ops)

    ## World
    tb_ongoing_world = tb.groupby(["year", "conflict_type"], as_index=False).agg(ops)
    tb_ongoing_world["region"] = "World"

    ## Add region=World
    tb_ongoing = pd.concat([tb_ongoing, tb_ongoing_world], ignore_index=True).sort_values(  # type: ignore
        by=["year", "region", "conflict_type"]
    )

    ## Rename columns
    tb_ongoing = tb_ongoing.rename(  # type: ignore
        columns={
            "warnum": "number_ongoing_conflicts",
        }
    )

    return tb_ongoing


def _get_new_metrics(tb: Table) -> Table:
    """Get new #conflicts, by region and conflict type."""

    # Operations
    ops = {"warnum": "nunique"}

    # By region and conflict_type
    ## Keep one row per (warnum, region) tuple. Otherwise, we might count the same conflict in multiple years!
    tb_ = tb.sort_values("year_start").drop_duplicates(subset=["warnum", "region"], keep="first")
    ## Estimate metrics
    tb_new = tb_.groupby(["year_start", "region", "conflict_type"], as_index=False).agg(ops)

    # World
    ## Keep one row per (warnum).
    tb_ = tb.sort_values("year_start").drop_duplicates(subset=["warnum"], keep="first")
    ## Estimate metrics
    tb_new_world = tb.groupby(["year_start", "conflict_type"], as_index=False).agg(ops)
    tb_new_world["region"] = "World"

    ## Combine
    tb_new = pd.concat([tb_new, tb_new_world], ignore_index=True).sort_values(  # type: ignore
        by=["year_start", "region", "conflict_type"]
    )

    ## Rename columns
    tb_new = tb_new.rename(  # type: ignore
        columns={
            "year_start": "year",
            "warnum": "number_new_conflicts",
        }
    )

    return tb_new


def replace_missing_data_with_zeros(tb: Table) -> Table:
    """Replace missing data with zeros.
    In some instances there is missing data. Instead, we'd like this to be zero-valued.
    """
    # Add missing (year, region, conflict_typ) entries (filled with NaNs)
    years = np.arange(tb["year"].min(), tb["year"].max() + 1)
    regions = set(tb["region"])
    conflict_types = set(tb["conflict_type"])
    new_idx = pd.MultiIndex.from_product([years, regions, conflict_types], names=["year", "region", "conflict_type"])
    tb = tb.set_index(["year", "region", "conflict_type"], verify_integrity=True).reindex(new_idx).reset_index()

    # Change NaNs for 0 for specific rows
    ## For columns "number_ongoing_conflicts", "number_new_conflicts"
    columns = [
        "number_ongoing_conflicts",
        "number_new_conflicts",
    ]
    tb.loc[:, columns] = tb.loc[:, columns].fillna(0)

    return tb
