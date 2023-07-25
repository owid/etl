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
# Region mapping
REGIONS_RENAME_EXTRA = {
    1: "Americas",
    2: "Europe",
    4: "Africa",
    6: "Middle East",
    7: "Asia",
    9: "Oceania",
}
# Conflict types rename
CONFLICT_TYPES_RENAME = {
    2: "colonial war",
    3: "imperial war",
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
    # Rename death-related metric columns
    tb_extra = tb_extra.rename(columns={"batdeath": "battle_deaths", "nonstatedeaths": "nonstate_deaths"})

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
    # Rename death-related metric columns
    tb_non = tb_non.rename(columns={"totalcombatdeaths": "battle_deaths"})

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
    tb_inter = tb_inter.rename(columns={"batdeath": "battle_deaths"})

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
    tb_intra = tb_intra.rename(columns={"totalbdeaths": "battle_deaths"})

    # Check that there are no overlapping warnums between tables
    log.info("war.cow: check overlapping warnum in tables")
    _check_overlapping_warnum(tb_extra, tb_non, tb_inter, tb_intra)

    # Process
    tb_extra = make_table_extra(tb_extra)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_extra], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


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
    return tb


## EXTRA-STATE
def make_table_extra(tb: Table) -> Table:
    """Generate extra-state table."""
    log.info("war.cow.extra: Sanity checks")
    _sanity_checks_extra(tb)

    log.info("war.cow.extra: Replace negative values where applicable")
    tb = replace_negative_values(tb)

    log.info("war.cow.extra: keep relevant columns")
    COLUMNS_RELEVANT = [
        "warnum",
        "year_start",
        "year_end",
        "conflict_type",
        "region",
        "battle_deaths",
        "nonstate_deaths",
    ]
    tb = tb[COLUMNS_RELEVANT]

    log.info("war.cow.extra: expand observations")
    tb = expand_observations(tb)

    log.info("war.cow.extra: estimate metrics")
    tb = estimate_metrics(tb)

    # Replace missing values with zeroes
    log.info("war.cow.extra: replace missing values with zeroes")
    tb = replace_missing_data_with_zeros(tb)

    # Rename regions
    log.info("war.cow.extra: rename regions")
    tb["region"] = tb["region"].map(REGIONS_RENAME_EXTRA | {"World": "World"})
    assert tb["region"].isna().sum() == 0, "Unmapped regions!"

    # Rename conflict types
    log.info("war.cow.extra: rename conflict types")
    tb["conflict_type"] = tb["conflict_type"].map(CONFLICT_TYPES_RENAME | {"all": "all"})
    assert tb["conflict_type"].isna().sum() == 0, "Unmapped conflict types!"

    # Set index
    log.info("war.cow.extra: set index")
    tb = tb.set_index(["year", "region", "conflict_type"], verify_integrity=True)

    # Create table
    tb = Table(tb, short_name="cow")
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
    assert set(tb.loc[tb[col] < 0, col]) == {-9, -8}, f"Negative values other than -9 found for {col}"


def replace_negative_values(tb: Table) -> Table:
    """Replace negative values for missing value where applicable

    Some fields use negative values to encode missing values.
    """
    # Replace endyear1=-7 for 2007
    tb.loc[tb["year_end"] == -7, "year_end"] = 2007
    # Replace missing values for deaths
    tb[["battle_deaths", "nonstate_deaths"]] = tb[["battle_deaths", "nonstate_deaths"]].replace(-9, np.nan)
    tb[["nonstate_deaths"]] = tb[["nonstate_deaths"]].replace(-8, np.nan)

    return tb


def expand_observations(tb: Table) -> Table:
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
    tb[["battle_deaths", "nonstate_deaths"]] = (
        tb[["battle_deaths", "nonstate_deaths"]].div(tb["year_end"] - tb["year_start"] + 1, "index").round()
    )

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
        "battle_deaths": sum_nan,
        "nonstate_deaths": sum_nan,
    }
    ## By region and conflict_type
    tb_ongoing = tb.groupby(["year", "region", "conflict_type"], as_index=False).agg(ops)

    ## All conflicts
    tb_ongoing_all_conf = tb.groupby(["year", "region"], as_index=False).agg(ops)
    tb_ongoing_all_conf["conflict_type"] = "all"

    ## World
    tb_ongoing_world = tb.groupby(["year", "conflict_type"], as_index=False).agg(ops)
    tb_ongoing_world["region"] = "World"

    ## World & all conflicts
    tb_ongoing_world_all_conf = tb.groupby(["year"], as_index=False).agg(ops)
    tb_ongoing_world_all_conf["region"] = "World"
    tb_ongoing_world_all_conf["conflict_type"] = "all"

    ## Add region=World
    tb_ongoing = pd.concat([tb_ongoing, tb_ongoing_all_conf, tb_ongoing_world, tb_ongoing_world_all_conf], ignore_index=True).sort_values(  # type: ignore
        by=["year", "region", "conflict_type"]
    )

    ## Rename columns
    tb_ongoing = tb_ongoing.rename(  # type: ignore
        columns={
            "warnum": "number_ongoing_conflicts",
            "battle_deaths": "number_deaths_ongoing_conflicts_battle",
            "nonstate_deaths": "number_deaths_ongoing_conflicts_nonstate",
        }
    )

    return tb_ongoing


def _get_new_metrics(tb: Table) -> Table:
    # Get new #conflicts, by region and conflict type.
    ops = {"warnum": "nunique"}
    ## By region and conflict_type
    tb_new = tb.groupby(["year_start", "region", "conflict_type"], as_index=False).agg(ops)

    ## All conflicts
    tb_new_all_conf = tb.groupby(["year_start", "region"], as_index=False).agg(ops)
    tb_new_all_conf["conflict_type"] = "all"

    ## World
    tb_new_world = tb.groupby(["year_start", "conflict_type"], as_index=False).agg(ops)
    tb_new_world["region"] = "World"

    ## World + all conflicts
    tb_new_world_all_conf = tb.groupby(["year_start"], as_index=False).agg(ops)
    tb_new_world_all_conf["region"] = "World"
    tb_new_world_all_conf["conflict_type"] = "all"

    ## Combine
    tb_new = pd.concat([tb_new, tb_new_all_conf, tb_new_world, tb_new_world_all_conf], ignore_index=True).sort_values(  # type: ignore
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
