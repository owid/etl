"""COW War data dataset.

- This dataset is built from four different files. Each of them come from the COW site, but have minor differences in
the fields they contain, etc.

- Each of the source files concerns a specific conflict type (i.e. one file only contains data on "inter-state" conflicts).
    - Therefore, a conflict (or at least how it is identified in this dataset) never changes its type.
    - There are fields explaining if a conflict transformed to another conflict (i.e. it stops being in the "inter-state" table with id X, and
    starts being in the "intra-state" table with id Y).


ON REGIONS

    - Handling regions in COW dataset is complex because we are integrating four different datasets:
        - Extra-state: Regions are as defined by REGIONS_RENAME.

                        Originally encoded regions:

                        1 = W. Hemisphere
                        2 = Europe
                        4 = Africa
                        6 = Middle East
                        7 = Asia
                        9 = Oceania

        - Non-state: Regions are as defined by REGIONS_RENAME.

                        Originally encoded regions:

                        1 = W. Hemisphere
                        2 = Europe
                        4 = Africa
                        6 = Middle East
                        7 = Asia
                        9 = Oceania

        - Inter-state: Contains several region composites(*). We de-aggregate these and distribute deaths uniformly across its regions.
                        E.g., if there are 10 deaths in "Asia & Europe", we assign 5 deaths to Asia and 5 deaths to Europe.

                        Originally encoded regions:

                        1 = W. Hemisphere
                        2 = Europe
                        4 = Africa
                        6 = Middle East
                        7 = Asia
                        9 = Oceania
                        11 = Europe & Middle East
                        12 = Europe & Asia
                        13 = W. Hemisphere & Asia
                        14 = Europe, Africa & Middle East
                        15 = Europe, Africa, Middle East, & Asia
                        16 = Africa, Middle East, Asia & Oceania
                        17 = Asia & Oceania
                        18 = Africa & Middle East
                        19 = Europe, Africa, Middle East, Asia & Oceania

        - Intra-state: For some reason, regions follow a different numbering. Therefore we standardise this in `standardise_region_ids` (i.e. assign the correct numbers).
                        Also, we have "undone" region composites. Note that we can't assign a conflict to multiple regions, since these are intra-state (i.e. should only be mapped to one region!).

                        TODO: Why does intra-state use a different region numbering? (contact people from COW)

                        Originally encoded regions:

                        1 = North America
                        2 = South America
                        3 = Europe
                        4 = Sub-Saharan Africa
                        5 = Middle East and North Africa
                        6 = Asia and Oceania.

    - For consistency with other datasets, we rename "Asia" -> "Asia and Oceania", and "Oceania" -> "Asia and Oceania".

    (*) A "region composite" is a region such that it is a combination of several regions, e.g. "Europe & Asia".

"""

import json
from typing import List, Set, Tuple

import numpy as np
import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Dataset, Table
from pandas.api.types import is_integer_dtype  # type: ignore
from shared import (
    add_indicators_extra,
    add_region_from_code,
    aggregate_conflict_types,
    expand_observations,
    fill_gaps_with_zeroes,
    get_number_of_countries_in_conflict_by_region,
)
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Logger
log = get_logger()
# Region mapping (extra- and non-state, intra after region code standardisation, and inter after de-aggregating composite regions)
# Source suffix (e.g. '(COW)') is added later, in `combine_tables`
REGIONS_RENAME = {
    1: "Americas",
    2: "Europe",
    4: "Africa",
    6: "Middle East",
    # There are three regions that we map to "Asia and Oceania"
    7: "Asia and Oceania",  # Originally "Asia"
    9: "Asia and Oceania",  # Originally "Oceania"
    17: "Asia and Oceania",  # Originally "Asia & Oceania"
}
# Mapping conflicts to regions for some intra-state conflicts
## We map intrastate conflicts to only one region. We have region composites (ME & NA, Asia & Oceania) and we need to find the actual region.
## We have done this manually, and is summarised in the dictionary below.
PATH_CUSTOM_REGIONS_INTRASTATE = paths.directory / "cow.intrastate.region_mapping_custom.json"

# Last reported end years for each of the building datasets
END_YEAR_MAX_EXTRA = 2007
END_YEAR_MAX_INTRA = 2014
END_YEAR_MAX_INTER = 2003
END_YEAR_MAX_NONSTATE = 2005
END_YEAR_MAX = min([END_YEAR_MAX_EXTRA, END_YEAR_MAX_INTRA, END_YEAR_MAX_INTER, END_YEAR_MAX_NONSTATE])
# Conflict types
CTYPE_EXTRA = "extra-state"
CTYPE_INTRA = "intra-state"
CTYPE_INTRA_INTL = f"{CTYPE_INTRA} (internationalized)"
CTYPE_INTRA_NINTL = f"{CTYPE_INTRA} (non-internationalized)"
CTYPE_INTER = "inter-state"
CTYPE_NONSTATE = "non-state"
CTYPE_SBASED = "state-based"


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("cow")

    # Read data (there are four different tables)
    log.info("war.cow: load data")
    tb_extra, tb_nonstate, tb_inter, tb_intra = load_tables(ds_meadow)

    # Read table from COW codes
    ds_cow_ssm = paths.load_dataset("cow_ssm")
    tb_regions = ds_cow_ssm["cow_ssm_regions"].reset_index()
    tb_codes = ds_cow_ssm["cow_ssm_countries"]
    tb_system = ds_cow_ssm["cow_ssm_system"].reset_index()

    # Read supplementary table (for locations)
    ds_chupilkin = paths.load_dataset("chupilkin_koczan")
    tb_chupilkin = ds_chupilkin["chupilkin_koczan"].reset_index()

    # Check that there are no overlapping warnums between tables
    log.info("war.cow: check overlapping warnum in tables")
    _check_overlapping_warnum(tb_extra, tb_nonstate, tb_inter, tb_intra)

    #
    # Process data.
    #
    # Format individual tables
    tb_extra = make_table_extra(tb_extra)  # finishes 2007
    tb_nonstate = make_table_nonstate(tb_nonstate)  # finishes 2005
    tb_inter = make_table_inter(tb_inter)  # finishes 2003
    tb_intra = make_table_intra(tb_intra)  # finishes 2014

    # Get country-level stuff
    paths.log.info("getting country-level indicators")
    tb_participants = estimate_metrics_participants(
        tb_extra=tb_extra, tb_intra=tb_intra, tb_inter=tb_inter, tb_codes=tb_codes
    )

    # Post-processing
    log.info("war.cow.extra: obtain total number of deaths (assign lower bound value to missing values)")
    tb_extra = aggregate_rows_by_periods_extra(tb_extra)
    log.info("war.cow.inter: assign lower bound of deaths where value is missing")
    tb_inter = aggregate_rows_by_periods_inter(tb_inter)
    log.info("war.cow.inter: split region composites")
    tb_inter = split_regions_composites(tb_inter)

    # Locations data
    ## Get conflict type into the chupilkin table
    paths.log.info("estimate locations of conflicts")
    tb_locations = estimate_metrics_locations(tb_chupilkin, tb_system, tb_participants)

    # Combine data
    tb = combine_tables(
        tb_extra=tb_extra,
        tb_nonstate=tb_nonstate,
        tb_inter=tb_inter,
        tb_intra=tb_intra,
        tb_regions=tb_regions,
    )

    # TEMPORARY
    # I was asked to remove all the data on death indicators from regions.
    # I decided not to remove the prior code estimating these bc it was very time consuming
    # Hence, if we decide to go back and incorporate these, we just need to comment the lines below.
    index_names = list(tb.index.names)
    tb = tb.reset_index()
    assert len(set(tb["region"])) == 6, "Number of regions (including 'World') is not 6!"
    tb.loc[
        tb["region"] != "World", ["number_deaths_ongoing_conflicts", "number_deaths_ongoing_conflicts_per_capita"]
    ] = np.nan
    tb = tb.set_index(index_names, verify_integrity=True)

    # Set new short_name
    tb.m.short_name = "cow"

    #
    # Save outputs.
    #
    tables = [
        tb,
        tb_participants,
        tb_locations,
    ]
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=tables, check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def load_tables(ds: Dataset) -> Tuple[Table, Table, Table, Table]:
    """Load all CoW tables.

    This includes tables for extra-, non-, inter- and intra-state conflicts.
    """
    # Extra-state
    log.info("war.cow.extra: read data")
    tb_extra = load_cow_table(
        ds=ds,
        table_name="cow_extra_state",
        column_start_year="startyear1",
        column_end_year="endyear1",
        column_location="wherefought",
        columns_deaths=["batdeath", "nonstatedeaths"],
        values_exp_wartype={2, 3},
    )

    # Non-state
    log.info("war.cow.non: read data")
    tb_nonstate = load_cow_table(
        ds=ds,
        table_name="cow_non_state",
        column_start_year="startyear",
        column_end_year="endyear",
        column_location="wherefought",
        columns_deaths=["totalcombatdeaths"],
        values_exp_wartype={8, 9},
    )

    # Inter-state
    log.info("war.cow.inter: read data")
    tb_inter = load_cow_table(
        ds=ds,
        table_name="cow_inter_state",
        column_start_year="startyear1",
        column_end_year="endyear1",
        column_location="wherefought",
        columns_deaths=["batdeath"],
        values_exp_wartype={1},
        check_unique_for_location=False,
    )

    # Intra-state
    log.info("war.cow.intra: read data")
    tb_intra = load_cow_table(
        ds=ds,
        table_name="cow_intra_state",
        column_start_year="startyr1",
        column_end_year="endyr1",
        column_location="v5regionnum",
        columns_deaths=["totalbdeaths"],
        values_exp_wartype={4, 5, 6, 7},
    )

    return tb_extra, tb_nonstate, tb_inter, tb_intra


def combine_tables(tb_extra: Table, tb_nonstate: Table, tb_inter: Table, tb_intra: Table, tb_regions: Table) -> Table:
    """Combine the tables from all four different conflict types.

    The original data comes in separate tables: extra-, non-, inter- and intra-state.
    """
    # Get relevant columns
    log.info("war.cow: keep relevant columns")
    COLUMNS_RELEVANT = [
        "warnum",
        "year_start",
        "year_end",
        "conflict_type",
        "region",
        "number_deaths_ongoing_conflicts",
    ]
    tb_extra = tb_extra[COLUMNS_RELEVANT]
    tb_nonstate = tb_nonstate[COLUMNS_RELEVANT]
    tb_inter = tb_inter[COLUMNS_RELEVANT]
    tb_intra = tb_intra[COLUMNS_RELEVANT]

    # Fill nulls with zeroes (TODO: this might be wrong, lower-bound is prefered)
    log.info("war.cow: filling NaNs")
    assert (
        not tb_nonstate["number_deaths_ongoing_conflicts"].isna().any()
    ), "Unexpected NaNs found in `number_deaths_ongoing_conflicts`."
    assert (
        not tb_intra["number_deaths_ongoing_conflicts"].isna().any()
    ), "Unexpected NaNs found in `number_deaths_ongoing_conflicts`."
    assert (
        not tb_inter["number_deaths_ongoing_conflicts"].isna().any()
    ), "Unexpected NaNs found in `number_deaths_ongoing_conflicts`."
    assert (
        not tb_intra["number_deaths_ongoing_conflicts"].isna().any()
    ), "Unexpected NaNs found in `number_deaths_ongoing_conflicts`."

    # Check NaNs and negative values in tables
    log.info("war.cow: checking NaNs and negative values")

    def _checks(tb: Table, tb_name: str) -> None:
        assert not tb.isna().any(axis=None), f"There are some NaN values in `{tb_name}`!"
        assert not (
            tb["number_deaths_ongoing_conflicts"] < 0
        ).any(), f"There are negative values NaN values in `{tb_name}`!"

    _checks(tb_extra, "tb_extra")
    _checks(tb_nonstate, "tb_nonstate")
    _checks(tb_inter, "tb_inter")
    _checks(tb_intra, "tb_intra")

    # Concatenate
    log.info("war.cow: concatenate tables")
    tb = pr.concat(
        [
            tb_extra,
            tb_nonstate,
            tb_inter,
            tb_intra,
        ],
        ignore_index=True,
    )

    # Rename regions
    log.info("war.cow: rename regions")
    tb["region"] = tb["region"].map(REGIONS_RENAME)
    assert tb["region"].isna().sum() == 0, "Unmapped regions!"

    # Add yearly observations (scale values)
    log.info("war.cow: expand observations")
    tb = expand_observations(
        tb,
        col_year_start="year_start",
        col_year_end="year_end",
        cols_scale=["number_deaths_ongoing_conflicts"],
    )
    # Estimate metrics (do the aggregations)
    log.info("war.cow: estimate metrics")
    tb = estimate_metrics(tb)

    # Replace missing values with zeroes
    log.info("war.cow: replace missing values with zeroes")
    tb = replace_missing_data_with_zeros(tb)

    log.info("war.cow: map fatality codes to names")
    tb = add_indicators_extra(
        tb,
        tb_regions,
        columns_conflict_rate=["number_ongoing_conflicts", "number_new_conflicts"],
        columns_conflict_mortality=["number_deaths_ongoing_conflicts"],
    )

    # Add suffix with source name
    msk = tb["region"] != "World"
    tb.loc[msk, "region"] = tb.loc[msk, "region"] + " (COW)"

    # Sanity check on NaNs
    log.info("war.cow: check NaNs in `number_deaths_ongoing_conflicts`")
    assert (
        not tb["number_deaths_ongoing_conflicts"].isna().any()
    ), "Some NaNs found in `number_deaths_ongoing_conflicts`!"

    # Fill gaps with zeroes
    paths.log.info("fill gaps with zeroes")
    tb = fill_gaps_with_zeroes(
        tb,
        columns=["region", "year", "conflict_type"],
        cols_use_range=["year"],
    )
    # Keep correct year coverage by conflict type
    tb = tb[
        # EXTRA, NON-STATE, ALL (2007)
        ((tb["conflict_type"].isin([CTYPE_EXTRA, CTYPE_NONSTATE, "all"])) & (tb["year"] <= 2007))
        # INTER, STATE-BASED (2010)
        | ((tb["conflict_type"].isin([CTYPE_INTER, CTYPE_SBASED])) & (tb["year"] <= 2010))
        # INTRA (2014)
        | ((tb["conflict_type"].isin([CTYPE_INTRA, CTYPE_INTRA_INTL, CTYPE_INTRA_NINTL])) & (tb["year"] <= 2014))
    ]

    # Set index
    log.info("war.cow: set index")
    tb = tb.set_index(["year", "region", "conflict_type"], verify_integrity=True)

    return tb


########################################################################
## EXTRA-STATE #########################################################
########################################################################
def make_table_extra(tb: Table) -> Table:
    """Generate extra-state table."""
    # Rename death-related metric columns
    tb = tb.rename(columns={"batdeath": "battle_deaths", "nonstatedeaths": "nonstate_deaths"})
    # Assign conflict_type
    tb["conflict_type"] = CTYPE_EXTRA

    log.info("war.cow.extra: Sanity checks")
    _sanity_checks_extra(tb)

    log.info("war.cow.extra: replace negative values where applicable")
    tb = replace_negative_values_extra(tb)
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
    # Check max end year is as expected
    assert (
        tb["year_end"].max() == END_YEAR_MAX_EXTRA - 1
    ), f"Extra-state data is expected to have its latest end year by {END_YEAR_MAX_EXTRA - 1}, but that was not the case! Revisit this assertion and 'set NaNs' section in `replace_missing_data_with_zeros` function."


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


def aggregate_rows_by_periods_extra(tb: Table) -> Table:
    """Aggregate rows by war, region, conflict type and period.

    Also, it makes sure that there is no missing value for the number of deaths. To this end:

        1. It summs all the deaths for each war conflict over the defined period of years (`battle_deaths` + `nonstate_deaths`).
        2. If an observation has a missing value, it assigns zero to it.
        3. If the summation is greater than the lower bound defined by the source (1,000 deaths per year) it is left as it is, otherwise
            it is replaced by the lower bound (e.g. 2,000 for 2 years, etc.)
    """
    # Get summation (assuming NaN=0) and flag stating if a NaN is found
    index = ["warnum", "conflict_type", "region", "year_start", "year_end"]
    tb_nan = tb.groupby(index, as_index=False).agg({"battle_deaths": has_nan, "nonstate_deaths": has_nan})
    tb_sum = tb.groupby(index, as_index=False).agg({"battle_deaths": sum, "nonstate_deaths": sum})
    tb = tb_nan.merge(tb_sum, on=index, suffixes=("_has_nan", "_sum"))

    # Obtain deaths threshold according to dataset description
    tb["deaths_threshold"] = 1000 * (tb["year_end"] - tb["year_start"] + 1)

    # Re-build `number_deaths_ongoing_conflicts`
    tb["number_deaths_ongoing_conflicts"] = tb["battle_deaths_sum"] + tb["nonstate_deaths_sum"]
    mask = tb["battle_deaths_has_nan"] + tb["nonstate_deaths_has_nan"] > 0
    tb.loc[mask, "number_deaths_ongoing_conflicts"] = tb.loc[
        mask, ["deaths_threshold", "number_deaths_ongoing_conflicts"]
    ].max(axis=1)

    return tb


########################################################################
## NON-STATE ###########################################################
########################################################################


def make_table_nonstate(tb: Table) -> Table:
    """Generate non-state table."""
    # Rename death-related metric columns
    tb = tb.rename(columns={"totalcombatdeaths": "number_deaths_ongoing_conflicts"})
    # Assign conflict_type
    tb["conflict_type"] = CTYPE_NONSTATE

    log.info("war.cow.non_state: sanity checks")
    _sanity_check_nonstate(tb)

    log.info("war.cow.non_state: replace negative values where applicable")
    tb[["number_deaths_ongoing_conflicts"]] = tb[["number_deaths_ongoing_conflicts"]].replace(-9, np.nan)

    log.info("war.cow.non_state: replace NaNs in number of deaths with lower bound (1,000)")
    tb["deaths_threshold"] = 1000 * (tb["year_end"] - tb["year_start"] + 1)
    tb["number_deaths_ongoing_conflicts"] = tb["number_deaths_ongoing_conflicts"].fillna(tb["deaths_threshold"])

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
    # Check latest end year
    assert (
        tb["year_end"].max() == END_YEAR_MAX_NONSTATE
    ), f"Non-state data is expected to have its latest end year by {END_YEAR_MAX_NONSTATE}, but that was not the case! Revisit this assertion and 'set NaNs' section in `replace_missing_data_with_zeros` function."
    # Check outcome
    assert (
        tb["outcome"] != 5
    ).all(), "Some conflicts are coded as if they were still on going in 2007! That shouldn't be the case! Check if maximum value for `year_end` has changed."


########################################################################
## INTER-STATE #########################################################
########################################################################


def make_table_inter(tb: Table) -> Table:
    """Generate inter-state table."""
    # Rename death-related metric columns
    tb = tb.rename(columns={"batdeath": "number_deaths_ongoing_conflicts"})
    # Assign conflict type
    tb["conflict_type"] = CTYPE_INTER

    log.info("war.cow.inter: sanity checks")
    _sanity_checks_inter(tb)

    log.info("war.cow.inter: replace negative values where applicable")
    tb[["number_deaths_ongoing_conflicts"]] = tb[["number_deaths_ongoing_conflicts"]].replace(-9, np.nan)
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
    assert not (tb["year_end"].isna().any()), "Unexpected NaN values for `year_end`!"
    assert not ((tb["year_end"] < 0).any()), "Unexpected negative values for `year_end`!"
    assert (
        tb["year_end"].max() == END_YEAR_MAX_INTER
    ), f"Inter-state data is expected to have its latest end year by {END_YEAR_MAX_INTER}, but that was not the case! Revisit this assertion and 'set NaNs' section in `replace_missing_data_with_zeros` function."
    # Check outcome
    assert (
        tb["outcome"] != 5
    ).all(), "Some conflicts are coded as if they were still on going in 2007! That shouldn't be the case! Check if maximum value for `year_end` has changed."


def aggregate_rows_by_periods_inter(tb: Table) -> Table:
    """Aggregate rows by war, region, conflict type and period.

    Also, it makes sure that there is no missing value for the number of deaths. To this end:

        1. It summs all the deaths for each war conflict over the defined period of years.
        2. If an observation has a missing value, it assigns zero to it.
        3. If the summation is greater than the lower bound defined by the source (1,000 deaths per year) it is left as it is, otherwise
            it is replaced by the lower bound (e.g. 2,000 for 2 years, etc.)
    """
    # Get summation (NaN if any NaN, and assuming NaN=0)
    index = ["warnum", "conflict_type", "region", "year_start", "year_end"]
    tb_nan = tb.groupby(index, as_index=False).agg({"number_deaths_ongoing_conflicts": has_nan})
    tb_sum = tb.groupby(index, as_index=False).agg({"number_deaths_ongoing_conflicts": sum})
    tb = tb_nan.merge(tb_sum, on=index, suffixes=("_has_nan", "_sum"))

    # Obtain deaths threshold according to dataset description
    tb["deaths_threshold"] = 1000 * (tb["year_end"] - tb["year_start"] + 1)

    # Re-build `number_deaths_ongoing_conflicts`
    tb["number_deaths_ongoing_conflicts"] = tb["number_deaths_ongoing_conflicts_sum"]
    mask = tb["number_deaths_ongoing_conflicts_has_nan"] == 1
    tb.loc[mask, "number_deaths_ongoing_conflicts"] = tb.loc[
        mask, ["number_deaths_ongoing_conflicts_sum", "deaths_threshold"]
    ].max(axis=1)

    return tb


def split_regions_composites(tb: Table) -> Table:
    """Split regions that are region composites.

    The inter-state COW war data dataset contains regions that are region composites, e.g. "Europe & Asia". This function
    deaggregates these region composites so that we only have individual regions (regions as defined by extra- and non-state datasets).
    We allocate the number of deaths evenly across the regions. That is, if a conflict in "Europe & Asia" has 4 deaths, we assign 2 deaths
    to Europe and Asia, respectively.
    """
    REGIONS_INTER_SPLIT = {
        # Europe & Middle East -> Europe, Middle East
        11: [2, 6],
        # Europe & Asia -> Europe, Asia
        12: [2, 7],
        # Americas & Asia -> Americas, Asia
        13: [1, 7],
        # Europe & Africa & Middle East -> Europe, Africa, Middle East
        14: [2, 4, 6],
        # Europe & Africa & Middle East & Asia -> Europe, Africa, Middle East, Asia
        15: [2, 4, 6, 7],
        # Africa, Middle East, Asia & Oceania -> Africa, Middle East, Asia, Oceania
        16: [4, 6, 7, 9],
        # Asia & Oceania -> Asia, Oceania  # IMPORTANT: this is actually no composite.
        # 17: [7],
        # Africa & Middle East -> Africa, Middle East
        18: [4, 6],
        # Europe, Africa, Middle East, Asia & Oceania -> Europe, Africa, Middle East, Asia, Oceania
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
    """Generate intra-state table."""
    # Rename death-related metric columns
    tb = tb.rename(columns={"totalbdeaths": "number_deaths_ongoing_conflicts"})

    log.info("war.cow.intra: sanity checks")
    _sanity_checks_intra(tb)

    log.info("war.cow.intra: replace negative values where applicable")
    tb = replace_negative_values_intra(tb)

    log.info("war.cow.non_state: replace NaNs in number of deaths with lower bound (1,000)")
    tb["deaths_threshold"] = 1000 * (tb["year_end"] - tb["year_start"] + 1)
    tb["number_deaths_ongoing_conflicts"] = tb["number_deaths_ongoing_conflicts"].fillna(tb["deaths_threshold"])

    log.info("war.cow.intra: standardise region numbering")
    tb = standardise_region_ids(tb)

    log.info("war.cow.intra: deaggregate international / non-international intrastate conflicts")
    assert (
        tb.groupby("warnum")["intnl"].nunique().max() == 1
    ), "An intra-state conflict is not expected to change between international / non-international!"
    mask = tb["intnl"] == 1
    tb.loc[mask, "conflict_type"] = CTYPE_INTRA_INTL
    tb.loc[-mask, "conflict_type"] = CTYPE_INTRA_NINTL

    return tb


def _sanity_checks_intra(tb: Table) -> None:
    assert tb.groupby(["warnum"]).size().max() == 1, "There should only be one instance for each (warnum, ccode, side)."
    # Check negative values in metric deaths
    col = "number_deaths_ongoing_conflicts"
    assert set(tb.loc[tb[col] < 0, col]) == {-9}, f"Negative values other than -9 found for {col}"
    # Check year end
    col = "year_end"
    assert not (tb[col].isna().any()), "Unexpected NaN value for `year_end`!"
    assert set(tb.loc[tb[col] < 0, col]) == {-7}, f"Negative values other than -7 found for {col}"
    assert (
        tb["year_end"].max() == END_YEAR_MAX_INTRA
    ), f"Intra-state data is expected to have its latest end year by {END_YEAR_MAX_INTRA}, but that was not the case! Revisit this assertion and 'set NaNs' section in `replace_missing_data_with_zeros` function."


def replace_negative_values_intra(tb: Table) -> Table:
    """Replace negative values for missing value where applicable.

    Some fields use negative values to encode missing values.
    """
    # Replace endyear1=-7 for 2007
    tb.loc[tb["year_end"] == -7, "year_end"] = 2014
    # Replace missing values for deaths
    tb[["number_deaths_ongoing_conflicts"]] = tb[["number_deaths_ongoing_conflicts"]].replace(-9, np.nan)

    return tb


def standardise_region_ids(tb: Table) -> Table:
    """Fix regions IDs.

    By default, regions in intra state conflicts table use a different numbering. This function standardises this.

    Also, this dataset uses composite regions, such as "Middle East and Northern Africa" or "Asia and Oceania". This function
    replaces rows with these composites with individual valid regions. That is, assigns "Africa" to a conflict labeled with region
    "Middle East and Northern Africa", if that's the actual region. This has been done manually, by carefully assigning regions to
    each conflict.
    """
    # Map to standard numbering (COW Intra-state uses different numbering)
    regions_mapping = {
        1: 1,  # Americas (NA)
        2: 1,  # Americas (SA)
        3: 2,  # Europe
        4: 4,  # Africa (SSA)
    }
    # Load custom mapping
    with open(PATH_CUSTOM_REGIONS_INTRASTATE, "r") as f:
        regions_mapping_default = json.load(f)
    regions_mapping_default = {float(k): v for k, v in regions_mapping_default.items()}
    # Apply custom & default mapping
    tb["region"] = tb["warnum"].map(regions_mapping_default).fillna(tb["region"].map(regions_mapping)).astype(int)

    return tb


########################################################################
## GENERIC #############################################################
########################################################################


def has_nan(x: pd.Series) -> bool:
    """Check if there is a NaN in a group."""
    return x.isna().any()


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
    tb = ds[table_name].reset_index()

    # Reset index
    tb = tb.reset_index()

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
    tb["warnum"] = tb["warnum"].astype(float).round(1)
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
    log.info("war.cow: estimate ongoing metrics (# ongoing conflicts, deaths in # ongoing conflicts)")
    tb_ongoing = _get_ongoing_metrics(tb)
    log.info("war.cow: estimate 'new' metrics (# new conflicts)")
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

    ## conflict_type='all'
    tb_ongoing_alltypes = tb.groupby(["year", "region"], as_index=False).agg(ops)
    tb_ongoing_alltypes["conflict_type"] = "all"

    ## World
    tb_ongoing_world = tb.groupby(["year", "conflict_type"], as_index=False).agg(ops)
    tb_ongoing_world["region"] = "World"

    ## conflict_type='all' and region='World'
    tb_ongoing_world_alltypes = tb.groupby(["year"], as_index=False).agg(ops)
    tb_ongoing_world_alltypes["region"] = "World"
    tb_ongoing_world_alltypes["conflict_type"] = "all"

    ## conflict_type='intra-state'
    tb_intra = tb[tb["conflict_type"].str.contains(CTYPE_INTRA)].copy()
    tb_ongoing_intra = tb_intra.groupby(["year", "region"], as_index=False).agg(ops)
    tb_ongoing_intra["conflict_type"] = CTYPE_INTRA

    ## conflict_type='intrastate' and region='World'
    tb_ongoing_world_intra = tb_intra.groupby(["year"], as_index=False).agg(ops)
    tb_ongoing_world_intra["region"] = "World"
    tb_ongoing_world_intra["conflict_type"] = CTYPE_INTRA

    ## Combine all
    tb_ongoing = pr.concat(
        [
            tb_ongoing,
            tb_ongoing_alltypes,
            tb_ongoing_world,
            tb_ongoing_world_alltypes,
            tb_ongoing_intra,
            tb_ongoing_world_intra,
        ],
        ignore_index=True,
    ).sort_values(  # type: ignore
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

    # By region and conflict_type='all'
    tb_new_alltypes = tb_.groupby(["year_start", "region"], as_index=False).agg(ops)
    tb_new_alltypes["conflict_type"] = "all"

    # By region and conflict_type='intra-state'
    tb_intra = tb[tb["conflict_type"].str.contains(CTYPE_INTRA)].copy()
    tb_new_intra = tb_intra.groupby(["year_start", "region"], as_index=False).agg(ops)
    tb_new_intra["conflict_type"] = CTYPE_INTRA

    # World
    ## Keep one row per (warnum).
    tb_ = tb.sort_values("year_start").drop_duplicates(subset=["warnum"], keep="first")
    ## Estimate metrics
    tb_new_world = tb.groupby(["year_start", "conflict_type"], as_index=False).agg(ops)
    tb_new_world["region"] = "World"

    # World and conflict_type='all'
    tb_new_world_alltypes = tb.groupby(["year_start"], as_index=False).agg(ops)
    tb_new_world_alltypes["region"] = "World"
    tb_new_world_alltypes["conflict_type"] = "all"

    # World and conflict_type='all'
    tb_new_world_intra = tb_intra.groupby(["year_start"], as_index=False).agg(ops)
    tb_new_world_intra["region"] = "World"
    tb_new_world_intra["conflict_type"] = CTYPE_INTRA

    ## Combine
    tb_new = pr.concat(
        [tb_new, tb_new_alltypes, tb_new_world, tb_new_world_alltypes, tb_new_intra, tb_new_world_intra],
        ignore_index=True,
    ).sort_values(  # type: ignore
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
        "number_deaths_ongoing_conflicts",
    ]
    tb.loc[:, columns] = tb.loc[:, columns].fillna(0)

    # Set NaNs
    tb.loc[(tb["year"] > END_YEAR_MAX) & (tb["conflict_type"] == "all"), columns] = np.nan
    tb.loc[(tb["year"] > END_YEAR_MAX_EXTRA) & (tb["conflict_type"] == CTYPE_EXTRA), columns] = np.nan
    tb.loc[(tb["year"] > END_YEAR_MAX_INTER) & (tb["conflict_type"] == CTYPE_INTER), columns] = np.nan
    tb.loc[
        (tb["year"] > END_YEAR_MAX_INTRA)
        & (
            tb["conflict_type"].isin(
                [
                    CTYPE_INTRA,
                    CTYPE_INTRA_INTL,
                    CTYPE_INTRA_NINTL,
                ]
            )
        ),
        columns,
    ] = np.nan
    tb.loc[(tb["year"] > END_YEAR_MAX_NONSTATE) & (tb["conflict_type"] == CTYPE_NONSTATE), columns] = np.nan

    # Drop all-NaN rows
    tb = tb.dropna(subset=columns, how="all")
    return tb


########################################################################
## COUNTRY-LEVEL########################################################
########################################################################
def _estimate_metrics_participants(tb: Table, tb_codes: Table, codes: List[str], conflict_type: str) -> Table:
    tb_country = pr.concat([tb[["year_start", "year_end", code]].rename(columns={code: "id"}).copy() for code in codes])

    # Remove NaNs
    tb_country["id"] = tb_country["id"].replace({-8: np.nan})
    tb_country = tb_country.dropna(subset=["id"])

    # Expand
    tb_country = expand_observations(
        tb_country,
        col_year_start="year_start",
        col_year_end="year_end",
    )

    # Keep relevant columns
    tb_country = tb_country[["year", "id"]]

    # Drop duplicates
    tb_country = tb_country.drop_duplicates()

    # Ensure numeric type
    tb_country["id"] = tb_country["id"].astype(int)

    # Sanity check
    assert not tb_country.isna().any(axis=None), "There are some NaNs!"

    # Add country name
    tb_country["country"] = tb_country.apply(lambda x: tb_codes.loc[(x["id"], x["year"])], axis=1)
    assert tb_country["country"].notna().all(), "Some countries were not found! NaN was set"

    # Add flag
    tb_country["participated_in_conflict"] = 1
    tb_country["participated_in_conflict"].m.origins = tb[codes[0]].m.origins

    # Prepare CoW table
    tb_codes["country"] = tb_codes["country"].astype(str)
    tb_codes = tb_codes.reset_index()

    # Combine all CoW entries with CoW
    columns_idx = ["year", "country", "id"]
    tb_country = tb_codes.merge(tb_country, on=columns_idx, how="outer")
    tb_country["participated_in_conflict"] = tb_country["participated_in_conflict"].fillna(0)
    tb_country = tb_country[columns_idx + ["participated_in_conflict"]]

    # Only preserve years that make sense
    tb_country = tb_country[
        (tb_country["year"] >= tb["year_start"].min()) & (tb_country["year"] <= tb["year_end"].max())
    ]

    # Add conflict type
    tb_country["conflict_type"] = conflict_type

    return tb_country


def estimate_metrics_participants(tb_extra: Table, tb_intra: Table, tb_inter: Table, tb_codes: Table) -> Table:
    """Add country-level indicators."""
    ###################
    # Participated in #
    ###################
    # FLAG YES/NO (country-level)

    # Get participations for each conflict type
    tb_extra_c = _estimate_metrics_participants(tb_extra, tb_codes, ["ccode1", "ccode2"], CTYPE_EXTRA)
    tb_inter_c = _estimate_metrics_participants(tb_inter, tb_codes, ["ccode"], CTYPE_INTER)
    tb_intra_i_c = _estimate_metrics_participants(
        tb_intra[tb_intra["intnl"] == 1], tb_codes, ["ccodea"], CTYPE_INTRA_INTL
    )
    tb_intra_ni_c = _estimate_metrics_participants(
        tb_intra[tb_intra["intnl"] != 1], tb_codes, ["ccodea"], CTYPE_INTRA_NINTL
    )

    tb_country = pr.concat(
        [
            tb_extra_c,
            tb_inter_c,
            tb_intra_i_c,
            tb_intra_ni_c,
        ],
        short_name="cow_country",
    )

    # Add intrastate (all)
    tb_country = aggregate_conflict_types(tb_country, CTYPE_INTRA, [CTYPE_INTRA_INTL, CTYPE_INTRA_NINTL])
    # Add state-based
    tb_country = aggregate_conflict_types(
        tb_country,
        "state-based",
        [CTYPE_EXTRA, CTYPE_INTER, CTYPE_INTRA],
    )

    # zero-fill
    # see https://github.com/owid/owid-issues/issues/1304
    ## Ddop column id
    tb_country = tb_country.drop(columns=["id"])
    ## Fill gaps with zeroes
    tb_country = fill_gaps_with_zeroes(
        tb_country,
        columns=["country", "year", "conflict_type"],
        cols_use_range=["year"],
    )

    ## Ensure correct year coverage
    ## see: https://github.com/owid/owid-issues/issues/1304#issuecomment-1853658729
    tb_country = tb_country[
        (tb_country["conflict_type"].isin([CTYPE_EXTRA, CTYPE_NONSTATE]) & (tb_country["year"] <= 2007))
        | ((tb_country["conflict_type"].isin([CTYPE_INTER, CTYPE_SBASED])) & (tb_country["year"] <= 2010))
        | (
            tb_country["conflict_type"].isin([CTYPE_INTRA, CTYPE_INTRA_INTL, CTYPE_INTRA_NINTL])
            & (tb_country["year"] <= 2014)
        )
    ]
    ## Add column id based on value from country
    dix_codes = tb_codes.reset_index().drop(columns="year").drop_duplicates()
    dix_codes = dix_codes.set_index("country", verify_integrity=True).squeeze().to_dict()
    tb_country["id"] = tb_country["country"].map(dix_codes)
    assert tb_country["id"].notna().all(), "NaN found! Couldn't match country to ID"

    ###################
    # Participated in #
    ###################
    # NUMBER COUNTRIES

    tb_num_participants = get_number_of_countries_in_conflict_by_region(tb_country, "conflict_type", "cow")

    # Filter known undesired datapoints
    tb_num_participants = tb_num_participants[
        (
            # Extra
            ((tb_num_participants["conflict_type"] == CTYPE_EXTRA) & (tb_num_participants["year"] <= 2007))
            |
            # Inter, State-based
            (
                (tb_num_participants["conflict_type"].isin([CTYPE_INTER, CTYPE_SBASED]))
                & (tb_num_participants["year"] <= 2010)
            )
            |
            # Intra
            (tb_num_participants["conflict_type"].isin([CTYPE_INTRA, CTYPE_INTRA_INTL, CTYPE_INTRA_NINTL]))
        )
    ]

    # Combine tables
    tb_country = pr.concat([tb_country, tb_num_participants], ignore_index=True)

    # Drop column `id`
    tb_country = tb_country.drop(columns=["id"])

    ###############
    # Final steps #
    ###############

    # Set short name
    tb_country.metadata.short_name = f"{paths.short_name}_country"
    # Set index
    tb_country = tb_country.set_index(["year", "country", "conflict_type"], verify_integrity=True)

    return tb_country


def estimate_metrics_locations(tb_chupilkin: Table, tb_system: Table, tb_participants: Table) -> Table:
    """Estimate locations metrics.

    tb_chupilkin: contains locations of inter-state conflicts.
    tb_system: contains 'lifetime' of states.
    tb_participants: contains countries that participated in a conflict. Useful for locations in intra-states, since location = participant in there.
    """
    tb_system = tb_system.rename(columns={"statenme": "country"})

    ########################################
    # 1) INTER-STATE ####
    # Get locations for inter-state wars: ccode, year, country, is_location_of_conflict
    # Sanity check
    assert (
        tb_chupilkin["warnum"] <= 227
    ).all(), "Unexpected value for `warnum`! All warnum values should be lower than 227, since Chupilkin only should contain inter-state conflicts."
    # Merge with COW SSM
    tb_locations_inter = tb_chupilkin[["country", "year", "is_location_of_conflict"]].drop_duplicates()
    tb_locations_inter = tb_system.merge(tb_locations_inter, on=["country", "year"], how="left")
    tb_locations_inter["is_location_of_conflict"] = tb_locations_inter["is_location_of_conflict"].fillna(0)
    # Filter irrelevant entries
    tb_locations_inter = tb_locations_inter[tb_locations_inter["year"] <= tb_chupilkin["year"].max()]
    # Reduce
    tb_locations_inter = tb_locations_inter.groupby(["year", "country"], as_index=False).agg(
        {"is_location_of_conflict": lambda x: min(1, x.sum())}
    )
    # Relevant columns
    tb_locations_inter = tb_locations_inter[["year", "country", "is_location_of_conflict"]]
    # Add conflict type
    tb_locations_inter["conflict_type"] = CTYPE_INTER

    ########################################
    # 2) INTRA-STATE ####
    # Get locations for intra-state wars: ccode, year, country, is_location_of_conflict
    # INTRA-STATE ####
    tb_system_ = tb_system.drop(columns=["stateabb", "version"])

    tb_locations_intra = tb_participants.reset_index().copy()
    tb_locations_intra = tb_locations_intra[
        tb_locations_intra["conflict_type"].isin([CTYPE_INTRA, CTYPE_INTRA_INTL, CTYPE_INTRA_NINTL])
    ]
    tb_locations_intra = (
        tb_locations_intra.rename(
            columns={
                "participated_in_conflict": "is_location_of_conflict",
            }
        )
        .drop(columns=["number_participants"])
        .dropna()
    )
    # Merge with COW SSM (for each conflict_type)
    tbs = []
    for ctype in [CTYPE_INTRA_INTL, CTYPE_INTRA_NINTL, CTYPE_INTRA]:
        tb_ = tb_locations_intra[tb_locations_intra["conflict_type"] == ctype].copy()
        year_max = tb_["year"].max()
        tb_ = tb_system_.merge(tb_, on=["country", "year"], how="left")
        tb_["conflict_type"] = ctype
        tb_ = tb_[tb_["year"] <= year_max]
        tbs.append(tb_)
    tb_locations_intra_countries = pr.concat(tbs, ignore_index=True)
    # Fill NaNs
    tb_locations_intra_countries["is_location_of_conflict"] = tb_locations_intra_countries[
        "is_location_of_conflict"
    ].fillna(0)

    # Replace Yugoslavia -> Serbia
    # tb_locations_intra_countries["country"] = tb_locations_intra_countries["country"].rename({"Yugoslavia": "Serbia"})

    # Drop ccode
    tb_locations_intra_countries = tb_locations_intra_countries.drop(columns=["ccode"])

    ########################################
    # 3) COMBINE INTRA + INTER ####

    # COMBINE INTER + INTRA
    tb_locations = pr.concat([tb_locations_inter, tb_locations_intra_countries], ignore_index=True)

    # Add state-based
    tb_locations = aggregate_conflict_types(
        tb=tb_locations,
        parent_name="state-based",
        children_names=[CTYPE_INTRA, CTYPE_INTER],
        columns_to_aggregate=["is_location_of_conflict"],
        columns_to_groupby=["country", "year"],
    )

    ###########################################
    # 4) ADD REGIONS AND WORLD DATA
    # Add region
    ## Quick fix (1/2): Serbia -> Yugoslavia
    tb_locations["country"] = tb_locations["country"].replace({"Serbia": "Yugoslavia"})
    ## Get country code
    tb_c2c = tb_system[["country", "ccode"]].drop_duplicates()
    tb_locations = tb_locations.merge(tb_c2c, how="left", on=["country"])
    assert tb_locations["ccode"].notna().all(), "Some countries were not found!"
    ## Add region name
    tb_locations = add_region_from_code(tb_locations, "cow", col_code="ccode")
    assert tb_locations.isna().sum().sum() == 0, "Some NaNs were found!"
    ## Quick fix (2/2): Yugoslavia -> Serbia
    tb_locations["country"] = tb_locations["country"].rename({"Yugoslavia": "Serbia"})

    # Get only entries with flag '1'
    tb_locations_active = tb_locations[tb_locations["is_location_of_conflict"] == 1].copy()

    # Get number of locations
    tb_locations_regions = tb_locations_active.groupby(
        ["year", "region", "conflict_type"], as_index=False, observed=True
    )["ccode"].nunique()
    tb_locations_world = tb_locations_active.groupby(["year", "conflict_type"], as_index=False, observed=True)[
        "ccode"
    ].nunique()
    tb_locations_world["country"] = "World"

    # Fix column names
    tb_locations_regions = tb_locations_regions.rename(
        columns={
            "ccode": "number_locations",
            "region": "country",
        }
    )
    tb_locations_world = tb_locations_world.rename(
        columns={
            "ccode": "number_locations",
        }
    )

    # Concat World with regions
    tb_locations_regions = pr.concat(
        [
            tb_locations_regions,
            tb_locations_world,
        ],
        ignore_index=True,
    )

    # Fill empty time periods with zero
    ## Hack to get all years since 1816
    assert tb_locations_regions["year"].min() == 1818
    tb_first = Table(
        {
            "year": [1816],
            "country": ["World"],
            "conflict_type": ["intra-state"],
            "number_locations": [0],
        }
    )
    tb_locations_regions = pr.concat([tb_first, tb_locations_regions], ignore_index=True)
    ## Actuall filling gaps
    tb_locations_regions = fill_gaps_with_zeroes(
        tb=tb_locations_regions,
        columns=["country", "year", "conflict_type"],
        cols_use_range=["year"],
    )

    ###########################################
    # 5) FINAL CONCAT, INDEX
    columns_idx = ["year", "country", "conflict_type"]

    # Drop ccode
    tb_locations = tb_locations.drop(columns=["ccode"])

    # Merge countries and regions
    tb_locations = pr.concat(
        [
            tb_locations[columns_idx + ["is_location_of_conflict"]],
            tb_locations_regions[columns_idx + ["number_locations"]],
        ],
        ignore_index=True,
        short_name="cow_locations",
    )

    # Correct year coverage by conflict type
    tb_locations = tb_locations[
        ((tb_locations["conflict_type"].isin([CTYPE_INTER, CTYPE_SBASED])) & (tb_locations["year"] <= 2010))
        | (
            tb_locations["conflict_type"].isin([CTYPE_INTRA, CTYPE_INTRA_INTL, CTYPE_INTRA_NINTL])
            & (tb_locations["year"] <= 2014)
        )
    ]

    # Fix metadata
    ## The metadata for `number_locations` should be the same as for `number_ongoing_conflicts`
    tb_locations["number_locations"].metadata = tb_locations["is_location_of_conflict"].metadata

    # Set index
    tb_locations = tb_locations.set_index(["year", "country", "conflict_type"], verify_integrity=True).sort_index()

    return tb_locations
