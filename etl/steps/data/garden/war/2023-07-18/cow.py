"""COW War data dataset.

- This dataset is built from four different files. Each of them come from the COW site, but have minor differences in
the fields they contain, etc.

- Each of the source files concerns a specific conflict type (i.e. one file only contains data on "inter-state" conflicts).
    - Therefore, a conflict (or at least how it is identified in this dataset) never changes its type.
    - There are fields explaining if a conflict transformed to another conflict (i.e. it stops being in the "inter-state" table with id X, and
    starts being in the "intra-state" table with id Y).
"""

import json
from typing import List, Set, Tuple, cast

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


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("cow"))

    # Read data (there are four different tables)
    log.info("war.cow: load data")
    tb_extra, tb_nonstate, tb_inter, tb_intra = load_tables(ds_meadow)

    # Check that there are no overlapping warnums between tables
    log.info("war.cow: check overlapping warnum in tables")
    _check_overlapping_warnum(tb_extra, tb_nonstate, tb_inter, tb_intra)

    #
    # Process data.
    #
    tb_extra = make_table_extra(tb_extra)
    tb_nonstate = make_table_nonstate(tb_nonstate)
    tb_inter = make_table_inter(tb_inter)
    tb_intra = make_table_intra(tb_intra)

    # Combine data
    tb = combine_tables(
        tb_extra=tb_extra,
        tb_nonstate=tb_nonstate,
        tb_inter=tb_inter,
        tb_intra=tb_intra,
    )

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

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
        table_name="extra_state",
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
        ds=ds,
        table_name="inter_state",
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
        table_name="intra_state",
        column_start_year="startyr1",
        column_end_year="endyr1",
        column_location="v5regionnum",
        columns_deaths=["totalbdeaths"],
        values_exp_wartype={4, 5, 6, 7},
    )

    return tb_extra, tb_nonstate, tb_inter, tb_intra


def combine_tables(tb_extra: Table, tb_nonstate: Table, tb_inter: Table, tb_intra: Table) -> Table:
    """Combine the tables from all four different conflict types.

    The original data comes in separate tables: extra-, non-, inter- and intra-state.
    """
    # Get relevant columns
    log.info("war.cow.extra: keep relevant columns")
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
    tb = pd.concat(
        [
            tb_extra,
            tb_nonstate,
            tb_inter,
            tb_intra,
        ],
        ignore_index=True,
    )

    # Add yearly observations (scale values)
    log.info("war.cow: expand observations")
    tb = expand_observations(tb, column_metrics=["number_deaths_ongoing_conflicts"])  # type: ignore

    # Estimate metrics (do the aggregations)
    log.info("war.cow: estimate metrics")
    tb = estimate_metrics(tb)

    # Rename regions
    log.info("war.cow: rename regions")
    tb["region"] = tb["region"].map(REGIONS_RENAME | {"World": "World"})
    assert tb["region"].isna().sum() == 0, "Unmapped regions!"
    # Add suffix with source name
    msk = tb["region"] != "World"
    tb.loc[msk, "region"] = tb.loc[msk, "region"] + " (COW)"

    # Sanity check on NaNs
    log.info("war.cow: check NaNs in `number_deaths_ongoing_conflicts`")
    assert (
        not tb["number_deaths_ongoing_conflicts"].isna().any()
    ), "Some NaNs found in `number_deaths_ongoing_conflicts`!"

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

    log.info("war.cow.extra: replace negative values where applicable")
    tb = replace_negative_values_extra(tb)

    log.info("war.cow.extra: obtain total number of deaths (assign lower bound value to missing values)")
    tb = aggregate_rows_by_periods_extra(tb)

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
    tb = tb.groupby(["warnum", "conflict_type", "region", "year_start", "year_end"], as_index=False).agg(
        {"battle_deaths": [has_nan, sum], "nonstate_deaths": [has_nan, sum]}
    )
    tb.columns = [f"{col1}_{col2}" if col2 != "" else col1 for col1, col2 in tb.columns]

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
    tb["conflict_type"] = "non-state"

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
    tb["conflict_type"] = "inter-state"

    log.info("war.cow.inter: sanity checks")
    _sanity_checks_inter(tb)

    log.info("war.cow.inter: replace negative values where applicable")
    tb[["number_deaths_ongoing_conflicts"]] = tb[["number_deaths_ongoing_conflicts"]].replace(-9, np.nan)

    log.info("war.cow.inter: assign lower bound of deaths where value is missing")
    tb = aggregate_rows_by_periods_inter(tb)

    log.info("war.cow.inter: split region composites")
    tb = split_regions_composites(tb)

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
    tb = tb.groupby(["warnum", "conflict_type", "region", "year_start", "year_end"], as_index=False).agg(
        {"number_deaths_ongoing_conflicts": [has_nan, sum]}
    )
    tb.columns = [f"{col1}_{col2}" if col2 != "" else col1 for col1, col2 in tb.columns]
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
    tb.loc[mask, "conflict_type"] = "intra-state (internationalized)"
    tb.loc[-mask, "conflict_type"] = "intra-state (non-internationalized)"

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
    "Middle East and Northern Afrrica", if that's the actual region. This has been done manually, bu carefully assigning regions to
    each conflict.
    """
    # Map to standard numbering
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
    tb["warnum"] = tb["warnum"].astype(float).round(1)

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
    tb_intra = tb[tb["conflict_type"].str.contains("intra-state")].copy()
    tb_ongoing_intra = tb_intra.groupby(["year", "region"], as_index=False).agg(ops)
    tb_ongoing_intra["conflict_type"] = "intra-state"

    ## conflict_type='intrastate' and region='World'
    tb_ongoing_world_intra = tb_intra.groupby(["year"], as_index=False).agg(ops)
    tb_ongoing_world_intra["region"] = "World"
    tb_ongoing_world_intra["conflict_type"] = "intra-state"

    ## Combine all
    tb_ongoing = pd.concat([tb_ongoing, tb_ongoing_alltypes, tb_ongoing_world, tb_ongoing_world_alltypes, tb_ongoing_intra, tb_ongoing_world_intra], ignore_index=True).sort_values(  # type: ignore
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
    tb_intra = tb[tb["conflict_type"].str.contains("intra-state")].copy()
    tb_new_intra = tb_intra.groupby(["year_start", "region"], as_index=False).agg(ops)
    tb_new_intra["conflict_type"] = "intra-state"

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
    tb_new_world_intra["conflict_type"] = "intra-state"

    ## Combine
    tb_new = pd.concat([tb_new, tb_new_alltypes, tb_new_world, tb_new_world_alltypes, tb_new_intra, tb_new_world_intra], ignore_index=True).sort_values(  # type: ignore
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
    tb.loc[(tb["year"] > END_YEAR_MAX_EXTRA) & (tb["conflict_type"] == "extra-state"), columns] = np.nan
    tb.loc[(tb["year"] > END_YEAR_MAX_INTER) & (tb["conflict_type"] == "inter-state"), columns] = np.nan
    tb.loc[
        (tb["year"] > END_YEAR_MAX_INTRA)
        & (
            tb["conflict_type"].isin(
                ["intra-state", "intra-state (internationalized)", "intra-state (non-internationalized)"]
            )
        ),
        columns,
    ] = np.nan
    tb.loc[(tb["year"] > END_YEAR_MAX_NONSTATE) & (tb["conflict_type"] == "non-state"), columns] = np.nan

    # Drop all-NaN rows
    tb = tb.dropna(subset=columns, how="all")
    return tb
