"""Data from UCDP.


Notes:
    - Conflict types for state-based violence is sourced from UCDP/PRIO dataset. non-state and one-sided violence is sourced from GED dataset.
    - There can be some mismatches with latest official reported data (UCDP's live dashboard). This is because UCDP uses latest data for their dashboard, which might not be available yet as bulk download.
    - Regions:
        - Uses `region` column for both GED and UCDP/PRIO datasets.
        - Incompatibilities in Oceania are encoded in "Asia". We therefore have changed the region name to "Asia and Oceania".
        - GED: Dataset uses names (not codes!)
            - You can learn more about the countries included in each region from section "Appendix 5 Main sources consulted during the 2022 update" in page 40,
            document: https://ucdp.uu.se/downloads/ged/ged231.pdf.
                - Note that countries from Oceania are included in Asia!
        - UCDP/PRIO: Dataset uses codes (note we changed "Asia" -> "Asia and Oceania")
            1 = Europe (GWNo: 200-399)
            2 = Middle East (GWNo: 630-699)
            3 = Asia (GWNo: 700-999)  [renamed to 'Asia and Oceania']
            4 = Africa (GWNo: 400-626)
            5 = Americas (GWNo: 2-199)
"""

from typing import List, Optional

import geopandas as gpd
import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog import processing as pr
from shapely import wkt
from shared import (
    add_indicators_extra,
    aggregate_conflict_types,
    fill_gaps_with_zeroes,
    get_number_of_countries_in_conflict_by_region,
)
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Mapping for Geo-referenced datase
TYPE_OF_VIOLENCE_MAPPING = {
    2: "non-state conflict",
    3: "one-sided violence",
}
# Mapping for armed conflicts dataset (inc PRIO/UCDP)
TYPE_OF_CONFLICT_MAPPING = {
    1: "extrasystemic",
    2: "interstate",
    3: "intrastate (non-internationalized)",
    4: "intrastate (internationalized)",
}
# Regions mapping (for PRIO/UCDP dataset)
REGIONS_MAPPING = {
    1: "Europe",
    2: "Middle East",
    3: "Asia and Oceania",
    4: "Africa",
    5: "Americas",
}
REGIONS_EXPECTED = set(REGIONS_MAPPING.values())


def run(dest_dir: str) -> None:
    paths.log.info("war_ucdp.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("ucdp")

    # Read table from GW codes
    ds_gw = paths.load_dataset("gleditsch")
    tb_regions = ds_gw["gleditsch_regions"].reset_index()
    tb_codes = ds_gw["gleditsch_countries"]

    # Load maps table
    short_name = "nat_earth_110"
    ds_maps = paths.load_dataset(short_name)
    tb_maps = ds_maps[short_name].reset_index()

    #
    # Process data.
    #
    paths.log.info("sanity checks")
    _sanity_checks(ds_meadow)

    # Load relevant tables
    tb_geo = ds_meadow["ucdp_geo"].reset_index()
    tb_conflict = ds_meadow["ucdp_battle_related_conflict"].reset_index()
    tb_prio = ds_meadow["ucdp_prio_armed_conflict"].reset_index()

    # Keep only active conflicts
    paths.log.info("keep active conflicts")
    tb_geo = tb_geo[tb_geo["active_year"] == 1]

    # Change region named "Asia" to "Asia and Oceania" (in GED)
    tb_geo["region"] = tb_geo["region"].replace(to_replace={"Asia": "Asia and Oceania"})

    # Create `conflict_type` column
    paths.log.info("add field `conflict_type`")
    tb = add_conflict_type(tb_geo, tb_conflict)

    # Get country-level stuff
    paths.log.info("getting country-level indicators")
    tb_participants = estimate_metrics_participants(tb, tb_codes)
    tb_locations = estimate_metrics_locations(tb, tb_maps)

    # Sanity check conflict_type transitions
    ## Only consider transitions between intrastate and intl intrastate. If other transitions are detected, raise error.
    _sanity_check_conflict_types(tb)
    _sanity_check_prio_conflict_types(tb_prio)

    # Add number of new conflicts and ongoing conflicts (also adds data for the World)
    paths.log.info("get metrics for main dataset (also estimate values for 'World')")
    tb = estimate_metrics(tb)

    # Add table from UCDP/PRIO
    paths.log.info("prepare data from ucdp/prio table (also estimate values for 'World')")
    tb_prio = prepare_prio_data(tb_prio)

    # Fill NaNs
    paths.log.info("replace missing data with zeros (where applicable)")
    tb_prio = replace_missing_data_with_zeros(tb_prio)
    tb = replace_missing_data_with_zeros(tb)

    # Combine main dataset with PRIO/UCDP
    paths.log.info("add data from ucdp/prio table")
    tb = combine_tables(tb, tb_prio)

    # Add extra-systemic after 1989
    paths.log.info("fix extra-systemic nulls")
    tb = fix_extrasystemic_entries(tb)

    # Add data for "all conflicts" conflict type
    paths.log.info("add data for 'all conflicts'")
    tb = add_conflict_all(tb)

    # Add data for "all intrastate" conflict types
    tb = add_conflict_all_intrastate(tb)

    # Add data for "state-based" conflict types
    tb = add_conflict_all_statebased(tb)

    # Force types
    # tb = tb.astype({"conflict_type": "category", "region": "category"})

    # Add conflict rates
    tb = add_indicators_extra(
        tb,
        tb_regions,
        columns_conflict_rate=["number_ongoing_conflicts", "number_new_conflicts"],
        columns_conflict_mortality=[
            "number_deaths_ongoing_conflicts",
            "number_deaths_ongoing_conflicts_high",
            "number_deaths_ongoing_conflicts_low",
        ],
    )

    # Adapt region names
    tb = adapt_region_names(tb)

    # Set index, sort rows
    tb = tb.set_index(["year", "region", "conflict_type"], verify_integrity=True).sort_index()
    tb_participants = tb_participants.set_index(
        ["year", "country", "conflict_type"], verify_integrity=True
    ).sort_index()
    tb_locations = tb_locations.set_index(["year", "country", "conflict_type"], verify_integrity=True).sort_index()

    # Add short_name to table
    paths.log.info("add shortname to table")
    tb.metadata.short_name = paths.short_name

    # Tables
    tables = [
        tb,
        tb_participants,
        tb_locations,
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

    paths.log.info("ucdp.end")


def _sanity_checks(ds: Dataset) -> None:
    """Check that the tables in the dataset are as expected."""
    tb_geo = ds["ucdp_geo"].reset_index()
    tb_conflict = ds["ucdp_battle_related_conflict"].reset_index()
    tb_nonstate = ds["ucdp_non_state"].reset_index()
    tb_onesided = ds["ucdp_one_sided"].reset_index()

    # Battle-related conflict #
    # Check IDs
    geo_ids = tb_geo.loc[tb_geo["type_of_violence"] == 1, ["conflict_new_id"]].drop_duplicates()
    conflict_ids = tb_conflict[["conflict_id"]].drop_duplicates()
    res = geo_ids.merge(conflict_ids, left_on="conflict_new_id", right_on="conflict_id", how="outer")
    assert res.isna().sum().sum() == 0, "Check NaNs in conflict_new_id or conflict_id"
    # Check number of deaths
    geo_deaths = (
        tb_geo.loc[(tb_geo["type_of_violence"] == 1) & (tb_geo["active_year"] == 1)]
        .groupby(["conflict_new_id", "year"], as_index=False)[["best"]]
        .sum()
        .sort_values(["conflict_new_id", "year"])
    )
    conflict_deaths = tb_conflict[["conflict_id", "year", "bd_best"]].sort_values(["conflict_id", "year"])
    res = geo_deaths.merge(
        conflict_deaths, left_on=["conflict_new_id", "year"], right_on=["conflict_id", "year"], how="outer"
    )
    assert res.isna().sum().sum() == 0, "Check NaNs in conflict_new_id or conflict_id"
    assert (
        len(res[res["best"] - res["bd_best"] != 0]) <= 1
    ), "Dicrepancy between number of deaths in conflict (Geo vs. Non-state datasets)"

    # Non-state #
    # Check IDs
    geo_ids = tb_geo.loc[tb_geo["type_of_violence"] == 2, ["conflict_new_id"]].drop_duplicates()
    nonstate_ids = tb_nonstate[["conflict_id"]].drop_duplicates()
    res = geo_ids.merge(nonstate_ids, left_on="conflict_new_id", right_on="conflict_id", how="outer")
    assert res.isna().sum().sum() == 0, "Check NaNs in conflict_new_id or conflict_id"
    # Check number of deaths
    geo_deaths = (
        tb_geo.loc[(tb_geo["type_of_violence"] == 2) & (tb_geo["active_year"] == 1)]
        .groupby(["conflict_new_id", "year"], as_index=False)[["best"]]
        .sum()
        .sort_values(["conflict_new_id", "year"])
    )
    nonstate_deaths = tb_nonstate[["conflict_id", "year", "best_fatality_estimate"]].sort_values(
        ["conflict_id", "year"]
    )
    res = geo_deaths.merge(
        nonstate_deaths, left_on=["conflict_new_id", "year"], right_on=["conflict_id", "year"], how="outer"
    )
    assert res.isna().sum().sum() == 0, "Check NaNs in conflict_new_id or conflict_id"
    assert (
        len(res[res["best"] - res["best_fatality_estimate"] != 0]) == 0
    ), "Dicrepancy between number of deaths in conflict (Geo vs. Non-state datasets)"

    # One-sided #
    # Check IDs
    geo_ids = tb_geo.loc[tb_geo["type_of_violence"] == 3, ["conflict_new_id"]].drop_duplicates()
    onesided_ids = tb_onesided[["conflict_id"]].drop_duplicates()
    res = geo_ids.merge(onesided_ids, left_on="conflict_new_id", right_on="conflict_id", how="outer")
    assert res.isna().sum().sum() == 0, "Check NaNs in conflict_new_id or conflict_id"
    # Check number of deaths
    geo_deaths = (
        tb_geo.loc[(tb_geo["type_of_violence"] == 3) & (tb_geo["active_year"] == 1)]
        .groupby(["conflict_new_id", "year"], as_index=False)[["best"]]
        .sum()
        .sort_values(["conflict_new_id", "year"])
    )
    onesided_deaths = tb_onesided[["conflict_id", "year", "best_fatality_estimate"]].sort_values(
        ["conflict_id", "year"]
    )
    res = geo_deaths.merge(
        onesided_deaths, left_on=["conflict_new_id", "year"], right_on=["conflict_id", "year"], how="outer"
    )
    assert res.isna().sum().sum() == 0, "Check NaNs in conflict_new_id or conflict_id"
    assert (
        len(res[res["best"] - res["best_fatality_estimate"] != 0]) <= 3
    ), "Dicrepancy between number of deaths in conflict (Geo vs. Non-state datasets)"


def add_conflict_type(tb_geo: Table, tb_conflict: Table) -> Table:
    """Add `conflict_type` to georeferenced dataset table.

    Values for conflict_type are:
       - non-state conflict
       - one-sided violence
       - extrasystemic
       - interstate
       - intrastate
       - internationalized intrastate

    The thing is that the original table `tb_geo` only contains a very high level categorisation. In particular,
    it labels all state-based conflicts as 'state-based'. Instead, we want to use a more fine grained definition:
    extrasystemic, intrastate, interstate.

    Parameters
    ----------
        tb_geo: Table
            This is the main table with the relevant data
        tb_conflict: Table
            This is a secondary table, that we use to obtain the conflict types of the conflicts.
    """
    tb_conflict = tb_conflict[["conflict_id", "year", "type_of_conflict"]].drop_duplicates()
    assert tb_conflict.groupby(["conflict_id", "year"]).size().max() == 1, "Some conflict_id-year pairs are duplicated!"

    # Add `type_of_conflict` to `tb_geo`.
    # This column contains the type of state-based conflict (1: inter-state, 2: intra-state, 3: extra-state, 4: internationalized intrastate)
    tb_geo = tb_geo.merge(
        tb_conflict, left_on=["conflict_new_id", "year"], right_on=["conflict_id", "year"], how="outer"
    )
    # Fill unknown types of violence
    mask = tb_geo["type_of_violence"] == 1  # these are state-based conflicts
    tb_geo.loc[mask, "type_of_conflict"] = tb_geo.loc[mask, "type_of_conflict"].fillna("state-based (unknown)")

    # Assert that `type_of_conflict` was only added for state-based events
    assert (
        tb_geo[tb_geo["type_of_violence"] != 1]["type_of_conflict"].isna().all()
    ), "There are some actual values for non-state based conflicts! These should only be NaN, since `tb_conflict` should only contain data for state-based conflicts."
    # Check that `type_of_conflict` is not NaN for state-based events
    assert (
        not tb_geo[tb_geo["type_of_violence"] == 1]["type_of_conflict"].isna().any()
    ), "Could not find the type of conflict for some state-based conflicts!"

    # Create `conflict_type` column as a combination of `type_of_violence` and `type_of_conflict`.
    tb_geo["conflict_type"] = (
        tb_geo["type_of_conflict"]
        .replace(TYPE_OF_CONFLICT_MAPPING)
        .fillna(tb_geo["type_of_violence"].replace(TYPE_OF_VIOLENCE_MAPPING))
    )

    # Sanity check
    assert tb_geo["conflict_type"].isna().sum() == 0, "Check NaNs in conflict_type (i.e. conflicts without a type)!"
    return tb_geo


def _sanity_check_conflict_types(tb: Table) -> Table:
    """Check conflict type.

    - Only transitions accepted are between intrastate conflicts.
    - The same conflict is only expceted to have one type in a year.
    """
    # Define expected combinations of conflicT_types for a conflict. Typically, only in the intrastate domain
    TRANSITION_EXPECTED = {"intrastate (internationalized)", "intrastate (non-internationalized)"}
    # Get conflicts with more than one conflict type assigned to them over their lifetime
    conflict_type_transitions = tb.groupby("conflict_new_id")["conflict_type"].apply(set)
    transitions = conflict_type_transitions[conflict_type_transitions.apply(len) > 1].drop_duplicates()
    # Extract unique combinations of conflict_types for a conflict
    assert (len(transitions) == 1) & (transitions.iloc[0] == TRANSITION_EXPECTED), "Error"

    # Check if different regions categorise the conflict differently in the same year
    assert not (
        tb.groupby(["conflict_id", "year"])["type_of_conflict"].nunique() > 1
    ).any(), "Seems like the conflict has multiple types for a single year! Is it categorised differently depending on the region? This case has not been taken into account -- please review the code!"


def _sanity_check_prio_conflict_types(tb: Table) -> Table:
    """Check conflict type in UCDP/PRIO data.

    - Only transitions accepted between intrastate conflicts.
    - The same conflict is only expceted to have one type in a year.
    """
    # Define expected combinations of conflict_types for a conflict. Typically, only in the intrastate domain
    TRANSITIONS_EXPECTED = {"{3, 4}"}
    # Get conflicts with more than one conflict type assigned to them over their lifetime
    conflict_type_transitions = tb.groupby("conflict_id")["type_of_conflict"].apply(set)
    transitions = conflict_type_transitions[conflict_type_transitions.apply(len) > 1].drop_duplicates()
    # Extract unique combinations of conflict_types for a conflict
    transitions = set(transitions.astype(str))
    transitions_unk = transitions - TRANSITIONS_EXPECTED

    # Check if different regions categorise the conflict differently in the same year
    assert not (
        tb.groupby(["conflict_id", "year"])["type_of_conflict"].nunique() > 1
    ).any(), "Seems like the conflict hast multiple types for a single year! Is it categorised differently depending on the region?"

    assert not transitions_unk, f"Unknown transitions found: {transitions_unk}"


def replace_missing_data_with_zeros(tb: Table, columns: Optional[List[str]] = None) -> Table:
    """Replace missing data with zeros.

    In some instances (e.g. extrasystemic conflicts after ~1964) there is missing data. Instead, we'd like this to be zero-valued.
    """
    # Add missing (year, region, conflict_typ) entries (filled with NaNs)
    years = np.arange(tb["year"].min(), tb["year"].max() + 1)
    regions = set(tb["region"])
    conflict_types = set(tb["conflict_type"])
    new_idx = pd.MultiIndex.from_product([years, regions, conflict_types], names=["year", "region", "conflict_type"])
    tb = tb.set_index(["year", "region", "conflict_type"]).reindex(new_idx).reset_index()

    # ADD HERE IF YOU WANT TO REPLACE MISSING DATA WITH ZEROS
    ## Change NaNs for 0 for specific rows
    ## For columns "number_ongoing_conflicts", "number_new_conflicts", and "number_deaths_ongoing_conflict*"
    if columns is None:
        columns = [
            "number_ongoing_conflicts",
            "number_new_conflicts",
            "number_deaths_ongoing_conflicts",
            "number_deaths_ongoing_conflicts_low",
            "number_deaths_ongoing_conflicts_high",
        ]
    for col in columns:
        if col in tb.columns:
            tb.loc[:, col] = tb.loc[:, col].fillna(0)

    return tb


def estimate_metrics(tb: Table) -> Table:
    """Add number of ongoing and new conflicts, and number of deaths.

    It also estimates the values for 'World', otherwise this can't be estimated later on.
    This is because some conflicts occur in multiple regions, and hence would be double counted. To overcome this,
    we need to access the actual conflict_id field to find the number of unique values. This can only be done here.
    """
    # Get number of ongoing conflicts, and deaths in ongoing conflicts
    paths.log.info("get number of ongoing conflicts and deaths in ongoing conflicts")
    tb_ongoing = _get_ongoing_metrics(tb)

    # Get number of new conflicts every year
    paths.log.info("get number of new conflicts every year")
    tb_new = _get_new_metrics(tb)
    # Combine and build single table
    paths.log.info("combine and build single table")
    tb = tb_ongoing.merge(
        tb_new,
        left_on=["year", "region", "conflict_type"],
        right_on=["year", "region", "conflict_type"],
        how="outer",  # data for (1991, intrastate) is available for 'ongoing conflicts' but not for 'new conflicts'. We don't want to loose it!
    )

    # If datapoint is missing, fill with zero
    tb = tb.fillna(0)

    # tb = tb.drop(columns=["year_start"])
    return tb


def _get_ongoing_metrics(tb: Table) -> Table:
    # For each region
    columns_idx = ["year", "region", "conflict_type"]
    tb_ongoing = (
        tb.groupby(columns_idx)
        .agg({"best": "sum", "high": "sum", "low": "sum", "conflict_new_id": "nunique"})
        .reset_index()
    )
    tb_ongoing.columns = columns_idx + [
        "number_deaths_ongoing_conflicts",
        "number_deaths_ongoing_conflicts_high",
        "number_deaths_ongoing_conflicts_low",
        "number_ongoing_conflicts",
    ]
    # For the World
    columns_idx = ["year", "conflict_type"]
    tb_ongoing_world = (
        tb.groupby(columns_idx)
        .agg({"best": "sum", "high": "sum", "low": "sum", "conflict_new_id": "nunique"})
        .reset_index()
    )
    tb_ongoing_world.columns = columns_idx + [
        "number_deaths_ongoing_conflicts",
        "number_deaths_ongoing_conflicts_high",
        "number_deaths_ongoing_conflicts_low",
        "number_ongoing_conflicts",
    ]
    tb_ongoing_world["region"] = "World"

    # Combine
    tb_ongoing = pr.concat([tb_ongoing, tb_ongoing_world], ignore_index=True).sort_values(  # type: ignore
        by=["year", "region", "conflict_type"]
    )
    return tb_ongoing


def _get_new_metrics(tb: Table) -> Table:
    # Reduce table to only preserve first appearing event
    tb = (
        tb[["conflict_new_id", "year", "region", "conflict_type"]]
        .sort_values("year")
        .drop_duplicates(subset=["conflict_new_id", "region"], keep="first")
    )

    # For each region
    columns_idx = ["year", "region", "conflict_type"]
    tb_new = tb.groupby(columns_idx)[["conflict_new_id"]].nunique().reset_index()
    tb_new.columns = columns_idx + ["number_new_conflicts"]

    # For the World
    ## Consider first start globally (a conflict may have started in region A in year X and in region B later in year X + 1)
    tb = tb.sort_values("year").drop_duplicates(subset=["conflict_new_id"], keep="first")
    columns_idx = ["year", "conflict_type"]
    tb_new_world = tb.groupby(columns_idx)[["conflict_new_id"]].nunique().reset_index()
    tb_new_world.columns = columns_idx + ["number_new_conflicts"]
    tb_new_world["region"] = "World"

    # Combine
    tb_new = pr.concat([tb_new, tb_new_world], ignore_index=True).sort_values(  # type: ignore
        by=["year", "region", "conflict_type"]
    )

    return tb_new


def prepare_prio_data(tb_prio: Table) -> Table:
    """Prepare PRIO table.

    This includes estimating all necessary metrics (ongoing and new).
    """
    tb_prio = _prepare_prio_table(tb_prio)
    tb_prio = _prio_add_metrics(tb_prio)
    return tb_prio


def combine_tables(tb: Table, tb_prio: Table) -> Table:
    """Combine main table with data from UCDP/PRIO.

    UCDP/PRIO table provides estimates for dates earlier then 1989.

    It only includes state-based conflicts!
    """
    # Ensure year period for each table is as expected
    assert tb["year"].min() == 1989, "Unexpected start year!"
    assert tb["year"].max() == 2022, "Unexpected start year!"
    assert tb_prio["year"].min() == 1946, "Unexpected start year!"
    assert tb_prio["year"].max() == 1989, "Unexpected start year!"

    # Force NaN in 1989 data from Geo-referenced dataset for `number_new_conflicts`
    # We want this data to come from PRIO/UCDP instead!
    tb.loc[tb["year"] == 1989, "number_new_conflicts"] = np.nan
    # Force NaN in 1989 data from PRIO/UCDP dataset for `number_ongoing_conflicts`
    # We want this data to come from GEO instead!
    tb_prio.loc[tb_prio["year"] == 1989, "number_ongoing_conflicts"] = np.nan

    # Merge Geo with UCDP/PRIO
    tb = tb_prio.merge(tb, on=["year", "region", "conflict_type"], suffixes=("_prio", "_main"), how="outer")

    # Sanity checks
    ## Data from PRIO/UCDP for `number_ongoing_conflicts` goes from 1946 to 1988 (inc)
    assert tb[tb["number_ongoing_conflicts_prio"].notna()]["year"].min() == 1946
    assert tb[tb["number_ongoing_conflicts_prio"].notna()]["year"].max() == 1988
    ## Data from GEO for `number_ongoing_conflicts` goes from 1989 to 2022 (inc)
    assert tb[tb["number_ongoing_conflicts_main"].notna()].year.min() == 1989
    assert tb[tb["number_ongoing_conflicts_main"].notna()]["year"].max() == 2022
    ## Data from PRIO/UCDP for `number_new_conflicts` goes from 1946 to 1989 (inc)
    assert tb[tb["number_new_conflicts_prio"].notna()]["year"].min() == 1946
    assert tb[tb["number_new_conflicts_prio"].notna()]["year"].max() == 1989
    ## Data from GEO for `number_new_conflicts` goes from 1990 to 2022 (inc)
    assert tb[tb["number_new_conflicts_main"].notna()]["year"].min() == 1990
    assert tb[tb["number_new_conflicts_main"].notna()]["year"].max() == 2022

    # Actually combine timeseries from UCDP/PRIO and GEO.
    # We prioritise values from PRIO for 1989, therefore the order `PRIO.fillna(MAIN)`
    tb["number_ongoing_conflicts"] = tb["number_ongoing_conflicts_prio"].fillna(tb["number_ongoing_conflicts_main"])
    tb["number_new_conflicts"] = tb["number_new_conflicts_prio"].fillna(tb["number_new_conflicts_main"])

    # Remove unnecessary columns
    columns_remove = tb.filter(regex=r"(_prio|_main)").columns
    tb = tb[[col for col in tb.columns if col not in columns_remove]]

    return tb


def fix_extrasystemic_entries(tb: Table) -> Table:
    """Fix entries with conflict_type='extrasystemic.

    Basically means setting to zero null entries after 1989.
    """
    # Sanity check
    assert (
        tb.loc[tb["conflict_type"] == "extrasystemic", "year"].max() == 1989
    ), "There are years beyond 1989 for extrasystemic conflicts by default!"

    # Get only extra-systemic stuff
    mask = tb.conflict_type == "extrasystemic"
    tb_extra = tb.loc[mask].copy()

    # add all combinations
    years = np.arange(tb["year"].min(), tb["year"].max() + 1)
    regions = set(tb["region"])
    new_idx = pd.MultiIndex.from_product([years, regions], names=["year", "region"])
    tb_extra = tb_extra.set_index(["year", "region"]).reindex(new_idx).reset_index()
    tb_extra["conflict_type"] = "extrasystemic"

    # Replace nulls with zeroes (all time series)
    columns = [
        "number_ongoing_conflicts",
        "number_new_conflicts",
    ]
    tb_extra[columns] = tb_extra[columns].fillna(0)

    # Replace nulls with zeroes (only post 1989 time series)
    columns = [
        "number_deaths_ongoing_conflicts",
        "number_deaths_ongoing_conflicts_high",
        "number_deaths_ongoing_conflicts_low",
    ]
    mask_1989 = tb_extra["year"] >= 1989
    tb_extra.loc[mask_1989, columns] = tb_extra.loc[mask_1989, columns].fillna(0)

    # Add to main table
    tb = pr.concat([tb[-mask], tb_extra])
    return tb


def _prepare_prio_table(tb: Table) -> Table:
    # Select relevant columns
    tb = tb[["conflict_id", "year", "region", "type_of_conflict", "start_date"]]

    # Flatten (some entries have multiple regions, e.g. `1, 2`). This should be flattened to multiple rows.
    # https://stackoverflow.com/a/42168328/5056599
    tb["region"] = tb["region"].str.split(", ")
    cols = tb.columns[tb.columns != "region"].tolist()
    tb = tb[cols].join(tb["region"].apply(pd.Series))
    tb = tb.set_index(cols).stack().reset_index()
    tb = tb.drop(tb.columns[-2], axis=1).rename(columns={0: "region"})
    tb["region"] = tb["region"].astype(int)

    # Obtain start year of the conflict
    tb["year_start"] = pd.to_datetime(tb["start_date"]).dt.year

    # Rename regions
    tb["region"] = tb["region"].map(REGIONS_MAPPING)

    # Create conflict_type
    tb["conflict_type"] = tb["type_of_conflict"].map(TYPE_OF_CONFLICT_MAPPING)

    # Checks
    assert tb["conflict_type"].isna().sum() == 0, "Some unknown conflict type ids were found!"
    assert tb["region"].isna().sum() == 0, "Some unknown region ids were found!"

    # Filter only data from the first year with ongoing conflicts
    tb = tb[tb["year_start"] >= tb["year"].min()]

    return tb


def _prio_add_metrics(tb: Table) -> Table:
    """Things to consider:

    Values for the `number_new_conflicts` in 1989 for conflict types 'one-sided' and 'non-state' (i.e. other than 'state-based')
    are not accurate.
    This is because the Geo-referenced dataset starts in 1989, and this leads somehow to an overestimate of the number of conflicts
    that started this year. We can solve this for 'state-based' conflicts, for which we can get data earlier than 1989 from
    the UCDP/PRIO Armed Conflicts dataset.
    """
    # Get number of ongoing conflicts for all regions
    cols_idx = ["year", "region", "conflict_type"]
    tb_ongoing = tb.groupby(cols_idx, as_index=False)["conflict_id"].nunique()
    tb_ongoing.columns = cols_idx + ["number_ongoing_conflicts"]
    # Get number of ongoing conflicts for 'World'
    cols_idx = ["year", "conflict_type"]
    tb_ongoing_world = tb.groupby(cols_idx, as_index=False)["conflict_id"].nunique()
    tb_ongoing_world.columns = cols_idx + ["number_ongoing_conflicts"]
    tb_ongoing_world["region"] = "World"
    # Combine regions & world
    tb_ongoing = pr.concat([tb_ongoing, tb_ongoing_world], ignore_index=True)
    # Keep only until 1989
    tb_ongoing = tb_ongoing[tb_ongoing["year"] < 1989]

    # Get number of new conflicts for all regions
    ## Reduce table to only preserve first appearing event
    tb = tb.sort_values("year").drop_duplicates(subset=["conflict_id", "year_start", "region"], keep="first")
    # Groupby operation
    cols_idx = ["year_start", "region", "conflict_type"]
    tb_new = tb.groupby(cols_idx, as_index=False)["conflict_id"].nunique()
    tb_new.columns = cols_idx + ["number_new_conflicts"]
    # Get number of new conflicts for 'World'
    tb = tb.sort_values("year").drop_duplicates(subset=["conflict_id", "year_start"], keep="first")
    cols_idx = ["year_start", "conflict_type"]
    tb_new_world = tb.groupby(cols_idx, as_index=False)["conflict_id"].nunique()
    tb_new_world.columns = cols_idx + ["number_new_conflicts"]
    tb_new_world["region"] = "World"
    # Combine regions & world
    tb_new = pr.concat([tb_new, tb_new_world], ignore_index=True)
    # Keep only until 1989 (inc)
    tb_new = tb_new[tb_new["year_start"] <= 1989]
    # Rename column
    tb_new = tb_new.rename(columns={"year_start": "year"})

    # Combine and build single table
    tb = tb_ongoing.merge(
        tb_new, left_on=["year", "region", "conflict_type"], right_on=["year", "region", "conflict_type"], how="outer"
    )

    # Dtypes
    tb = tb.astype({"year": "uint64", "region": "category"})

    return tb


def add_conflict_all(tb: Table) -> Table:
    """Add metrics for conflict_type = 'all'.

    Note that this should only be added for years after 1989, since prior to that year we are missing data on 'one-sided' and 'non-state'.
    """
    # Estimate number of all conflicts
    tb_all = tb.groupby(["year", "region"], as_index=False)[
        [
            "number_deaths_ongoing_conflicts",
            "number_deaths_ongoing_conflicts_high",
            "number_deaths_ongoing_conflicts_low",
            "number_ongoing_conflicts",
            "number_new_conflicts",
        ]
    ].sum()
    tb_all["conflict_type"] = "all"

    # Only append values after 1989 (before that we don't have 'one-sided' or 'non-state' counts)
    tb_all = tb_all[tb_all["year"] >= 1989]
    tb = pr.concat([tb, tb_all], ignore_index=True)

    # Set `number_new_conflicts` to NaN for 1989
    tb.loc[(tb["year"] == 1989) & (tb["conflict_type"] == "all"), "number_new_conflicts"] = np.nan

    return tb


def add_conflict_all_intrastate(tb: Table) -> Table:
    """Add metrics for conflict_type = 'intrastate'."""
    tb_intra = tb[
        tb["conflict_type"].isin(["intrastate (non-internationalized)", "intrastate (internationalized)"])
    ].copy()
    tb_intra = tb_intra.groupby(["year", "region"], as_index=False).sum(numeric_only=True, min_count=1)
    tb_intra["conflict_type"] = "intrastate"
    tb = pr.concat([tb, tb_intra], ignore_index=True)
    return tb


def add_conflict_all_statebased(tb: Table) -> Table:
    """Add metrics for conflict_type = 'state-based'."""
    tb_state = tb[tb["conflict_type"].isin(TYPE_OF_CONFLICT_MAPPING.values())].copy()
    tb_state = tb_state.groupby(["year", "region"], as_index=False).sum(numeric_only=True, min_count=1)
    tb_state["conflict_type"] = "state-based"
    tb = pr.concat([tb, tb_state], ignore_index=True)
    return tb


def adapt_region_names(tb: Table) -> Table:
    assert not tb["region"].isna().any(), "There were some NaN values found for field `region`. This is not expected!"
    # Get regions in table
    regions = set(tb["region"])
    # Check they are as expected
    regions_unknown = regions - (REGIONS_EXPECTED | {"World"})
    assert not regions_unknown, f"Unexpected regions: {regions_unknown}, please review!"

    # Add suffix with source name
    msk = tb["region"] != "World"
    tb.loc[msk, "region"] = tb.loc[msk, "region"] + " (UCDP)"
    return tb


def estimate_metrics_participants(tb: Table, tb_codes: Table) -> Table:
    """Add participant information at country-level."""
    ###################
    # Participated in #
    ###################
    # FLAG YES/NO (country-level)

    # Get table with [year, conflict_type, code]
    codes = ["gwnoa", "gwnob"]
    tb_country = pr.concat([tb[["year", "conflict_type", code]].rename(columns={code: "id"}).copy() for code in codes])

    # Drop rows with code = NaN
    tb_country = tb_country.dropna(subset=["id"])
    # Drop duplicates
    tb_country = tb_country.drop_duplicates()

    # Explode where multiple codes
    tb_country["id"] = tb_country["id"].astype(str).str.split(";")
    tb_country = tb_country.explode("id")
    # Drop duplicates (may appear duplicates after exploding)
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
    tb_country["participated_in_conflict"].m.origins = tb["gwnoa"].m.origins

    # Prepare GW table
    tb_alltypes = Table(pd.DataFrame({"conflict_type": tb_country["conflict_type"].unique()}))
    tb_codes = tb_codes.reset_index().merge(tb_alltypes, how="cross")
    tb_codes["country"] = tb_codes["country"].astype(str)

    # Combine all GW entries with UCDP
    columns_idx = ["year", "country", "id", "conflict_type"]
    tb_country = tb_codes.merge(tb_country, on=columns_idx, how="outer")
    tb_country["participated_in_conflict"] = tb_country["participated_in_conflict"].fillna(0)
    tb_country = tb_country[columns_idx + ["participated_in_conflict"]]

    # Add intrastate (all)
    tb_country = aggregate_conflict_types(
        tb_country, "intrastate", ["intrastate (non-internationalized)", "intrastate (internationalized)"]
    )
    # Add state-based
    tb_country = aggregate_conflict_types(tb_country, "state-based", list(TYPE_OF_CONFLICT_MAPPING.values()))

    # Only preserve years that make sense
    tb_country = tb_country[(tb_country["year"] >= tb["year"].min()) & (tb_country["year"] <= tb["year"].max())]

    ###################
    # Participated in #
    ###################
    # NUMBER COUNTRIES

    tb_num_participants = get_number_of_countries_in_conflict_by_region(tb_country, "conflict_type", "gw")

    # Combine tables
    tb_country = pr.concat([tb_country, tb_num_participants], ignore_index=True)

    # Drop column `id`
    tb_country = tb_country.drop(columns=["id"])

    ###############
    # Final steps #
    ###############

    # Set short name
    tb_country.metadata.short_name = f"{paths.short_name}_country"

    return tb_country


def estimate_metrics_locations(tb: Table, tb_maps: Table) -> Table:
    """Add participant information at country-level.

    reference: https://github.com/owid/notebooks/blob/main/JoeHasell/UCDP%20and%20PRIO/UCDP_georeferenced/ucdp_country_extract.ipynb
    """
    # Add country name using geometry
    paths.log.info("adding location name of conflict event...")
    tb_locations = _get_location_of_conflict_in_ucdp_ged(tb, tb_maps).copy()

    # There are some countries not in GW (remove, replace?). We keep Palestine and Western Sahara since
    # these are mappable in OWID maps.
    # We map entry with id "53238" and relid "PAK-2003-1-345-88" from "Siachen Glacier" to "Pakistan" based on
    # the text in `where_description` field, which says: "Giang sector in Siachen, Pakistani Kashmir"
    tb_locations.loc[tb_locations["country_name_location"] == "Siachen Glacier", "country_name_location"] = "Pakistan"

    ###################
    # COUNTRY-LEVEL: Country in conflict or not (1 or 0)
    ###################
    paths.log.info("estimating country flag 'is_location_of_conflict'...")

    # tb_locations = tb_locations.merge(
    #     tb_codes.reset_index(), left_on="country_name_location", right_on="country", how="left"
    # )

    # Estimate number of conflicts ocurring in each country
    tb_locations_country = (
        tb_locations.groupby(["country_name_location", "year", "conflict_type"], as_index=False)["conflict_new_id"]
        .nunique()
        .rename(
            columns={
                "conflict_new_id": "is_location_of_conflict",
                "country_name_location": "country",
            }
        )
    )
    assert tb_locations_country["is_location_of_conflict"].notna().all(), "Missing values in `is_location_of_conflict`!"

    # Convert into a binary indicator: 1 (if more than one conflict), 0 (otherwise)
    tb_locations_country["is_location_of_conflict"] = tb_locations_country["is_location_of_conflict"].apply(
        lambda x: 1 if x > 0 else 0
    )

    # Fill with zeroes
    tb_locations_country = fill_gaps_with_zeroes(
        tb=tb_locations_country,
        columns=["country", "year", "conflict_type"],
        cols_use_range=["year"],
    )

    # Add origins from Natural Earth
    tb_locations_country["is_location_of_conflict"].origins += tb_maps["name"].m.origins

    ###################
    # Add conflict type aggregates
    ###################
    paths.log.info("adding conflict type aggregates...")

    # Add missing conflict types
    # Add intrastate (all)
    tb_locations_country = aggregate_conflict_types(
        tb=tb_locations_country,
        parent_name="intrastate",
        children_names=["intrastate (non-internationalized)", "intrastate (internationalized)"],
        columns_to_aggregate=["is_location_of_conflict"],
        columns_to_groupby=["country", "year"],
    )
    # Add state-based
    state_based_conflicts = list(TYPE_OF_CONFLICT_MAPPING.values())
    tb_locations_country = aggregate_conflict_types(
        tb=tb_locations_country,
        parent_name="state-based",
        children_names=list(state_based_conflicts),
        columns_to_aggregate=["is_location_of_conflict"],
        columns_to_groupby=["country", "year"],
    )
    # Add all
    non_state_conflicts = list(TYPE_OF_VIOLENCE_MAPPING.values())
    tb_locations_country = aggregate_conflict_types(
        tb=tb_locations_country,
        parent_name="all",
        children_names=list(state_based_conflicts + non_state_conflicts),
        columns_to_aggregate=["is_location_of_conflict"],
        columns_to_groupby=["country", "year"],
    )

    ###################
    # REGION-LEVEL: Number of locations with conflict
    ###################
    paths.log.info("estimating number of locations with conflict...")

    # Add regions
    cols = ["region", "year", "conflict_type"]
    tb_locations_regions = (
        tb_locations.groupby(cols)["country_name_location"]
        .nunique()
        .reset_index()
        .sort_values(cols)
        .rename(
            columns={
                "country_name_location": "number_locations",
                "region": "country",
            }
        )
    )
    # World
    cols = ["year", "conflict_type"]
    tb_locations_world = (
        tb_locations.groupby(cols)["country_name_location"]
        .nunique()
        .reset_index()
        .sort_values(cols)
        .rename(
            columns={
                "country_name_location": "number_locations",
                "region": "country",
            }
        )
    )
    tb_locations_world["country"] = "World"
    # Combine
    tb_locations_regions = pr.concat([tb_locations_regions, tb_locations_world], ignore_index=True)

    # Add origins
    tb_locations_regions["number_locations"].m.origins = tb_locations["conflict_new_id"].origins

    paths.log.info("adding conflict type aggregates...")
    # Add aggregates of conflict types
    tb_locations_regions = aggregate_conflict_types(
        tb=tb_locations_regions,
        parent_name="intrastate",
        children_names=["intrastate (non-internationalized)", "intrastate (internationalized)"],
        columns_to_aggregate=["number_locations"],
        columns_to_aggregate_absolute=["number_locations"],
        columns_to_groupby=["country", "year"],
    )
    # Add state-based
    state_based_conflicts = list(TYPE_OF_CONFLICT_MAPPING.values())
    tb_locations_regions = aggregate_conflict_types(
        tb=tb_locations_regions,
        parent_name="state-based",
        children_names=list(state_based_conflicts),
        columns_to_aggregate=["number_locations"],
        columns_to_aggregate_absolute=["number_locations"],
        columns_to_groupby=["country", "year"],
    )
    # Add all
    non_state_conflicts = list(TYPE_OF_VIOLENCE_MAPPING.values())
    tb_locations_regions = aggregate_conflict_types(
        tb=tb_locations_regions,
        parent_name="all",
        children_names=list(state_based_conflicts + non_state_conflicts),
        columns_to_aggregate=["number_locations"],
        columns_to_aggregate_absolute=["number_locations"],
        columns_to_groupby=["country", "year"],
    )

    ###################
    # COMBINE: Country flag + Regional counts
    ###################
    paths.log.info("combining country flag and regional counts...")
    tb_locations = pr.concat(
        [tb_locations_country, tb_locations_regions], short_name=f"{paths.short_name}_locations", ignore_index=True
    )
    return tb_locations


def _get_location_of_conflict_in_ucdp_ged(tb: Table, tb_maps: Table) -> Table:
    """Add column with country name of the conflict."""
    # Convert the UCDP data to a GeoDataFrame (so it can be mapped and used in spatial analysis).
    # The 'wkt.loads' function takes the coordinates in the 'geometry' column and ensures geopandas will use it to map the data.
    gdf = tb[["relid", "geom_wkt"]]
    gdf.rename(columns={"geom_wkt": "geometry"}, inplace=True)
    gdf["geometry"] = gdf["geometry"].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(gdf, crs="epsg:4326")

    # Format the map to be a GeoDataFrame with a gemoetry column
    gdf_maps = gpd.GeoDataFrame(tb_maps)
    gdf_maps["geometry"] = gdf_maps["geometry"].apply(wkt.loads)
    gdf_maps = gdf_maps.set_geometry("geometry")
    gdf_maps.crs = "epsg:4326"

    # Use the overlay function to extract data from the world map that each point sits on top of.
    gdf_match = gpd.overlay(gdf, gdf_maps, how="intersection")
    # Events not assigned to any country
    # There are 1618 points that are missed - likely because they are in the sea perhaps due to the conflict either happening at sea or at the coast and the coordinates are slightly inaccurate.
    assert gdf.shape[0] - gdf_match.shape[0] == 1618, "Unexpected number of events without exact coordinate match!"

    # Get missing entries
    ids_missing = set(gdf["relid"]) - set(gdf_match["relid"])
    gdf_missing = gdf[gdf["relid"].isin(ids_missing)]

    # Reprojecting the points and the world into the World Equidistant Cylindrical Sphere projection.
    wec_crs = "+proj=eqc +lat_ts=0 +lat_0=0 +lon_0=0 +x_0=0 +y_0=0 +a=6371007 +b=6371007 +units=m +no_defs"
    gdf_missing_wec = gdf_missing.to_crs(wec_crs)
    gdf_maps_wec = gdf_maps.to_crs(wec_crs)
    # For these points we can find the nearest country using the distance function
    polygon_near = []
    for _, row in gdf_missing_wec.iterrows():
        polygon_index = gdf_maps_wec.distance(row["geometry"]).sort_values().index[0]
        ne_country_name = gdf_maps_wec["name"][polygon_index]
        polygon_near.append(ne_country_name)
    # Assign
    gdf_missing["name"] = polygon_near

    # Combining and adding name to original table
    COLUMN_COUNTRY_NAME = "country_name_location"
    gdf_country_names = pr.concat([Table(gdf_match[["relid", "name"]]), Table(gdf_missing[["relid", "name"]])])
    tb = tb.merge(gdf_country_names, on="relid", how="left", validate="one_to_one").rename(
        columns={"name": COLUMN_COUNTRY_NAME}
    )
    assert tb[COLUMN_COUNTRY_NAME].notna().all(), "Some missing values found in `COLUMN_COUNTRY_NAME`"

    # SOME CORRECTIONS #
    # To align with OWID borders we will rename the conflicts in Somaliland to Somalia and the conflicts in Morocco that were below 27.66727 latitude to Western Sahara.
    ## Somaliland -> Somalia
    mask = tb[COLUMN_COUNTRY_NAME] == "Somaliland"
    paths.log.info(f"{len(tb.loc[mask, COLUMN_COUNTRY_NAME])} datapoints in Somaliland")
    tb.loc[mask, COLUMN_COUNTRY_NAME] = "Somalia"
    ## Morocco -> Western Sahara
    mask = (tb[COLUMN_COUNTRY_NAME] == "Morocco") & (tb["latitude"] < 27.66727)
    paths.log.info(f"{len(tb.loc[mask, COLUMN_COUNTRY_NAME])} datapoints in land contested by Morocco/W.Sahara")
    tb.loc[mask, COLUMN_COUNTRY_NAME] = "Western Sahara"

    # Add a flag column for points likely to have inccorect corrdinates:
    # a) points where coordiantes are (0 0), or points where latitude and longitude are exactly the same
    tb["flag"] = ""
    # Items are (mask, flag_message)
    errors = [
        (
            tb["geom_wkt"] == "POINT (0 0)",
            "coordinates (0 0)",
        ),
        (tb["latitude"] == tb["longitude"], "latitude = longitude"),
    ]
    for error in errors:
        tb.loc[error[0], "flag"] = error[1]
        tb.loc[mask, COLUMN_COUNTRY_NAME] = np.nan

    assert tb["country_name_location"].isna().sum() == 4, "4 missing values were expected! Found a different amount!"
    tb = tb.dropna(subset=["country_name_location"])

    return tb
