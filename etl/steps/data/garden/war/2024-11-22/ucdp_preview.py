"""Data from UCDP.


IMPORTANT NOTE:

    - This script is basically a copy of the latest script used to generate UCDP dataset. At some point we should align the tools in both scripts to avoid duplication.


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

from datetime import datetime
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
    get_number_of_countries_in_conflict_by_region,
)
from structlog import get_logger

from etl.data_helpers import geo
from etl.data_helpers.misc import expand_time_column
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
UNKNOWN_TYPE_ID = 99
UNKNOWN_TYPE_NAME = "state-based (unknown)"
TYPE_OF_CONFLICT_MAPPING = {
    1: "extrasystemic",
    2: "interstate",
    3: "intrastate (non-internationalized)",
    4: "intrastate (internationalized)",
    UNKNOWN_TYPE_ID: UNKNOWN_TYPE_NAME,
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
# Last year of data
LAST_YEAR_STABLE = 2023
LAST_YEAR_CED = 2024
LAST_YEAR = 2023


def run(dest_dir: str) -> None:
    paths.log.info("start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("ucdp")
    ds_ced = paths.load_dataset("ucdp_ced")

    # Read table from GW codes
    ds_gw = paths.load_dataset("gleditsch")
    tb_regions = ds_gw.read("gleditsch_regions")
    tb_codes = ds_gw["gleditsch_countries"]

    # Load maps table
    short_name = "nat_earth_110"
    ds_maps = paths.load_dataset(short_name)
    tb_maps = ds_maps.read(short_name)

    # Load population
    ds_population = paths.load_dataset("population")

    #
    # Process data.
    #
    paths.log.info("sanity checks")
    _sanity_checks(ds_meadow)

    # Load relevant tables
    tb_ged = ds_meadow.read("ucdp_ged").astype(
        {
            "deaths_a": float,
            "deaths_b": float,
            "deaths_civilians": float,
            "deaths_unknown": float,
            "best": float,
            "high": float,
            "low": float,
        }
    )
    tb_ced = ds_ced.read("ucdp_ced").astype(
        {
            "deaths_a": float,
            "deaths_b": float,
            "deaths_civilians": float,
            "deaths_unknown": float,
            "best": float,
            "high": float,
            "low": float,
        }
    )
    tb_conflict = ds_meadow.read("ucdp_battle_related_conflict").astype(
        {
            "bd_best": float,
            "bd_low": float,
            "bd_high": float,
        }
    )
    tb_prio = ds_meadow.read("ucdp_prio_armed_conflict")

    # Extend codes to have data for latest years
    tb_codes = extend_latest_years(tb_codes)

    # Merge CED into GED
    assert (tb_ced.columns == tb_ged.columns).all(), "Columns are not the same!"
    assert tb_ged["year"].max() == LAST_YEAR_STABLE, "GED data is not up to date!"
    assert tb_ced["year"].max() == LAST_YEAR_CED, "CED data is not up to date!"
    tb_ced = tb_ced[tb_ged.columns]
    tb_ged = pr.concat([tb_ged, tb_ced], ignore_index=True)

    # Keep only active conflicts
    paths.log.info("keep active conflicts")
    tb_ged = tb_ged.loc[tb_ged["active_year"] == 1]

    # Change region named "Asia" to "Asia and Oceania" (in GED)
    tb_ged["region"] = tb_ged["region"].replace({"Asia": "Asia and Oceania"})

    # Create `conflict_type` column
    paths.log.info("add field `conflict_type`")
    tb = add_conflict_type(tb_ged, tb_conflict)

    # Sanity-check that the number of 'unknown' types of some conflicts is controlled
    # NOTE: Export summary of conflicts that have no category assigned
    tb_summary = get_summary_unknown(tb)
    assert len(tb_summary) / tb["conflict_new_id"].nunique() < 0.01, "Too many conflicts without a category assigned!"
    # tb_summary.to_csv("summary.csv")

    # Get country-level stuff
    paths.log.info("getting country-level indicators")
    tb_participants = estimate_metrics_participants(tb, tb_prio, tb_codes)
    tb_locations = estimate_metrics_locations(tb, tb_maps, tb_codes, ds_population)

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
    tb_prio = expand_time_column(
        tb_prio,
        dimension_col=["region", "conflict_type"],
        time_col="year",
        method="full_range",
        fillna_method="zero",
    )
    tb = expand_time_column(
        tb,
        dimension_col=["region", "conflict_type"],
        time_col="year",
        method="full_range",
        fillna_method="zero",
    )
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
            # "number_deaths_ongoing_conflicts_civilians",
            # "number_deaths_ongoing_conflicts_unknown",
            # "number_deaths_ongoing_conflicts_combatants",
        ],
    )

    # Adapt region names
    tb = adapt_region_names(tb)

    # Tables
    tables = [
        tb.format(["year", "region", "conflict_type"], short_name=paths.short_name),
        tb_participants.format(["year", "country", "conflict_type"]),
        tb_locations.format(["year", "country", "conflict_type"]),
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

    def _check_consistency_of_ged(
        tb_ged: Table,
        tb_type: Table,
        death_col: str,
        type_of_violence: int,
        conflict_ids_errors: Optional[List[int]] = None,
    ):
        ERR_THRESHOLD = 0.015

        # Check IDs
        ged_ids = tb_ged.loc[tb_ged["type_of_violence"] == type_of_violence, ["conflict_new_id"]].drop_duplicates()
        conflict_ids = tb_type[["conflict_id"]].drop_duplicates()
        res = ged_ids.merge(conflict_ids, left_on="conflict_new_id", right_on="conflict_id", how="outer")
        assert res.isna().sum().sum() == 0, "Check NaNs in conflict_new_id or conflict_id"

        # Check number of deaths
        deaths_ged = (
            tb_ged.loc[(tb_ged["type_of_violence"] == type_of_violence) & (tb_ged["active_year"] == 1)]
            .groupby(["conflict_new_id", "year"], as_index=False)[["best"]]
            .sum()
            .sort_values(["conflict_new_id", "year"])
        )
        deaths = tb_type[["conflict_id", "year", death_col]].sort_values(["conflict_id", "year"])
        res = deaths_ged.merge(
            deaths, left_on=["conflict_new_id", "year"], right_on=["conflict_id", "year"], how="outer"
        )

        # Get error
        res["err"] = res["best"].astype(float) - res[death_col].astype(float)
        res["err_rel"] = res["err"] / res["best"]
        res = res[res["err_rel"] > ERR_THRESHOLD]
        # Remove accepted errors
        if conflict_ids_errors is not None:
            res = res.loc[~res["conflict_new_id"].isin(conflict_ids_errors)]
        assert (
            len(res) == 0
        ), f"Dicrepancy between number of deaths in conflict ({tb_ged.m.short_name} vs. {tb_type.m.short_name}). \n {res})"

    # Read tables
    tb_ged = ds["ucdp_ged"].reset_index()
    tb_conflict = ds["ucdp_battle_related_conflict"].reset_index()
    tb_nonstate = ds["ucdp_non_state"].reset_index()
    tb_onesided = ds["ucdp_one_sided"].reset_index()

    # Battle-related conflict #
    _check_consistency_of_ged(
        tb_ged,
        tb_conflict,
        "bd_best",
        1,
    )

    # Non-state #
    _check_consistency_of_ged(
        tb_ged,
        tb_nonstate,
        "best_fatality_estimate",
        2,
        [16009],
    )

    # One-sided #
    _check_consistency_of_ged(
        tb_ged,
        tb_onesided,
        "best_fatality_estimate",
        3,
        [16009],
    )


def add_conflict_type(tb_ged: Table, tb_conflict: Table) -> Table:
    """Add `conflict_type` to georeferenced dataset table.

    Values for conflict_type are:
       - non-state conflict
       - one-sided violence
       - extrasystemic
       - interstate
       - intrastate
       - internationalized intrastate

    The thing is that the original table `tb_ged` only contains a very high level categorisation. In particular,
    it labels all state-based conflicts as 'state-based'. Instead, we want to use a more fine grained definition:
    extrasystemic, intrastate, interstate.

    Parameters
    ----------
        tb_ged: Table
            This is the main table with the relevant data
        tb_conflict: Table
            This is a secondary table, that we use to obtain the conflict types of the conflicts.
    """
    tb_conflict = tb_conflict.loc[:, ["conflict_id", "year", "type_of_conflict"]].drop_duplicates()
    assert tb_conflict.groupby(["conflict_id", "year"]).size().max() == 1, "Some conflict_id-year pairs are duplicated!"

    # Add `type_of_conflict` to `tb_ged`.
    # This column contains the type of state-based conflict (1: inter-state, 2: intra-state, 3: extra-state, 4: internationalized intrastate)
    tb_ged = tb_ged.merge(
        tb_conflict,
        left_on=["conflict_new_id", "year"],
        right_on=["conflict_id", "year"],
        how="outer",
    )

    # Assign latest available conflict type to unknown state-based conflicts
    tb_ged = patch_unknown_conflict_type_ced(tb_ged)

    # Assert that `type_of_conflict` was only added for state-based events
    assert (
        tb_ged[tb_ged["type_of_violence"] != 1]["type_of_conflict"].isna().all()
    ), "There are some actual values for non-state based conflicts! These should only be NaN, since `tb_conflict` should only contain data for state-based conflicts."
    # Check that `type_of_conflict` is not NaN for state-based events
    assert (
        not tb_ged[tb_ged["type_of_violence"] == 1]["type_of_conflict"].isna().any()
    ), "Could not find the type of conflict for some state-based conflicts!"

    # Create `conflict_type` column as a combination of `type_of_violence` and `type_of_conflict`.
    tb_ged["conflict_type"] = (
        tb_ged["type_of_conflict"]
        .astype(object)
        .replace(TYPE_OF_CONFLICT_MAPPING)
        .fillna(tb_ged["type_of_violence"].astype(object).replace(TYPE_OF_VIOLENCE_MAPPING))
    )

    # Sanity check
    assert tb_ged["conflict_type"].isna().sum() == 0, "Check NaNs in conflict_type (i.e. conflicts without a type)!"

    return tb_ged


def patch_unknown_conflict_type_ced(tb):
    """Assign conflict types to unknown state-based conflicts (based on latest category appearing in GED)"""
    mask = (tb["type_of_violence"] == 1) & (tb["type_of_conflict"].isna())
    assert (
        tb.loc[mask, "year"] > LAST_YEAR_STABLE
    ).all(), "Unknown conflict types should only be present in years after GED!"
    ids_unknown = list(tb.loc[mask, "conflict_new_id"].unique())

    # Get table with the latest assigned conflict type for each conflict that has category 'state-based (unknown)' assigned
    id_to_type = (
        tb.loc[tb["conflict_new_id"].isin(ids_unknown) & ~mask, ["conflict_new_id", "year", "type_of_conflict"]]
        .sort_values("year")
        .drop_duplicates(subset=["conflict_new_id"], keep="last")
        .set_index("conflict_new_id")["type_of_conflict"]
        .to_dict()
    )
    tb.loc[mask, "type_of_conflict"] = tb.loc[mask, "conflict_new_id"].apply(
        lambda x: id_to_type.get(x, UNKNOWN_TYPE_ID)
    )
    return tb


def _sanity_check_conflict_types(tb: Table) -> Table:
    """Check conflict type.

    - Only transitions accepted are between intrastate conflicts.
    - The same conflict is only expceted to have one type in a year.
    """
    # Define expected combinations of conflicT_types for a conflict. Typically, only in the intrastate domain
    TRANSITION_EXPECTED = {"intrastate (internationalized)", "intrastate (non-internationalized)"}
    # Get conflicts with more than one conflict type assigned to them over their lifetime
    tb_ = tb.loc[tb["year"] < LAST_YEAR_STABLE]
    conflict_type_transitions = tb_.groupby("conflict_new_id")["conflict_type"].apply(set)
    transitions = conflict_type_transitions[conflict_type_transitions.apply(len) > 1].drop_duplicates()
    # Extract unique combinations of conflict_types for a conflict
    assert (len(transitions) == 1) & (transitions.iloc[0] == TRANSITION_EXPECTED), "Error"

    # Check if different regions categorise the conflict differently in the same year
    assert not (
        tb_.groupby(["conflict_id", "year"])["type_of_conflict"].nunique() > 1
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
    # Estimate combatant deaths per conflict
    tb_ = tb.copy()
    tb_["deaths_combatants"] = tb_["deaths_a"] + tb_["deaths_b"]

    # Define aggregations
    column_props = {
        # Deaths (estimates)
        "best": {
            "f": "sum",
            "rename": "number_deaths_ongoing_conflicts",
        },
        "high": {
            "f": "sum",
            "rename": "number_deaths_ongoing_conflicts_high",
        },
        "low": {
            "f": "sum",
            "rename": "number_deaths_ongoing_conflicts_low",
        },
        # Deaths by type
        "deaths_civilians": {
            "f": "sum",
            "rename": "number_deaths_ongoing_conflicts_civilians",
        },
        "deaths_unknown": {
            "f": "sum",
            "rename": "number_deaths_ongoing_conflicts_unknown",
        },
        "deaths_combatants": {
            "f": "sum",
            "rename": "number_deaths_ongoing_conflicts_combatants",
        },
        # Number of conflicts
        "conflict_new_id": {
            "f": "nunique",
            "rename": "number_ongoing_conflicts",
        },
    }
    col_funcs = {k: v["f"] for k, v in column_props.items()}
    col_renames = {k: v["rename"] for k, v in column_props.items()}
    # For each region
    columns_idx = ["year", "region", "conflict_type"]
    tb_ongoing = tb_.groupby(columns_idx, as_index=False).agg(col_funcs)
    tb_ongoing = tb_ongoing.rename(columns={n: n for n in columns_idx} | col_renames)

    # For the World
    columns_idx = ["year", "conflict_type"]
    tb_ongoing_world = tb_.groupby(columns_idx, as_index=False).agg(col_funcs)
    tb_ongoing_world = tb_ongoing_world.rename(columns={n: n for n in columns_idx} | col_renames)
    tb_ongoing_world["region"] = "World"

    # Combine
    tb_ongoing = pr.concat([tb_ongoing, tb_ongoing_world], ignore_index=True).sort_values(  # type: ignore
        by=["year", "region", "conflict_type"]
    )

    # Check that `deaths = deaths_combatants + deaths_civilians + deaths_unknown` holds
    assert (
        tb_ongoing["number_deaths_ongoing_conflicts"]
        - tb_ongoing[
            [
                "number_deaths_ongoing_conflicts_civilians",
                "number_deaths_ongoing_conflicts_unknown",
                "number_deaths_ongoing_conflicts_combatants",
            ]
        ].sum(axis=1)
        == 0
    ).all(), "Sum of deaths from combatants, civilians and unknown should equal best estimate!"
    return tb_ongoing


def _get_new_metrics(tb: Table) -> Table:
    # Reduce table to only preserve first appearing event
    tb = (
        tb.loc[:, ["conflict_new_id", "year", "region", "conflict_type"]]
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
    assert tb["year"].max() == LAST_YEAR_CED, "Unexpected start year!"
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
    ## Data from GEO for `number_ongoing_conflicts` goes from 1989 to 2023 (inc)
    assert tb[tb["number_ongoing_conflicts_main"].notna()].year.min() == 1989
    assert tb[tb["number_ongoing_conflicts_main"].notna()]["year"].max() == LAST_YEAR_CED
    ## Data from PRIO/UCDP for `number_new_conflicts` goes from 1946 to 1989 (inc)
    assert tb[tb["number_new_conflicts_prio"].notna()]["year"].min() == 1946
    assert tb[tb["number_new_conflicts_prio"].notna()]["year"].max() == 1989
    ## Data from GEO for `number_new_conflicts` goes from 1990 to 2022 (inc)
    assert tb[tb["number_new_conflicts_main"].notna()]["year"].min() == 1990
    assert tb[tb["number_new_conflicts_main"].notna()]["year"].max() == LAST_YEAR_CED

    # Actually combine timeseries from UCDP/PRIO and GEO.
    # We prioritise values from PRIO for 1989, therefore the order `PRIO.fillna(MAIN)`
    tb["number_ongoing_conflicts"] = tb["number_ongoing_conflicts_prio"].fillna(tb["number_ongoing_conflicts_main"])
    tb["number_new_conflicts"] = tb["number_new_conflicts_prio"].fillna(tb["number_new_conflicts_main"])

    # Remove unnecessary columns
    columns_remove = tb.filter(regex=r"(_prio|_main)").columns
    tb = tb[[col for col in tb.columns if col not in columns_remove]]

    return tb


def fix_extrasystemic_entries(tb: Table) -> Table:
    """Fix entries with conflict_type='extrasystemic'.

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
        "number_deaths_ongoing_conflicts_civilians",
        "number_deaths_ongoing_conflicts_unknown",
        "number_deaths_ongoing_conflicts_combatants",
    ]
    mask_1989 = tb_extra["year"] >= 1989
    tb_extra.loc[mask_1989, columns] = tb_extra.loc[mask_1989, columns].fillna(0)

    # Add to main table
    tb = pr.concat([tb[-mask], tb_extra])
    return tb


def _prepare_prio_table(tb: Table) -> Table:
    # Select relevant columns
    tb = tb.loc[:, ["conflict_id", "year", "region", "type_of_conflict", "start_date"]]

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
            "number_deaths_ongoing_conflicts_civilians",
            "number_deaths_ongoing_conflicts_unknown",
            "number_deaths_ongoing_conflicts_combatants",
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


def estimate_metrics_participants(tb: Table, tb_prio: Table, tb_codes: Table) -> Table:
    """Add participant information at country-level."""
    ###################
    # Participated in #
    ###################
    # FLAG YES/NO (country-level)

    # Get table with [year, conflict_type, code]
    codes = ["gwnoa", "gwnob"]
    tb_country = pr.concat(
        [tb.loc[:, ["year", "conflict_type", code]].rename(columns={code: "id"}).copy() for code in codes]
    )

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
    ctypes_all = list(set(tb_country["conflict_type"]))
    tb_alltypes = Table(pd.DataFrame({"conflict_type": ctypes_all}))
    tb_codes_ = tb_codes.reset_index().merge(tb_alltypes, how="cross")
    tb_codes_["country"] = tb_codes_["country"].astype(str)

    # Combine all GW entries with UCDP
    columns_idx = ["year", "country", "id", "conflict_type"]
    tb_country = tb_codes_.merge(tb_country, on=columns_idx, how="outer")
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

    tb_num_participants = get_number_of_countries_in_conflict_by_region(tb_country, "conflict_type")

    # Combine tables
    tb_country = pr.concat([tb_country, tb_num_participants], ignore_index=True)

    # Drop column `id`
    tb_country = tb_country.drop(columns=["id"])

    ############
    # Add PRIO #
    ############
    tb_country_prio = estimate_metrics_participants_prio(tb_prio, tb_codes)

    tb_country = pr.concat([tb_country, tb_country_prio], ignore_index=True, short_name=f"{paths.short_name}_country")

    return tb_country


def estimate_metrics_participants_prio(tb_prio: Table, tb_codes: Table) -> Table:
    """Add participant information at country-level.

    Only works for UCDP/PRIO data.
    """
    ###################
    # Participated in #
    ###################
    # FLAG YES/NO (country-level)

    # Get table with [year, conflict_type, code]
    codes = ["gwno_a", "gwno_a_2nd", "gwno_b", "gwno_b_2nd"]
    tb_country = pr.concat(
        [tb_prio[["year", "type_of_conflict", code]].rename(columns={code: "id"}).copy() for code in codes]
    )

    # Drop rows with code = NaN
    tb_country = tb_country.dropna(subset=["id"])
    # Drop duplicates
    tb_country = tb_country.drop_duplicates()

    # Explode where multiple codes
    tb_country["id"] = tb_country["id"].astype(str).str.split(",")
    tb_country = tb_country.explode("id")
    # Ensure numeric type
    tb_country["id"] = tb_country["id"].astype(int)
    # Drop duplicates (may appear duplicates after exploding)
    tb_country = tb_country.drop_duplicates()

    # Sanity check
    assert not tb_country.isna().any(axis=None), "There are some NaNs!"

    # Correct codes
    ## 751 'Government of Hyderabad' -> 750 'India'
    tb_country.loc[tb_country["id"] == 751, "id"] = 750
    ## 817 'Republic of Vietnam' in 1975 -> 816 'Vietnam'
    tb_country.loc[(tb_country["id"] == 817) & (tb_country["year"] == 1975), "id"] = 816
    ## 345 'Yugoslavia' after 2005 -> 340 'Serbia'
    tb_country.loc[(tb_country["id"] == 345) & (tb_country["year"] > 2005), "id"] = 340
    # Add country name
    tb_country["country"] = tb_country.apply(lambda x: tb_codes.loc[(x["id"], x["year"])], axis=1)
    assert tb_country["country"].notna().all(), "Some countries were not found! NaN was set"
    ## Remove duplicates after correcting codes
    tb_country = tb_country.drop_duplicates()

    # Add flag
    tb_country["participated_in_conflict"] = 1
    tb_country["participated_in_conflict"].m.origins = tb_prio["gwno_a"].m.origins

    # Format conflict tyep
    tb_country["conflict_type"] = tb_country["type_of_conflict"].astype(object).replace(TYPE_OF_CONFLICT_MAPPING)
    tb_country = tb_country.drop(columns=["type_of_conflict"])

    # Prepare GW table
    tb_alltypes = Table(pd.DataFrame({"conflict_type": tb_country["conflict_type"].unique()}))
    tb_codes = tb_codes.reset_index().merge(tb_alltypes, how="cross")
    tb_codes["country"] = tb_codes["country"].astype(str)

    # Combine all GW entries with UCDP/PRIO
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
    tb_country = tb_country[
        (tb_country["year"] >= tb_prio["year"].min()) & (tb_country["year"] <= tb_prio["year"].max())
    ]

    ###################
    # Participated in #
    ###################
    # NUMBER COUNTRIES

    tb_num_participants = get_number_of_countries_in_conflict_by_region(tb_country, "conflict_type")

    # Combine tables
    tb_country = pr.concat([tb_country, tb_num_participants], ignore_index=True)

    # Drop column `id`
    tb_country = tb_country.drop(columns=["id"])

    ###############
    # Final steps #
    ###############

    # Keep only years not covered by UCDP (except for 'extrasystemic')
    tb_country = tb_country[(tb_country["year"] < 1989) | (tb_country["conflict_type"] == "extrasystemic")]
    return tb_country


def estimate_metrics_locations(tb: Table, tb_maps: Table, tb_codes: Table, ds_population: Dataset) -> Table:
    """Add participant information at country-level.

    reference: https://github.com/owid/notebooks/blob/main/JoeHasell/UCDP%20and%20PRIO/UCDP_georeferenced/ucdp_country_extract.ipynb

    tb: actual data
    tb_maps: map data (borders and stuff)
    tb_codes: from gw codes. so that all countries have either a 1 or 0 (instead of missing data).
    ds_population: population data (for rates)
    """
    tb_codes_ = tb_codes.reset_index().drop(columns=["id"]).copy()
    tb_codes_ = tb_codes_[tb_codes_["year"] >= 1989]

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

    # Check that number of deaths is all zero
    assert (
        tb_locations["best"] - tb_locations[["deaths_a", "deaths_b", "deaths_civilians", "deaths_unknown"]].sum(axis=1)
        == 0
    ).all(), "Sum of deaths from combatants, civilians and unknown should equal best estimate!"
    tb_locations["deaths_combatants"] = tb_locations["deaths_a"] + tb_locations["deaths_b"]

    # Estimate if a conflict occured in a country, and the number of deaths in it
    # Define aggregations
    INDICATOR_BASE_NAME = "number_deaths"
    column_props = {
        # Deaths (estimates)
        "best": {
            "f": "sum",
            "rename": f"{INDICATOR_BASE_NAME}",
        },
        "high": {
            "f": "sum",
            "rename": f"{INDICATOR_BASE_NAME}_high",
        },
        "low": {
            "f": "sum",
            "rename": f"{INDICATOR_BASE_NAME}_low",
        },
        # Deaths by type
        "deaths_civilians": {
            "f": "sum",
            "rename": f"{INDICATOR_BASE_NAME}_civilians",
        },
        "deaths_unknown": {
            "f": "sum",
            "rename": f"{INDICATOR_BASE_NAME}_unknown",
        },
        "deaths_combatants": {
            "f": "sum",
            "rename": f"{INDICATOR_BASE_NAME}_combatants",
        },
        # Number of conflicts
        "conflict_new_id": {
            "f": "nunique",
            "rename": "is_location_of_conflict",
        },
    }
    # TODO: continue here
    col_funcs = {k: v["f"] for k, v in column_props.items()}
    col_renames = {k: v["rename"] for k, v in column_props.items()}
    tb_locations_country = (
        tb_locations.groupby(["country_name_location", "year", "conflict_type"], as_index=False)
        .agg(col_funcs)
        .rename(
            columns={
                "country_name_location": "country",
            }
            | col_renames
        )
    )
    assert tb_locations_country["is_location_of_conflict"].notna().all(), "Missing values in `is_location_of_conflict`!"
    cols_num_deaths = [v for v in col_renames.values() if v != "is_location_of_conflict"]
    for col in cols_num_deaths:
        assert tb_locations_country[col].notna().all(), f"Missing values in `{col}`!"
    # Convert into a binary indicator: 1 (if more than one conflict), 0 (otherwise)
    tb_locations_country["is_location_of_conflict"] = tb_locations_country["is_location_of_conflict"].apply(
        lambda x: 1 if x > 0 else 0
    )

    # Add missing countries using tb_codes as reference
    tb_locations_country = tb_codes_.merge(
        tb_locations_country,
        on=["country", "year"],
        how="outer",
    )
    # Add Greenland
    assert (
        "Greenland" not in set(tb_locations_country.country)
    ), "Greenland is not expected to be there! That's why we force it to zero. If it appears, just remove the following code line"
    tb_green = Table(pd.DataFrame({"country": ["Greenland"], "year": [LAST_YEAR]}))
    tb_locations_country = pr.concat([tb_locations_country, tb_green], ignore_index=True)

    # NaNs of numeric indicators to zero
    cols_indicators = ["is_location_of_conflict"] + cols_num_deaths
    tb_locations_country[cols_indicators] = tb_locations_country[cols_indicators].fillna(0)
    # NaN in conflict_type to arbitrary (since missing ones are filled from the next operation with fill_gaps_with_zeroes)
    mask = tb_locations_country["conflict_type"].isna()
    assert (
        tb_locations_country.loc[mask, cols_indicators].sum().sum() == 0
    ), "There are some non-NaNs for NaN-valued conflict types!"
    tb_locations_country["conflict_type"] = tb_locations_country["conflict_type"].fillna("one-sided violence")

    # Fill with zeroes
    tb_locations_country = expand_time_column(
        tb_locations_country,
        dimension_col=["country", "conflict_type"],
        time_col="year",
        method="full_range",
        fillna_method="zero",
    )

    # Add origins from Natural Earth
    cols = ["is_location_of_conflict"] + cols_num_deaths
    for col in cols:
        tb_locations_country[col].origins += tb_maps["name"].m.origins

    ###################
    # Add conflict type aggregates
    ###################
    paths.log.info("adding conflict type aggregates...")

    # Add missing conflict types
    CTYPES_AGGREGATES = {
        "intrastate": ["intrastate (non-internationalized)", "intrastate (internationalized)"],
        "state-based": list(TYPE_OF_CONFLICT_MAPPING.values()),
        "all": list(TYPE_OF_VIOLENCE_MAPPING.values()) + list(TYPE_OF_CONFLICT_MAPPING.values()),
    }
    for ctype_agg, ctypes in CTYPES_AGGREGATES.items():
        tb_locations_country = aggregate_conflict_types(
            tb=tb_locations_country,
            parent_name=ctype_agg,
            children_names=ctypes,
            columns_to_aggregate=["is_location_of_conflict"] + cols_num_deaths,
            columns_to_aggregate_absolute=cols_num_deaths,
            columns_to_groupby=["country", "year"],
        )

    ###################
    # Add rates
    ###################
    # Add population column
    tb_locations_country = geo.add_population_to_table(
        tb=tb_locations_country,
        ds_population=ds_population,
    )
    # Divide and obtain rates
    factor = 100_000
    suffix = [c.replace(INDICATOR_BASE_NAME, "") for c in cols_num_deaths]
    suffix = [suf for suf in suffix if suf not in {"_combatants", "_unknown", "_civilians"}]
    for suf in suffix:
        tb_locations_country[f"death_rate{suf}"] = (
            factor * tb_locations_country[f"{INDICATOR_BASE_NAME}{suf}"] / tb_locations_country["population"]
        )

    # Drop population column
    tb_locations_country = tb_locations_country.drop(columns=["population"])

    ###################
    # REGION-LEVEL: Number of locations with conflict
    ###################
    paths.log.info("estimating number of locations with conflict...")

    def _get_number_of_locations_with_conflict_regions(tb: Table, cols: List[str]) -> Table:
        """Get number of locations with conflict."""
        # For each group, get the number of unique locations
        tb = (
            tb.groupby(cols)
            .agg(
                {
                    "country_name_location": "nunique",
                }
            )
            .reset_index()
        )
        # Rename columns
        if "region" in cols:
            column_rename = {
                "country_name_location": "number_locations",
                "region": "country",
            }
        else:
            column_rename = {
                "country_name_location": "number_locations",
            }

        tb = tb.rename(columns=column_rename)
        return tb

    # Regions
    ## Number of countries (given ctypes)
    tb_locations_regions = _get_number_of_locations_with_conflict_regions(
        tb_locations, ["region", "year", "conflict_type"]
    )
    tb_locations_regions_world = _get_number_of_locations_with_conflict_regions(tb_locations, ["year", "conflict_type"])
    tb_locations_regions_world["country"] = "World"

    tbs_locations_regions = [
        tb_locations_regions,
        tb_locations_regions_world,
    ]

    ## Extra conflict types (aggregates)
    cols = ["region", "year"]
    for ctype_agg, ctypes in CTYPES_AGGREGATES.items():
        # Keep only children for this ctype aggregate
        tb_locations_ = tb_locations[tb_locations["conflict_type"].isin(ctypes)]
        # Get actual table, add ctype. (also for region 'World')
        tb_locations_regions_agg = _get_number_of_locations_with_conflict_regions(tb_locations_, ["region", "year"])
        tb_locations_regions_agg["conflict_type"] = ctype_agg
        tb_locations_regions_agg_world = _get_number_of_locations_with_conflict_regions(tb_locations_, ["year"])
        tb_locations_regions_agg_world["conflict_type"] = ctype_agg
        tb_locations_regions_agg_world["country"] = "World"
        tbs_locations_regions.extend([tb_locations_regions_agg, tb_locations_regions_agg_world])

    # Combine
    tb_locations_regions = pr.concat(
        tbs_locations_regions,
        ignore_index=True,
    )

    # Add origins
    tb_locations_regions["number_locations"].m.origins = tb_locations_country["is_location_of_conflict"].origins

    # Extend to full time-series + fill NaNs with zeros.
    tb_locations_regions = expand_time_column(
        df=tb_locations_regions,
        dimension_col=["country", "conflict_type"],
        time_col="year",
        method="full_range",
        fillna_method="zero",
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
    # There are 2271 points that are missed - likely because they are in the sea perhaps due to the conflict either happening at sea or at the coast and the coordinates are slightly inaccurate.
    # I've soften the assertion, otherwise a bit of a pain!
    assert (
        diff := gdf.shape[0] - gdf_match.shape[0]
    ) <= 2280, f"Unexpected number of events without exact coordinate match! {diff}"
    # DEBUG: Examine which are these unlabeled conflicts
    # mask = ~tb["relid"].isin(gdf_match["relid"])
    # tb.loc[mask, ["relid", "year", "conflict_name", "side_a", "side_b", "best"]]

    # Get missing entries
    ids_missing = set(gdf["relid"]) - set(gdf_match["relid"])
    gdf_missing = gdf.loc[gdf["relid"].isin(ids_missing)]

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

    assert tb[COLUMN_COUNTRY_NAME].isna().sum() == 4, "4 missing values were expected! Found a different amount!"
    tb = tb.dropna(subset=[COLUMN_COUNTRY_NAME])

    return tb


def extend_latest_years(tb: Table) -> Table:
    """Create table with each country present in a year."""

    index = list(tb.index.names)
    tb = tb.reset_index()

    # define mask for last year
    mask = tb["year"] == LAST_YEAR_STABLE

    # Get year to extend to
    current_year = datetime.now().year

    tb_all_years = Table(pd.RangeIndex(LAST_YEAR_STABLE + 1, current_year + 1), columns=["year"])
    tb_last = tb[mask].drop(columns="year").merge(tb_all_years, how="cross")

    tb = pr.concat([tb, tb_last], ignore_index=True, short_name="gleditsch_countries")

    tb = tb.set_index(index)
    return tb


def get_summary_unknown(tb: Table):
    """Get a table summary of the ongoing conflicts that couldn't be mapped to a specific category.

    We know that these are state-based conflicts, but we don't have more information about them!

    By looking at them, we may be able to map these to a specific category:

        - "extrasystemic",
        - "interstate"
        - "intrastate (non-internationalized)"
        - "intrastate (internationalized)"
    """
    tbx = tb.loc[
        tb["type_of_conflict"] == UNKNOWN_TYPE_ID,
        ["id", "conflict_new_id", "conflict_name", "date_start", "date_end", "side_a", "side_b"],
    ]
    tbx = tbx.groupby(["conflict_new_id", "conflict_name"], as_index=False).agg(
        {
            "date_start": "min",
            "date_end": "max",
            "side_a": (lambda x: "; ".join(set(x))),
            "side_b": (lambda x: "; ".join(set(x))),
            "id": "nunique",
        }
    )
    tbx = tbx.drop_duplicates(subset=["conflict_new_id", "conflict_name"])
    tbx["date_start"] = pd.to_datetime(tbx["date_start"])
    tbx["date_end"] = pd.to_datetime(tbx["date_end"])
    tbx = tbx.rename(columns={"id": "num_events"})
    tbx = tbx.sort_values(["num_events", "date_start"], ascending=False)

    return tbx
