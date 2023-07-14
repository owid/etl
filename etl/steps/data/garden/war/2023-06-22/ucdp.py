"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
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
    3: "Asia",
    4: "Africa",
    5: "Americas",
}
REGIONS_EXPECTED = set(REGIONS_MAPPING.values())


def run(dest_dir: str) -> None:
    log.info("war_ucdp.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("ucdp"))

    #
    # Process data.
    #
    log.info("ucdp: sanity checks")
    _sanity_checks(ds_meadow)

    # Load relevant tables
    tb_geo = ds_meadow["geo"]
    tb_conflict = ds_meadow["battle_related_conflict"]
    tb_prio = ds_meadow["prio_armed_conflict"]

    # Create `conflict_type` column
    log.info("ucdp: add field `conflict_type`")
    tb_geo = tb_geo[tb_geo["active_year"] == 1]
    tb = add_conflict_type(tb_geo, tb_conflict)

    # Sanity check conflict_type transitions
    ## Only consider transitions between intrastate and intl intrastate
    _sanity_check_conflict_types(tb)
    _sanity_check_prio_conflict_types(tb_prio)

    # Add number of new conflicts and ongoing conflicts (also adds data for the World)
    log.info("ucdp: get metrics for main dataset (also estimate values for 'World')")
    tb = add_number_conflicts_and_deaths(tb)

    # Add table from UCDP/PRIO
    log.info("ucdp: prepare data from ucdp/prio table (also estimate values for 'World')")
    tb_prio = prepare_prio_data(tb_prio)

    # Fill NaNs
    log.info("ucdp: replace missing data with zeros (where applicable)")
    tb_prio = replace_missing_data_with_zeros(tb_prio)
    tb = replace_missing_data_with_zeros(tb)

    # Combine main dataset with PRIO/UCDP
    log.info("ucdp: add data from ucdp/prio table")
    tb = add_prio_data(tb, tb_prio)

    # Add data for "all conflicts" conflict type
    log.info("ucdp: add data for 'all conflicts'")
    tb = add_conflict_all(tb)

    # Add data for "all intrastate" conflict types
    tb = add_conflict_all_intrastate(tb)

    # Force types
    # tb = tb.astype({"conflict_type": "category", "region": "category"})

    # Adapt region names
    tb = adapt_region_names(tb)

    # Set index, sort rows
    tb = tb.set_index(["year", "region", "conflict_type"], verify_integrity=True).sort_index()

    # Add short_name to table
    log.info("ucdp: add shortname to table")
    tb.metadata.short_name = paths.short_name

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("ucdp.end")


def _sanity_checks(ds: Dataset) -> None:
    """Check that the tables in the dataset are as expected."""
    tb_geo = ds["geo"]
    tb_conflict = ds["battle_related_conflict"]
    tb_nonstate = ds["non_state"]
    tb_onesided = ds["one_sided"]

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
    """
    tb_conflict = tb_conflict[["conflict_id", "year", "type_of_conflict"]].drop_duplicates()
    assert tb_conflict.groupby(["conflict_id", "year"]).size().max() == 1, "Some conflict_id-year pairs are duplicated!"

    # Add `type_of_conflict` to `tb_geo`.
    # This column contains the type of state-based conflict (1: inter-state, 2: intra-state, 3: extra-state, 4: internationalized intrastate)
    tb_geo = tb_geo.merge(
        tb_conflict, left_on=["conflict_new_id", "year"], right_on=["conflict_id", "year"], how="outer"
    )
    # Fill unknown types of violence
    msk = tb_geo["type_of_violence"] == 1
    tb_geo.loc[msk, "type_of_conflict"] = tb_geo.loc[msk, "type_of_conflict"].fillna("state-based (unknown)")

    # Assert that `type_of_conflict` was only added for state-based events
    assert tb_geo[tb_geo["type_of_violence"] != 1]["type_of_conflict"].isna().all()
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
    # Define expected combinations of conflicT_types for a conflict. Typically, only in the intrastate domain
    TRANSITION_EXPECTED = {"intrastate (internationalized)", "intrastate (non-internationalized)"}
    # Get conflicts with more than one conflict type assigned to them over their lifetime
    conflict_type_transitions = tb.groupby("conflict_new_id")["conflict_type"].apply(set)
    transitions = conflict_type_transitions[conflict_type_transitions.apply(len) > 1].drop_duplicates()
    # Extract unique combinations of conflict_types for a conflict
    print(transitions)
    assert (len(transitions) == 1) & (transitions.iloc[0] == TRANSITION_EXPECTED), "Error"

    # Check if different regions categorise the conflict differently in the same year
    assert not (
        tb.groupby(["conflict_id", "year"])["type_of_conflict"].nunique() > 1
    ).any(), "Seems like the conflict hast multiple types for a single year! Is it categorised differently depending on the region?"


def _sanity_check_prio_conflict_types(tb: Table) -> Table:
    # Define expected combinations of conflicT_types for a conflict. Typically, only in the intrastate domain
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


def replace_missing_data_with_zeros(tb: Table) -> Table:
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
    ## For columns "number_ongoing_conflicts", "number_new_conflicts"; conflict_type="extrasystemic"
    columns = ["number_ongoing_conflicts", "number_new_conflicts"]
    tb.loc[:, columns] = tb.loc[:, columns].fillna(0)

    return tb


def add_number_conflicts_and_deaths(tb: Table) -> Table:
    """Add number of ongoing and new conflicts.

    It also estimates the values for 'World', otherwise this can't be estimated later on.
    This is because some conflicts occur in multiple regions, and hence would be double counted. To overcome this,
    we need to access the actual conflict_id field to find the number of unique values. This can only be done here.
    """
    # Get number of ongoing conflicts, and deaths in ongoing conflicts
    log.info("ucdp: get number of ongoing conflicts and deaths in ongoing conflicts")
    tb_ongoing = _add_number_ongoing_conflicts_and_deaths(tb)

    # Get number of new conflicts every year
    log.info("ucdp: get number of new conflicts every year")
    tb_new = _add_number_new_conflicts(tb)
    # Combine and build single table
    log.info("ucdp: combine and build single table")
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


def _add_number_ongoing_conflicts_and_deaths(tb: Table) -> Table:
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
    tb_ongoing = pd.concat([tb_ongoing, tb_ongoing_world], ignore_index=True).sort_values(  # type: ignore
        by=["year", "region", "conflict_type"]
    )
    return tb_ongoing


def _add_number_new_conflicts(tb: Table) -> Table:
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
    tb_new = pd.concat([tb_new, tb_new_world], ignore_index=True).sort_values(  # type: ignore
        by=["year", "region", "conflict_type"]
    )

    return tb_new


def prepare_prio_data(tb_prio: Table) -> Table:
    tb_prio = _prepare_prio_table(tb_prio)
    tb_prio = _prio_add_metrics(tb_prio)
    return tb_prio


def add_prio_data(tb: Table, tb_prio: Table) -> Table:
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
    tb_ongoing = pd.concat([tb_ongoing, tb_ongoing_world], ignore_index=True)
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
    tb_new = pd.concat([tb_new, tb_new_world], ignore_index=True)
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
    tb = pd.concat([tb, tb_all], ignore_index=True)

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
    tb = pd.concat([tb, tb_intra], ignore_index=True)
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
