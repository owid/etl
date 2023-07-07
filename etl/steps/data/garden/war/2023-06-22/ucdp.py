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

    # Create `conflict_type` column
    log.info("ucdp: add field `conflict_type`")
    tb_geo = tb_geo[tb_geo["active_year"] == 1]
    tb = add_conflict_type(tb_geo, tb_conflict)
    # Add `year_start`, which denotes the year when the conflict corresponding to the event started (only considering events with `active_year` == 1)
    log.info("ucdp: add conflict year start to each event")
    tb = add_year_start(tb)

    # Add number of new conflicts and ongoing conflicts
    tb = add_number_conflicts_and_deaths(tb)
    # Add data for World
    log.info("ucdp: add data for World")
    tb = add_world(tb)

    # Add data for "all conflicts" conflict type
    log.info("ucdp: add data for 'all conflicts'")
    tb = add_conflict_all(tb)

    # Add data for "all intrastate" conflict types
    tb = add_conflict_all_intrastate(tb)

    # Filter datapoints in 1989 for `number_new_conflicts`
    tb.loc[tb["year"] == 1989, "number_new_conflicts"] = np.nan

    # Force types
    # tb = tb.astype({"conflict_type": "category", "region": "category"})

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
    tb_conflict_relevant = tb_conflict[["conflict_id", "year", "type_of_conflict"]].drop_duplicates()
    assert (
        tb_conflict_relevant.groupby(["conflict_id", "year"]).size().max() == 1
    ), "Some conflict_id-year pairs are duplicated!"

    # Add `type_of_conflict` to `tb_geo`.
    # This column contains the type of state-based conflict (1: inter-state, 2: intra-state, 3: extra-state, 4: internationalized intrastate)
    tb_geo = tb_geo.merge(
        tb_conflict_relevant, left_on=["conflict_new_id", "year"], right_on=["conflict_id", "year"], how="outer"
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
    type_of_violence_mapping = {
        2: "non-state conflict",
        3: "one-sided violence",
    }
    type_of_conflict_mapping = {
        1: "extrasystemic",
        2: "interstate",
        3: "intrastate",
        4: "internationalized intrastate",
    }
    tb_geo["conflict_type"] = (
        tb_geo["type_of_conflict"]
        .replace(type_of_conflict_mapping)
        .fillna(tb_geo["type_of_violence"].replace(type_of_violence_mapping))
    )

    # Sanity check
    assert tb_geo["conflict_type"].isna().sum() == 0, "Check NaNs in conflict_type (i.e. conflicts without a type)!"
    return tb_geo


def add_year_start(tb: Table) -> Table:
    """Add year of conflict start."""
    tb_start_year = tb.groupby("conflict_new_id", as_index=False)[["year"]].min()
    tb = tb.merge(tb_start_year, on="conflict_new_id", how="left", suffixes=("", "_start"))
    assert tb.year_start.isna().sum() == 0, "Check NaNs in year_start!"
    return tb


def add_number_conflicts_and_deaths(tb: Table) -> Table:
    """Add number of ongoing and new conflicts."""
    # Get number of ongoing conflicts, and deaths in ongoing conflicts
    log.info("ucdp: get number of ongoing conflicts and deaths in ongoing conflicts")
    tb_ongoing = _add_number_ongoing_conflicts_and_deaths(tb)

    # Get number of new conflicts every year
    log.info("ucdp: get number of new conflicts every year")
    tb_new = _add_number_new_conflicts(tb)
    # Combine and build single table
    log.info("ucdp: combine and build single table")
    tb = tb_ongoing.merge(
        tb_new, left_on=["year", "region", "conflict_type"], right_on=["year_start", "region", "conflict_type"]
    )
    tb = tb.drop(columns=["year_start"])
    return tb


def _add_number_ongoing_conflicts_and_deaths(tb: Table) -> Table:
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
    return tb_ongoing


def _add_number_new_conflicts(tb: Table) -> Table:
    columns_idx = ["year_start", "region", "conflict_type"]
    tb_new = tb.groupby(columns_idx)[["conflict_new_id"]].nunique().reset_index()
    tb_new.columns = columns_idx + ["number_new_conflicts"]
    return tb_new


def add_world(tb: Table) -> Table:
    """Add metrics for country = 'World'."""
    tb_world = tb.groupby(["year", "conflict_type"], as_index=False)[
        [
            "number_deaths_ongoing_conflicts",
            "number_deaths_ongoing_conflicts_high",
            "number_deaths_ongoing_conflicts_low",
            "number_ongoing_conflicts",
            "number_new_conflicts",
        ]
    ].sum()
    tb_world["region"] = "World"
    tb = pd.concat([tb, tb_world], ignore_index=True)
    return tb


def add_conflict_all(tb: Table) -> Table:
    """Add metrics for conflict_type = 'all'."""
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
    tb = pd.concat([tb, tb_all], ignore_index=True)
    return tb


def add_conflict_all_intrastate(tb: Table) -> Table:
    """Add metrics for conflict_type = 'all intrastate'."""
    tb_intra = tb[tb["conflict_type"].isin(["intrastate", "internationalized intrastate"])].copy()
    tb_intra = tb_intra.groupby(["year", "region"], as_index=False).sum(numeric_only=True)
    tb_intra["conflict_type"] = "all intrastate"
    tb = pd.concat([tb, tb_intra], ignore_index=True)
    return tb
