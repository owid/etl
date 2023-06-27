"""Load a meadow dataset and create a garden dataset."""

from typing import cast

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

    # Create `conflict_type` column
    log.info("ucdp: add field `conflict_type`")
    df_geo = ds_meadow["geo"]
    df_conflict = ds_meadow["battle_related_conflict"]
    # Preserve only active conflicts
    df_geo = df_geo[df_geo["active_year"] == 1]
    df = add_conflict_type(df_geo, df_conflict)

    # Add `year_start`, which denotes the year when the conflict corresponding to the event started (only considering events with `active_year` == 1)
    df_start_year = df.groupby("conflict_new_id", as_index=False)[["year"]].min()
    df = df.merge(df_start_year, on="conflict_new_id", how="left", suffixes=("", "_start"))
    assert df.year_start.isna().sum() == 0, "Check NaNs in year_start!"

    # Get number of ongoing conflicts, and deaths in ongoing conflicts
    log.info("ucdp: get number of ongoing conflicts and deaths in ongoing conflicts")
    columns_idx = ["year", "region", "conflict_type"]
    df_ongoing = df.groupby(columns_idx).agg({"best": "sum", "conflict_new_id": "nunique"}).reset_index()
    df_ongoing.columns = columns_idx + ["number_deaths_ongoing_conflicts", "number_ongoing_conflicts"]

    # Get number of new conflicts every year
    log.info("ucdp: get number of new conflicts every year")
    columns_idx = ["year_start", "region", "conflict_type"]
    df_new = df.groupby(columns_idx)[["conflict_new_id"]].nunique().reset_index()
    df_new.columns = columns_idx + ["number_new_conflicts"]

    # Combine and build single table
    log.info("ucdp: combine and build single table")
    df = df_ongoing.merge(
        df_new, left_on=["year", "region", "conflict_type"], right_on=["year_start", "region", "conflict_type"]
    )
    df = df.drop(columns=["year_start"])

    # Add data for World
    log.info("ucdp: add data for World")
    df_world = df.groupby(["year", "conflict_type"], as_index=False)[
        ["number_deaths_ongoing_conflicts", "number_ongoing_conflicts", "number_new_conflicts"]
    ].sum()
    df_world["region"] = "World"
    df = pd.concat([df, df_world], ignore_index=True)

    # Add data for "all conflicts"
    log.info("ucdp: add data for 'all conflicts'")
    df_all = df.groupby(["year", "region"], as_index=False)[
        ["number_deaths_ongoing_conflicts", "number_ongoing_conflicts", "number_new_conflicts"]
    ].sum()
    df_all["conflict_type"] = "all"
    df = pd.concat([df, df_all], ignore_index=True)

    # SEt index, sort rows
    df = df.set_index(["year", "region", "conflict_type"]).sort_index()

    # Build table
    log.info("ucdp: build table")
    tb = Table(df, short_name=paths.short_name)

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
    df_geo = ds["geo"]
    df_conflict = ds["battle_related_conflict"]
    df_nonstate = ds["non_state"]
    df_onesided = ds["one_sided"]

    # Battle-related conflict #
    # Check IDs
    geo_ids = df_geo.loc[df_geo["type_of_violence"] == 1, ["conflict_new_id"]].drop_duplicates()
    conflict_ids = df_conflict[["conflict_id"]].drop_duplicates()
    res = geo_ids.merge(conflict_ids, left_on="conflict_new_id", right_on="conflict_id", how="outer")
    assert res.isna().sum().sum() == 0, "Check NaNs in conflict_new_id or conflict_id"
    # Check number of deaths
    geo_deaths = (
        df_geo.loc[(df_geo["type_of_violence"] == 1) & (df_geo["active_year"] == 1)]
        .groupby(["conflict_new_id", "year"], as_index=False)[["best"]]
        .sum()
        .sort_values(["conflict_new_id", "year"])
    )
    conflict_deaths = df_conflict[["conflict_id", "year", "bd_best"]].sort_values(["conflict_id", "year"])
    res = geo_deaths.merge(
        conflict_deaths, left_on=["conflict_new_id", "year"], right_on=["conflict_id", "year"], how="outer"
    )
    assert res.isna().sum().sum() == 0, "Check NaNs in conflict_new_id or conflict_id"
    assert (
        len(res[res["best"] - res["bd_best"] != 0]) <= 1
    ), "Dicrepancy between number of deaths in conflict (Geo vs. Non-state datasets)"

    # Non-state #
    # Check IDs
    geo_ids = df_geo.loc[df_geo["type_of_violence"] == 2, ["conflict_new_id"]].drop_duplicates()
    nonstate_ids = df_nonstate[["conflict_id"]].drop_duplicates()
    res = geo_ids.merge(nonstate_ids, left_on="conflict_new_id", right_on="conflict_id", how="outer")
    assert res.isna().sum().sum() == 0, "Check NaNs in conflict_new_id or conflict_id"
    # Check number of deaths
    geo_deaths = (
        df_geo.loc[(df_geo["type_of_violence"] == 2) & (df_geo["active_year"] == 1)]
        .groupby(["conflict_new_id", "year"], as_index=False)[["best"]]
        .sum()
        .sort_values(["conflict_new_id", "year"])
    )
    nonstate_deaths = df_nonstate[["conflict_id", "year", "best_fatality_estimate"]].sort_values(
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
    geo_ids = df_geo.loc[df_geo["type_of_violence"] == 3, ["conflict_new_id"]].drop_duplicates()
    onesided_ids = df_onesided[["conflict_id"]].drop_duplicates()
    res = geo_ids.merge(onesided_ids, left_on="conflict_new_id", right_on="conflict_id", how="outer")
    assert res.isna().sum().sum() == 0, "Check NaNs in conflict_new_id or conflict_id"
    # Check number of deaths
    geo_deaths = (
        df_geo.loc[(df_geo["type_of_violence"] == 3) & (df_geo["active_year"] == 1)]
        .groupby(["conflict_new_id", "year"], as_index=False)[["best"]]
        .sum()
        .sort_values(["conflict_new_id", "year"])
    )
    onesided_deaths = df_onesided[["conflict_id", "year", "best_fatality_estimate"]].sort_values(
        ["conflict_id", "year"]
    )
    res = geo_deaths.merge(
        onesided_deaths, left_on=["conflict_new_id", "year"], right_on=["conflict_id", "year"], how="outer"
    )
    assert res.isna().sum().sum() == 0, "Check NaNs in conflict_new_id or conflict_id"
    assert (
        len(res[res["best"] - res["best_fatality_estimate"] != 0]) <= 3
    ), "Dicrepancy between number of deaths in conflict (Geo vs. Non-state datasets)"


def add_conflict_type(df_geo: pd.DataFrame, df_conflict: pd.DataFrame) -> pd.DataFrame:
    """Add `conflict_type` to georeferenced dataset table.

    Values for conflict_type are:
       - non-state conflict
       - one-sided violence
       - extrasystemic
       - interstate
       - intrastate
       - internationalized intrastate
    """
    df_conflict_relevant = df_conflict[["conflict_id", "year", "type_of_conflict"]].drop_duplicates()
    assert (
        df_conflict_relevant.groupby(["conflict_id", "year"]).size().max() == 1
    ), "Some conflict_id-year pairs are duplicated!"
    # Add `type_of_conflict` to `df_geo`.
    # This column contains the type of state-based conflict (1: inter-state, 2: intra-state, 3: extra-state, 4: internationalized intrastate)
    df_geo = df_geo.merge(
        df_conflict_relevant, left_on=["conflict_new_id", "year"], right_on=["conflict_id", "year"], how="outer"
    )
    # Assert that `type_of_conflict` was only added for state-based events
    assert df_geo[df_geo["type_of_violence"] != 1].type_of_conflict.isna().all()

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
    df_geo["conflict_type"] = (
        df_geo["type_of_conflict"]
        .replace(type_of_conflict_mapping)
        .fillna(df_geo["type_of_violence"].replace(type_of_violence_mapping))
    )

    # Sanity check
    assert df_geo["conflict_type"].isna().sum() == 0, "Check NaNs in conflict_type (i.e. conflicts without a type)!"
    return df_geo
