"""History of War dataset built using PRIO v3.1 dataset.

In PRIO 3.1 dataset, each row is a year-observation of a certain conflict. That is, for a certain year, we have the number of fatalities that occured in a certain
conflict. There are a total of approx 1900 observations.

Death estimates are given in low, best and high estimates. While values for high and low estimates are always present, best estimates are sometimes missing (~800 observaionts).

Also, a conflict (i.e. one specific `id`) can have multiple campaigns. Take `id=1`, where we have three entries separated in time (i.e. three campaigns):

    - First campaign: 1946 (Bolivia and Popular Revolutionary Movement)
    - Second campaign: 1952 (Bolivia and MNR)
    - Third campaign: 1967 (Bolivia and ELN)

"""
from typing import cast

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Logger
log = get_logger()
# Rename columns
REGIONS_RENAME = {
    1: "Europe (PRIO)",
    2: "Middle East (PRIO)",
    3: "Asia (PRIO)",
    4: "Africa (PRIO)",
    5: "Americas (PRIO)",
}
CONFTYPES_RENAME = {
    1: "extrasystemic",
    2: "interstate",
    3: "intrastate (non-internationalized)",
    4: "intrastate (internationalized)",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("prio_v31"))

    # Read table from meadow dataset.
    tb = ds_meadow["prio_v31"].reset_index()

    #
    # Process data.
    #
    # Relevant rows
    log.info("war.prio_v31: keep relevant columns")
    COLUMNS_RELEVANT = ["id", "year", "region", "type", "startdate", "ependdate", "bdeadlow", "bdeadhig", "bdeadbes"]
    tb = tb[COLUMNS_RELEVANT]

    log.info("war.prio_v31: sanity checks")
    _sanity_checks(tb)

    log.info("war.prio_v31: replace NA in best estimate with lower bound")
    tb["bdeadbes"] = tb["bdeadbes"].fillna(tb["bdeadlow"])

    log.info("war.prio_v31: estimate metrics")
    tb = estimate_metrics(tb)

    log.info("war.prio_v31: rename columns")
    tb = tb.rename(
        columns={
            "type": "conflict_type",
        }
    )

    log.info("war.prio_v31: replace NaNs with zeroes")
    tb = replace_missing_data_with_zeros(tb)

    # Rename regions
    log.info("war.prio_v31: rename regions")
    tb["region"] = tb["region"].map(REGIONS_RENAME | {"World": "World"})
    assert tb["region"].isna().sum() == 0, "Unmapped regions!"

    # Rename conflict_type
    log.info("war.prio_v31: rename regions")
    tb["conflict_type"] = tb["conflict_type"].map(CONFTYPES_RENAME | {"all": "all", "intrastate": "intrastate"})
    assert tb["conflict_type"].isna().sum() == 0, "Unmapped conflict_type!"

    # sanity check: summing number of ongoing and new conflicts of all types is equivalent to conflict_type="all"
    log.info("war.prio_v31: sanity checking number of conflicts")
    _sanity_check_final(tb)

    log.info("war.prio_v31: set index")
    tb = tb.set_index(["year", "region", "conflict_type"], verify_integrity=True)

    log.info("war.prio_v31: add shortname to table")
    tb = Table(tb, short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def _sanity_checks(tb: Table) -> None:
    # Low and High estimates
    columns = ["bdeadlow", "bdeadhig"]
    for column in columns:
        assert (
            tb[column].isna().sum() == 0
        ), f"Missing values found in {column}. Consequently, we can't set to zero this field in `replace_missing_data_with_zeros`!"
        assert not set(
            tb.loc[tb[column] < 0, column]
        ), f"Negative values found in {column}. Consequently, we can't set to zero this field in `replace_missing_data_with_zeros`!"

    # Best estimate
    column = "bdeadbes"
    assert tb[column].isna().sum() == 0, f"Missing values found in {column}"
    assert not set(tb.loc[tb[column] < 0, column]) - {-999}, f"Negative values other than '-999' found in {column}"
    # Replace -999 with NaN
    tb[column] = tb[column].replace(-999, np.nan)

    # Check regions
    assert (
        tb.groupby("id")["region"].nunique() == 1
    ).all(), "Some conflicts occurs in multiple regions! That was not expected."
    assert (
        tb.groupby(["id", "year"])["type"].nunique() == 1
    ).all(), "Some conflicts has different values for `type` in the same year! That was not expected."


def estimate_metrics(tb: Table) -> Table:
    tb_ongoing = _add_ongoing_metrics(tb)
    tb_new = _add_new_metrics(tb)

    # Combine
    tb = tb_ongoing.merge(tb_new, on=["year", "region", "type"], how="outer")

    return tb


def _add_ongoing_metrics(tb: Table) -> Table:
    # Get ongoing metrics
    def sum_nan(x: pd.Series):
        if not x.isna().any():
            return x.sum()
        return np.nan

    ops = {"id": "nunique", "bdeadlow": sum_nan, "bdeadbes": sum_nan, "bdeadhig": sum_nan}
    ## By region and type
    tb_ongoing = tb.groupby(["year", "type", "region"], as_index=False).agg(ops)
    ## Type='all'
    tb_ongoing_alltype = tb.groupby(["year", "region"], as_index=False).agg(ops)
    tb_ongoing_alltype["type"] = "all"
    ## Type='intrastate'
    tb_intra_ = tb[tb["type"].isin([3, 4])].copy()
    tb_ongoing_intratype = tb_intra_.groupby(["year", "region"], as_index=False).agg(ops)
    tb_ongoing_intratype["type"] = "intrastate"
    ## World (by type)
    tb_ongoing_world = tb.groupby(["year", "type"], as_index=False).agg(ops)
    tb_ongoing_world["region"] = "World"
    ## World (type 'all')
    tb_ongoing_world_alltype = tb.groupby(["year"], as_index=False).agg(ops)
    tb_ongoing_world_alltype["region"] = "World"
    tb_ongoing_world_alltype["type"] = "all"
    ## World (type 'intrastate')
    tb_ongoing_world_intratype = tb_intra_.groupby(["year"], as_index=False).agg(ops)
    tb_ongoing_world_intratype["region"] = "World"
    tb_ongoing_world_intratype["type"] = "intrastate"

    ## Combine
    tb_ongoing = pd.concat(
        [
            tb_ongoing,
            tb_ongoing_alltype,
            tb_ongoing_intratype,
            tb_ongoing_world,
            tb_ongoing_world_alltype,
            tb_ongoing_world_intratype,
        ],
        ignore_index=True,
    )
    tb_ongoing = tb_ongoing.sort_values(["year", "region", "type"])  # type: ignore

    ## Rename
    tb_ongoing = tb_ongoing.rename(  # type: ignore
        columns={
            "id": "number_ongoing_conflicts",
            "bdeadlow": "number_deaths_ongoing_conflicts_battle_low",
            "bdeadhig": "number_deaths_ongoing_conflicts_battle_high",
            "bdeadbes": "number_deaths_ongoing_conflicts_battle",
        }
    )

    return tb_ongoing


def _add_new_metrics(tb: Table) -> Table:
    # Reduce table
    tb_new = tb.sort_values("year").drop_duplicates(subset=["id", "region"], keep="first")[
        ["year", "region", "type", "id"]
    ]
    assert (
        tb_new["id"].value_counts().max() == 1
    ), "There are multiple instances of a conflict with the same ID. Maybe same conflict in different regions or with different types? This is assumed not to happen"

    # Estimate metric for regions and types
    tb_new_regions = tb_new.groupby(["year", "region", "type"], as_index=False)["id"].nunique()

    # Estimate metric for new type='all'
    tb_new_alltype = tb_new_regions.groupby(["year", "region"], as_index=False)["id"].sum()
    tb_new_alltype["type"] = "all"

    # Estimate metric for new type='intrastate'
    tb_new_regions_intra_ = tb_new_regions[tb_new_regions["type"].isin([3, 4])]
    tb_new_intratype = tb_new_regions_intra_.groupby(["year", "region"], as_index=False)["id"].sum()
    tb_new_intratype["type"] = "intrastate"

    tb_new = pd.concat([tb_new_regions, tb_new_alltype, tb_new_intratype], ignore_index=True)

    # Estimate metric for new region='World'
    tb_new_world = tb_new.groupby(["year", "type"], as_index=False)["id"].sum()
    tb_new_world["region"] = "World"

    # Combine
    tb_new = pd.concat([tb_new, tb_new_world], ignore_index=True)

    # Rename
    tb_new = tb_new.rename(columns={"id": "number_new_conflicts"})  # type: ignore

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
    tb = tb.set_index(["year", "region", "conflict_type"]).reindex(new_idx).reset_index()

    # Change NaNs for 0 for specific rows
    ## For columns "number_ongoing_conflicts", "number_new_conflicts"
    columns = [
        "number_ongoing_conflicts",
        "number_new_conflicts",
        "number_deaths_ongoing_conflicts_battle_high",
        "number_deaths_ongoing_conflicts_battle_low",
    ]
    tb.loc[:, columns] = tb.loc[:, columns].fillna(0)

    # Set number of deaths to zero whenever high and low estimates are zero
    tb.loc[
        (tb["number_deaths_ongoing_conflicts_battle_high"] == 0)
        & (tb["number_deaths_ongoing_conflicts_battle_low"] == 0),
        "number_deaths_ongoing_conflicts_battle",
    ] = 0
    return tb


def _sanity_check_final(tb: Table) -> Table:
    # 1) Check that number of conflicts by type makes sense with aggregate. That is `ALL = INTRA + INTER + ...`
    ## Preserve all conflict types (without overlap)
    conflict_types_exclude = ["all", "intrastate"]
    mask = ~tb["conflict_type"].isin(conflict_types_exclude)
    ## Sum metrics
    tb_check = (
        tb.loc[mask]
        .groupby(["year", "region"], as_index=False)[["number_ongoing_conflicts", "number_new_conflicts"]]
        .sum()
    )
    ## Compare with conflict_type="all"
    tb_all = tb[tb["conflict_type"] == "all"]
    tb_ = tb_all.merge(tb_check, on=["year", "region"], suffixes=("_all", "_check"), validate="one_to_one")
    ## Assertions
    assert (
        tb_["number_ongoing_conflicts_all"] - tb_["number_ongoing_conflicts_check"] == 0
    ).all(), (
        "Number of ongoing conflicts for conflict_type='all' is not equivalent to the sum of individual conflict types"
    )
    assert (
        tb_["number_new_conflicts_all"] - tb_["number_new_conflicts_check"] == 0
    ).all(), "Number of new conflicts for conflict_type='all' is not equivalent to the sum of individual conflict types"

    # 2)
    msk = tb["number_deaths_ongoing_conflicts_battle_low"] > tb["number_deaths_ongoing_conflicts_battle_high"]
    assert not msk.any(), f"Low estimates higher than high estimates. This can't be correct! {tb[msk]}"
