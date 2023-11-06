"""History of War dataset built using PRIO v3.1 dataset.

In PRIO 3.1 dataset, each row is a year-observation of a certain conflict. That is, for a certain year, we have the number of fatalities that occured in a certain
conflict. There are a total of approx 1900 observations.

Death estimates are given in low, best and high estimates. While values for high and low estimates are always present, best estimates are sometimes missing (~800 observaionts).

Also, a conflict (i.e. one specific `id`) can have multiple campaigns. Take `id=1`, where we have three entries separated in time (i.e. three campaigns):

    - First campaign: 1946 (Bolivia and Popular Revolutionary Movement)
    - Second campaign: 1952 (Bolivia and MNR)
    - Third campaign: 1967 (Bolivia and ELN)


ON REGIONS:
    - PRIO uses the source encoding from UCDP. In particular, it uses the field `region` from UCDP/PRIO Armed Conflict Dataset.
    - From UCDP/PRIO, the regions are defined as follows:
        1 = Europe (GWNo: 200-399)
        2 = Middle East (GWNo: 630-699)
        3 = Asia (GWNo: 700-999)  [renamed to 'Asia and Oceania']
        4 = Africa (GWNo: 400-626)
        5 = Americas (GWNo: 2-199)
    - The source includes data for incompatibilities in Oceania in region Asia. Therefore, we have changed the region's name from "Asia" to "Asia and Oceania".
"""
import numpy as np
import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table
from shared import (
    add_indicators_extra,
    aggregate_conflict_types,
    get_number_of_countries_in_conflict_by_region,
)
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Logger
log = get_logger()
# Rename columns
REGIONS_RENAME = {
    1: "Europe",
    2: "Middle East",
    3: "Asia and Oceania",
    4: "Africa",
    5: "Americas",
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
    ds_meadow = paths.load_dataset("prio_v31")
    # Read table from meadow dataset.
    tb = ds_meadow["prio_v31"].reset_index()

    # Read table from GW codes
    ds_gw = paths.load_dataset("gleditsch")
    tb_regions = ds_gw["gleditsch_regions"].reset_index()
    tb_codes = ds_gw["gleditsch_countries"]

    #
    # Process data.
    #
    paths.log.info("rename columns")
    tb = tb.rename(
        columns={
            "type": "conflict_type",
        }
    )

    # Country-level stuff
    paths.log.info("getting country-level indicators")
    tb_country = estimate_metrics_country_level(tb, tb_codes)

    # Relevant rows
    paths.log.info("keep relevant columns")
    COLUMNS_RELEVANT = [
        "id",
        "year",
        "region",
        "conflict_type",
        "startdate",
        "ependdate",
        "bdeadlow",
        "bdeadhig",
        "bdeadbes",
    ]
    tb = tb[COLUMNS_RELEVANT]

    paths.log.info("sanity checks")
    _sanity_checks(tb)

    paths.log.info("replace NA in best estimate with lower bound")
    tb["bdeadbes"] = tb["bdeadbes"].fillna(tb["bdeadlow"])

    paths.log.info("estimate metrics")
    tb = estimate_metrics(tb)

    paths.log.info("replace NaNs with zeroes")
    tb = replace_missing_data_with_zeros(tb)

    # Rename regions
    log.info("war.cow: rename regions")
    tb["region"] = tb["region"].map(REGIONS_RENAME | {"World": "World"})
    assert tb["region"].isna().sum() == 0, "Unmapped regions!"

    # Add conflict rates
    paths.log.info("war.cow: map fatality codes to names")
    tb = add_indicators_extra(
        tb,
        tb_regions,
        columns_conflict_rate=["number_ongoing_conflicts", "number_new_conflicts"],
        columns_conflict_mortality=[
            "number_deaths_ongoing_conflicts_battle_high",
            "number_deaths_ongoing_conflicts_battle_low",
            "number_deaths_ongoing_conflicts_battle",
        ],
    )

    # Add suffix with source name
    msk = tb["region"] != "World"
    tb.loc[msk, "region"] = tb.loc[msk, "region"] + " (PRIO)"

    # Rename conflict_type
    paths.log.info("rename regions")
    tb["conflict_type"] = tb["conflict_type"].map(CONFTYPES_RENAME | {"all": "all", "intrastate": "intrastate"})
    assert tb["conflict_type"].isna().sum() == 0, "Unmapped conflict_type!"

    # sanity check: summing number of ongoing and new conflicts of all types is equivalent to conflict_type="all"
    paths.log.info("sanity checking number of conflicts")
    _sanity_check_final(tb)

    # Set index
    paths.log.info("set index")
    tb = tb.set_index(["year", "region", "conflict_type"], verify_integrity=True)
    tb_country = tb_country.set_index(["year", "country", "conflict_type"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    tables = [
        tb,
        tb_country,
    ]
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=tables, check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

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
        tb.groupby(["id", "year"])["conflict_type"].nunique() == 1
    ).all(), "Some conflicts has different values for `type` in the same year! That was not expected."


def estimate_metrics(tb: Table) -> Table:
    tb_ongoing = _add_ongoing_metrics(tb)
    tb_new = _add_new_metrics(tb)

    # Combine
    tb = tb_ongoing.merge(tb_new, on=["year", "region", "conflict_type"], how="outer")

    return tb


def _add_ongoing_metrics(tb: Table) -> Table:
    # Get ongoing metrics
    def sum_nan(x: pd.Series):
        if not x.isna().any():
            return x.sum()
        return np.nan

    ops = {"id": "nunique", "bdeadlow": sum_nan, "bdeadbes": sum_nan, "bdeadhig": sum_nan}
    ## By region and type
    tb_ongoing = tb.groupby(["year", "conflict_type", "region"], as_index=False).agg(ops)
    ## Type='all'
    tb_ongoing_alltype = tb.groupby(["year", "region"], as_index=False).agg(ops)
    tb_ongoing_alltype["conflict_type"] = "all"
    ## Type='intrastate'
    tb_intra_ = tb[tb["conflict_type"].isin([3, 4])].copy()
    tb_ongoing_intratype = tb_intra_.groupby(["year", "region"], as_index=False).agg(ops)
    tb_ongoing_intratype["conflict_type"] = "intrastate"
    ## World (by type)
    tb_ongoing_world = tb.groupby(["year", "conflict_type"], as_index=False).agg(ops)
    tb_ongoing_world["region"] = "World"
    ## World (type 'all')
    tb_ongoing_world_alltype = tb.groupby(["year"], as_index=False).agg(ops)
    tb_ongoing_world_alltype["region"] = "World"
    tb_ongoing_world_alltype["conflict_type"] = "all"
    ## World (type 'intrastate')
    tb_ongoing_world_intratype = tb_intra_.groupby(["year"], as_index=False).agg(ops)
    tb_ongoing_world_intratype["region"] = "World"
    tb_ongoing_world_intratype["conflict_type"] = "intrastate"

    ## Combine
    tb_ongoing = pr.concat(
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
    tb_ongoing = tb_ongoing.sort_values(["year", "region", "conflict_type"])  # type: ignore

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
        ["year", "region", "conflict_type", "id"]
    ]
    assert (
        tb_new["id"].value_counts().max() == 1
    ), "There are multiple instances of a conflict with the same ID. Maybe same conflict in different regions or with different types? This is assumed not to happen"

    # Estimate metric for regions and types
    tb_new_regions = tb_new.groupby(["year", "region", "conflict_type"], as_index=False)["id"].nunique()

    # Estimate metric for new type='all'
    tb_new_alltype = tb_new_regions.groupby(["year", "region"], as_index=False)["id"].sum()
    tb_new_alltype["conflict_type"] = "all"

    # Estimate metric for new type='intrastate'
    tb_new_regions_intra_ = tb_new_regions[tb_new_regions["conflict_type"].isin([3, 4])]
    tb_new_intratype = tb_new_regions_intra_.groupby(["year", "region"], as_index=False)["id"].sum()
    tb_new_intratype["conflict_type"] = "intrastate"

    tb_new = pr.concat([tb_new_regions, tb_new_alltype, tb_new_intratype], ignore_index=True)

    # Estimate metric for new region='World'
    tb_new_world = tb_new.groupby(["year", "conflict_type"], as_index=False)["id"].sum()
    tb_new_world["region"] = "World"

    # Combine
    tb_new = pr.concat([tb_new, tb_new_world], ignore_index=True)

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


def estimate_metrics_country_level(tb: Table, tb_codes: Table) -> Table:
    """Add country-level indicators."""
    ###################
    # Participated in #
    ###################

    # Get table with [year, conflict_type, code]
    codes = ["gwnoa", "gwnob"]
    tb_country = pr.concat([tb[["year", "conflict_type", code]].rename(columns={code: "id"}).copy() for code in codes])

    # Drop rows with code = NaN
    tb_country = tb_country.dropna(subset=["id"])
    tb_country = tb_country[~tb_country["id"].isin(["nan", "-99"])]
    # Drop duplicates
    tb_country = tb_country.drop_duplicates()

    # Explode where multiple codes
    tb_country["id"] = tb_country["id"].astype(str).str.split(",")
    tb_country = tb_country.explode("id")
    # Drop duplicates (may appear duplicates after exploding)
    tb_country = tb_country.drop_duplicates()
    # Ensure numeric type
    tb_country["id"] = tb_country["id"].str.strip().astype(int)

    # Sanity check
    assert not tb_country.isna().any(axis=None), "There are some NaNs!"

    # Add country name
    tb_country["country"] = tb_country.apply(lambda x: _get_country_name(tb_codes, x["id"], x["year"]), axis=1)
    assert tb_country["country"].notna().all(), "Some countries were not found! NaN was set"

    # Add flag
    tb_country["participated_in_conflict"] = 1
    tb_country["participated_in_conflict"].m.origins = tb["gwnoa"].m.origins

    # Prepare GW table
    tb_alltypes = Table(pd.DataFrame({"conflict_type": tb_country["conflict_type"].unique()}))
    tb_codes = tb_codes.reset_index().merge(tb_alltypes, how="cross")
    tb_codes["country"] = tb_codes["country"].astype(str)

    # Combine all GW entries with PRIO
    columns_idx = ["year", "country", "id", "conflict_type"]
    tb_country = tb_codes.merge(tb_country, on=columns_idx, how="outer")
    tb_country["participated_in_conflict"] = tb_country["participated_in_conflict"].fillna(0)
    tb_country = tb_country[columns_idx + ["participated_in_conflict"]]

    # Map conflict codes to labels
    tb_country["conflict_type"] = tb_country["conflict_type"].map(CONFTYPES_RENAME)
    assert tb_country["conflict_type"].isna().sum() == 0, "Unmapped conflict_type!"

    # Add intrastate (all)
    tb_country = aggregate_conflict_types(
        tb_country, "intrastate", ["intrastate (non-internationalized)", "intrastate (internationalized)"]
    )
    # Add state-based
    tb_country = aggregate_conflict_types(tb_country, "state-based", list(CONFTYPES_RENAME.values()))

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


def _get_country_name(tb_codes: Table, code: int, year: int) -> str:
    try:
        country_name = tb_codes.loc[(code, year)]
    except KeyError:
        country_name = tb_codes.loc[(code, year - 1)]
    return country_name
