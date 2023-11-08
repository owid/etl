"""Load a meadow dataset and create a garden dataset.

- This dataset only contains inter-state conflicts. We use the hostility level to differentiate different "types" of conflicts.

- The same conflict might be happening in different regions, with different hostility levels. This is important to consider when
estimating the global number of ongoing (or new) conflicts by broken down by hostility level

    - Such a conflict (occuring in mutliple regions at the same time with different hostility levels) has been coded using the
    most hostile category at global level.

- Each entry in this dataset describes a conflict (its participants and period). Therefore we need to "explode" it to add observations
for each year, participand of the conflict.
    - One entry provides deaths for side 1 (ccode1) and side 2 (ccode2).

- The number of deaths is not estimated for each hostile level, but rather only the aggregate is obtained.


ON regions:
    - We use the region of the participants to assign a region to each conflict, and not the region of the conflict itself. We use
    the country code (ccode) to assign a region to each participant. Same as in COW MID (we used `ccode` from MIDB).
    - We encode the region using the codes from COW (based on GW).
    - It uses the codes from Correlates of War (https://correlatesofwar.org/data-sets/state-system-membership/, file "states2016.csv"). We encode regions as follows:

        Americas: 2-165
        Europe: 200-399
        Africa: 402-626
        Middle East: 630-698
        Asia and Oceania: 700-999

"""

import numpy as np
import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table
from shared import (
    add_indicators_extra,
    aggregate_conflict_types,
    expand_observations,
    get_number_of_countries_in_conflict_by_region,
)
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Logger
log = get_logger()
# Mapping from hostility level code to name
HOSTILITY_LEVEL_MAP = {
    1: "No militarized action",
    2: "Threat to use force",
    3: "Display of force",
    4: "Use of force",
    5: "War",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("mie")
    # Read table from meadow dataset.
    tb = ds_meadow["mie"].reset_index()

    # Read table from COW codes
    ds_cow_ssm = paths.load_dataset("cow_ssm")
    tb_regions = ds_cow_ssm["cow_ssm_regions"].reset_index()
    tb_codes = ds_cow_ssm["cow_ssm_countries"]

    #
    # Process data.
    #
    log.info("war.mie: sanity checks")
    _sanity_checks(tb)

    log.info("war.mie: keep relevant columns")
    COLUMNS_RELEVANT = [
        "micnum",
        "eventnum",
        "ccode1",
        "ccode2",
        "styear",
        "endyear",
        "hostlev",
        "fatalmin1",
        "fatalmax1",
        "fatalmin2",
        "fatalmax2",
    ]
    tb = tb[COLUMNS_RELEVANT]

    log.info("war.mie: rename columns")
    tb = reshape_table(tb)

    tb_country = estimate_metrics_country_level(tb, tb_codes)

    # Checks
    assert not tb["fatalmin"].isna().any(), "Nulls found in fatalmin!"
    assert not tb["fatalmax"].isna().any(), "Nulls found in fatalmax!"

    log.info("war.mie: add regions")
    tb = add_regions(tb)

    log.info("war.mie: rename column")
    tb = tb.rename(columns={"hostlev": "hostility_level"})

    log.info("war.mie: aggregate entries")
    tb = tb.groupby(["micnum", "region", "hostility_level", "styear", "endyear"], as_index=False).agg(
        {"fatalmin": "sum", "fatalmax": "sum"}
    )

    log.info("war.mie: expand observations")
    # tb = expand_observations(tb)
    tb = expand_observations(
        tb,
        col_year_start="styear",
        col_year_end="endyear",
        cols_scale=["fatalmax", "fatalmin"],
    )

    # estimate metrics
    log.info("war.mie: estimate metrics")
    tb = estimate_metrics(tb)

    # Rename hotility levels
    log.info("war.mie: rename hostility_level")
    tb["hostility_level"] = tb["hostility_level"].map(HOSTILITY_LEVEL_MAP | {"all": "all"})
    assert tb["hostility_level"].isna().sum() == 0, "Unmapped regions!"

    log.info("war.cow_mid: replace NaNs with zeros where applicable")
    tb = replace_missing_data_with_zeros(tb)

    # Add normalised indicators
    log.info("war.mie: add normalised indicators")
    tb = add_indicators_extra(
        tb,
        tb_regions,
        columns_conflict_rate=["number_ongoing_conflicts", "number_new_conflicts"],
        columns_conflict_mortality=["number_deaths_ongoing_conflicts_low", "number_deaths_ongoing_conflicts_high"],
    )

    # Add suffix with source name
    msk = tb["region"] != "World"
    tb.loc[msk, "region"] = tb.loc[msk, "region"] + " (COW)"

    # set index
    log.info("war.mie: set index")
    tb = tb.set_index(["year", "region", "hostility_level"], verify_integrity=True)

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


def _sanity_checks(tb: Table) -> Table:
    # sanity checks
    assert tb.groupby(["micnum", "eventnum"]).size().max() == 1, "More than one event for a given (micnum, eventnum)"
    assert set(tb["hostlev"]) == {2, 3, 4, 5}, "Set of hostlev values should be {1, 2, 3, 4, 5}."
    assert ((tb["ccode1"] > 1) & (tb["ccode1"] < 1000)).all(), "ccode should be between 1 and 1000."
    assert ((tb["ccode2"] > 1) & (tb["ccode2"] < 1000)).all(), "ccode should be between 1 and 1000."


def add_regions(tb: Table) -> Table:
    """Assign region to each conflict-country pair.

    The region is assigned based on the country code (ccode) of the participant.
    """
    ## COW uses custom country codes, so we need the following custom mapping.
    tb.loc[(tb["ccode"] >= 1) & (tb["ccode"] < 200), "region"] = "Americas"
    tb.loc[(tb["ccode"] >= 200) & (tb["ccode"] < 400), "region"] = "Europe"
    tb.loc[(tb["ccode"] >= 400) & (tb["ccode"] < 627), "region"] = "Africa"
    tb.loc[(tb["ccode"] >= 630) & (tb["ccode"] < 699), "region"] = "Middle East"
    tb.loc[(tb["ccode"] >= 700) & (tb["ccode"] <= 999), "region"] = "Asia and Oceania"
    # tb.loc[(tb["ccode"] >= 700) & (tb["ccode"] <= 899), "region"] = "Asia"
    # tb.loc[(tb["ccode"] >= 900) & (tb["ccode"] <= 999), "region"] = "Oceania"

    return tb


def reshape_table(tb: Table) -> Table:
    """Reshape table so that each row contains an event-country pair.

    By default, each row in the table contains information at event level. This method disaggregates this
    into event-country granularity.
    """
    tbs = []
    for i in [1, 2]:
        tb_ = tb[["micnum", "eventnum", f"ccode{i}", "hostlev", "styear", "endyear", f"fatalmin{i}", f"fatalmax{i}"]]
        tb_ = tb_.rename(
            columns={
                f"ccode{i}": "ccode",
                f"fatalmin{i}": "fatalmin",
                f"fatalmax{i}": "fatalmax",
            }
        )
        tbs.append(tb_)

    tb = pr.concat(tbs, ignore_index=True)

    return tb


def estimate_metrics(tb: Table) -> Table:
    """Remix table to have the desired metrics.

    These metrics are:
        - number_ongoing_conflicts
        - number_new_conflicts
        - number_deaths_ongoing_conflicts_low
        - number_deaths_ongoing_conflicts_high

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
    tb_ongoing = _add_ongoing_metrics(tb)
    tb_new = _add_new_metrics(tb)

    # Combine
    columns_idx = ["year", "region", "hostility_level"]
    tb = tb_ongoing.merge(tb_new, on=columns_idx, how="outer").sort_values(columns_idx)

    return tb


def _add_ongoing_metrics(tb: Table) -> Table:
    ## Aggregate by (conflict, region, year)
    ## The same conflict may appear more than once in the same region and year with a different hostility_level.
    ## We want a single hostility_level per conflict, region and year. Therefore, we assign the highest hostility level.
    tb = tb.groupby(["micnum", "region", "year"], as_index=False).agg(
        {"hostility_level": "max", "fatalmin": "sum", "fatalmax": "sum"}
    )

    ops = {"micnum": "nunique"}
    ## By region and hostility_level
    tb_ongoing = tb.groupby(["year", "region", "hostility_level"], as_index=False).agg(ops)
    ## region='World' and by hostility_level
    tb_ = tb.groupby(["micnum", "year"], as_index=False).agg({"hostility_level": max})
    tb_ongoing_world = tb_.groupby(["year", "hostility_level"], as_index=False).agg(ops)
    tb_ongoing_world["region"] = "World"

    ops = {"micnum": "nunique", "fatalmin": "sum", "fatalmax": "sum"}
    ## By region and hostility_level='all'
    tb_ongoing_alltypes = tb.groupby(["year", "region"], as_index=False).agg(ops)
    tb_ongoing_alltypes["hostility_level"] = "all"
    ## region='World' and hostility_level='all'
    tb_ongoing_world_alltypes = tb.groupby(["year"], as_index=False).agg(ops)
    tb_ongoing_world_alltypes["region"] = "World"
    tb_ongoing_world_alltypes["hostility_level"] = "all"

    ## Combine tables
    tb_ongoing = pr.concat([tb_ongoing, tb_ongoing_world, tb_ongoing_alltypes, tb_ongoing_world_alltypes], ignore_index=True).sort_values(  # type: ignore
        by=["year", "region", "hostility_level"]
    )

    ## Rename columns
    tb_ongoing = tb_ongoing.rename(  # type: ignore
        columns={
            "micnum": "number_ongoing_conflicts",
            "fatalmin": "number_deaths_ongoing_conflicts_low",
            "fatalmax": "number_deaths_ongoing_conflicts_high",
        }
    )

    return tb_ongoing


def _add_new_metrics(tb: Table) -> Table:
    # Operations
    ops = {"micnum": "nunique"}

    # Regions
    ## Keep one row per (micnum, region).
    tb_ = tb.groupby(["micnum", "region", "styear"], as_index=False).agg({"hostility_level": max})
    tb_ = tb_.sort_values("styear").drop_duplicates(subset=["micnum", "region"], keep="first")
    ## By region and hostility_level
    tb_new = tb_.groupby(["styear", "region", "hostility_level"], as_index=False).agg(ops)
    ## By region and hostility_level='all'
    tb_new_alltypes = tb_.groupby(["styear", "region"], as_index=False).agg(ops)
    tb_new_alltypes["hostility_level"] = "all"

    # World
    ## Keep one row per (micnum). Otherwise, we might count the same conflict in multiple years!
    tb_ = tb.groupby(["micnum", "styear"], as_index=False).agg({"hostility_level": max})
    tb_ = tb_.sort_values("styear").drop_duplicates(subset=["micnum"], keep="first")
    ## region='World' and by hostility_level
    tb_new_world = tb_.groupby(["styear", "hostility_level"], as_index=False).agg(ops)
    tb_new_world["region"] = "World"
    ## World and hostility_level='all'
    tb_new_world_alltypes = tb_.groupby(["styear"], as_index=False).agg(ops)
    tb_new_world_alltypes["region"] = "World"
    tb_new_world_alltypes["hostility_level"] = "all"

    # Combine
    tb_new = pr.concat([tb_new, tb_new_alltypes, tb_new_world, tb_new_world_alltypes], ignore_index=True).sort_values(  # type: ignore
        by=["styear", "region", "hostility_level"]
    )

    # Rename columns
    tb_new = tb_new.rename(  # type: ignore
        columns={
            "styear": "year",
            "micnum": "number_new_conflicts",
        }
    )

    return tb_new


def replace_missing_data_with_zeros(tb: Table) -> Table:
    """Replace missing data with zeros.
    In some instances there is missing data. Instead, we'd like this to be zero-valued.
    """
    # Add missing (year, region, hostility_type) entries (filled with NaNs)
    years = np.arange(tb["year"].min(), tb["year"].max() + 1)
    regions = set(tb["region"])
    hostility_types = set(tb["hostility_level"])
    new_idx = pd.MultiIndex.from_product([years, regions, hostility_types], names=["year", "region", "hostility_level"])
    tb = tb.set_index(["year", "region", "hostility_level"], verify_integrity=True).reindex(new_idx).reset_index()

    # Change NaNs for 0 for specific rows
    ## For columns "number_ongoing_conflicts", "number_new_conflicts"
    columns = [
        "number_ongoing_conflicts",
        "number_new_conflicts",
    ]
    tb.loc[:, columns] = tb.loc[:, columns].fillna(0)

    # We are only reporting number of deaths for hostility_level='all'
    ## for hostility_level != 'all' we don't mind having NaNs
    mask = tb["hostility_level"] == "all"
    tb.loc[mask, "number_deaths_ongoing_conflicts_low"] = tb.loc[mask, "number_deaths_ongoing_conflicts_low"].fillna(0)
    tb.loc[mask, "number_deaths_ongoing_conflicts_high"] = tb.loc[mask, "number_deaths_ongoing_conflicts_high"].fillna(
        0
    )

    # Drop all-NaN rows
    tb = tb.dropna(subset=columns, how="all")
    return tb


def estimate_metrics_country_level(tb: Table, tb_codes: Table) -> Table:
    """Add country-level indicators."""
    ###################
    # Participated in #
    ###################

    # Get table with [year, conflict_type, code]
    tb_country = tb[["styear", "endyear", "ccode", "hostlev"]].copy()
    # Rename
    tb_country = tb_country.rename(columns={"ccode": "id"})

    # Drop rows with code = NaN, drop duplicates
    tb_country = tb_country.dropna(subset=["id"]).drop_duplicates()

    # Expand observations
    tb_country = expand_observations(
        tb_country,
        col_year_start="styear",
        col_year_end="endyear",
    )

    # Remove not relevant columns, drop duplicates
    tb_country = tb_country.drop(columns=["styear", "endyear"]).drop_duplicates()
    # Ensure numeric type
    tb_country["id"] = tb_country["id"].astype(int)

    # Sanity check
    assert not tb_country.isna().any(axis=None), "There are some NaNs!"

    ##
    # Alternative Flow
    # columns = ["id", "year"]
    # tb_codes = tb_codes.reset_index()
    # tb_country["participated_in_conflict"] = 1
    # tb_country[columns] = tb_country[columns].astype(int)
    # tb_codes[columns] = tb_codes[columns].astype(int)
    # tb_country = tb_country.merge(tb_codes, how="outer")

    # ## Find missmatches
    # ids_missed = set(tb_country.loc[(tb_country["participated_in_conflict"] == 1) & (tb_country["country"].isna()), "id"])
    # tb_codes_missed = tb_codes[tb_codes["id"].isin(ids_missed)]
    # countries_per_id = tb_codes_missed.groupby("id")["country"].nunique()
    # errors = countries_per_id[countries_per_id != 1]
    # if not errors.empty:
    #     raise ValueError(f"Found some errors in the mapping (formatr is 'code -> number of countries with this code': {errors}")
    ##

    # Add country name
    tb_country["country"] = tb_country.apply(lambda x: _get_country_name(tb_codes, x["id"], x["year"]), axis=1)
    assert tb_country["country"].notna().all(), "Some countries were not found! NaN was set"

    # Add participation flag
    tb_country["participated_in_conflict"] = 1
    tb_country["participated_in_conflict"].m.origins = tb["ccode"].m.origins

    # Prepare codes table
    tb_alltypes = Table(pd.DataFrame({"hostlev": tb_country["hostlev"].unique()}))
    tb_codes = tb_codes.reset_index().merge(tb_alltypes, how="cross")
    tb_codes["country"] = tb_codes["country"].astype(str)

    # Combine all codes entries with MIE table
    columns_idx = ["year", "country", "id", "hostlev"]
    tb_country = tb_codes.merge(tb_country, on=columns_idx, how="outer")
    tb_country["participated_in_conflict"] = tb_country["participated_in_conflict"].fillna(0)
    tb_country = tb_country[columns_idx + ["participated_in_conflict"]]

    # Add "all" hostility level
    tb_country = aggregate_conflict_types(tb_country, "all", dim_name="hostlev")

    # Only preserve years that make sense
    tb_country = tb_country[(tb_country["year"] >= tb["styear"].min()) & (tb_country["year"] <= tb["endyear"].max())]

    # Replace hostlev codes with names
    tb_country["hostlev"] = tb_country["hostlev"].map(HOSTILITY_LEVEL_MAP | {"all": "all"})

    ###################
    # Participated in #
    ###################
    # NUMBER COUNTRIES
    tb_num_participants = get_number_of_countries_in_conflict_by_region(tb_country, "hostlev", "cow")

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
    tb_country = tb_country.set_index(["year", "hostlev", "country"], verify_integrity=True)
    return tb_country


def _get_country_name(tb_codes: Table, code: int, year: int) -> str:
    try:
        country_name = tb_codes.loc[(code, year)].item()
    except KeyError:
        if (code == 20) and (year in [1918, 1919]):
            country_name = "Canada"
        else:
            raise ValueError(f"Unknown country with code {code} for year {year}")
    return country_name
