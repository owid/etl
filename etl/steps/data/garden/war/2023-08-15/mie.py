"""Load a meadow dataset and create a garden dataset.

- This dataset only contains inter-state conflicts. We use the hostility level to differentiate different "types" of conflicts.

- The same conflict might be happening in different regions, with different hostility levels. This is important to consider when
estimating the global number of ongoing (or new) conflicts by broken down by hostility level

    - Such a conflict (occuring in mutliple regions at the same time with different hostility levels) has been coded using the
    most hostile category at global level.

- Each entry in this dataset describes a conflict (its participants and period). Therefore we need to "explode" it to add observations
for each year of the conflict.

- The number of deaths is not estimated for each hostile level, but rather only the aggregate is obtained.

"""

from typing import cast

import numpy as np
import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Dataset, Table
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
    ds_meadow = cast(Dataset, paths.load_dependency("mie"))

    # Read table from meadow dataset.
    tb = ds_meadow["mie"].reset_index()

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
    tb = expand_observations(tb)

    # estimate metrics
    log.info("war.mie: estimate metrics")
    tb = estimate_metrics(tb)

    # Rename hotility levels
    log.info("war.mie: rename hostility_level")
    tb["hostility_level"] = tb["hostility_level"].map(HOSTILITY_LEVEL_MAP | {"all": "all"})
    assert tb["hostility_level"].isna().sum() == 0, "Unmapped regions!"
    # Add suffix with source name
    msk = tb["region"] != "World"
    tb.loc[msk, "region"] = tb.loc[msk, "region"] + " (COW)"

    log.info("war.cow_mid: replace NaNs with zeros where applicable")
    tb = replace_missing_data_with_zeros(tb)

    # set index
    log.info("war.mie: set index")
    tb = tb.set_index(["year", "region", "hostility_level"], verify_integrity=True)

    # Add short_name to table
    log.info("war.mie: add shortname to table")
    tb = Table(tb, short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

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
    tb.loc[(tb["ccode"] >= 700) & (tb["ccode"] <= 899), "region"] = "Asia"
    tb.loc[(tb["ccode"] >= 900) & (tb["ccode"] <= 999), "region"] = "Oceania"

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


def expand_observations(tb: Table) -> Table:
    """Expand to have a row per (year, conflict).

    Parameters
    ----------
    tb : Table
        Original table, where each row is a conflict with its start and end year.

    Returns
    -------
    Table
        Here, each conflict has as many rows as years of activity. Its deaths have been uniformly distributed among the years of activity.
    """
    # For that we scale the number of deaths proportional to the duration of the conflict.
    tb[["fatalmax"]] = tb[["fatalmax"]].div(tb["endyear"] - tb["styear"] + 1, "index").round()
    tb[["fatalmin"]] = tb[["fatalmin"]].div(tb["endyear"] - tb["styear"] + 1, "index").round()

    # Add missing years for each triplet ("warcode", "campcode", "ccode")
    YEAR_MIN = tb["styear"].min()
    YEAR_MAX = tb["endyear"].max()
    tb_all_years = pd.DataFrame(pd.RangeIndex(YEAR_MIN, YEAR_MAX + 1), columns=["year"])
    df = pd.DataFrame(tb)  # to prevent error "AttributeError: 'DataFrame' object has no attribute 'all_columns'"
    df = df.merge(tb_all_years, how="cross")  # type: ignore
    tb = Table(df, metadata=tb.metadata)
    # Filter only entries that actually existed
    tb = tb[(tb["year"] >= tb["styear"]) & (tb["year"] <= tb["endyear"])]

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
    tb_new = pd.concat([tb_new, tb_new_alltypes, tb_new_world, tb_new_world_alltypes], ignore_index=True).sort_values(  # type: ignore
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
