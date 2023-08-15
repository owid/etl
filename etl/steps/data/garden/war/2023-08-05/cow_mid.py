"""COW Militarised Inter-state Dispute dataset.


- This dataset only contains inter-state conflicts. We use the hostility level to differentiate different "types" of conflicts.

- The same conflict might be happening in different regions, with different hostility levels. This is important to consider when
estimating the global number of ongoing (or new) conflicts by broken down by hostility level

    - Such a conflict (occuring in mutliple regions at the same time with different hostility levels) has been coded using the
    most hostile category at global level.

- Each entry in this dataset describes a conflict (its participants and period). Therefore we need to "explode" it to add observations
for each year of the conflict.
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
# Log
log = get_logger()
# Mapping from hostility level code to name
HOSTILITY_LEVEL_MAP = {
    1: "No militarized action",
    2: "Threat to use force",
    3: "Display of force",
    4: "Use of force",
    5: "War",
}


def load_countries_regions() -> Table:
    """Load countries-regions table from reference dataset (e.g. to map from iso codes to country names)."""
    ds_reference = cast(Dataset, paths.load_dependency("regions"))
    tb_countries_regions = ds_reference["regions"]

    return tb_countries_regions


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("cow_mid"))

    # Read table from meadow dataset.
    tb = ds_meadow["midb"]

    #
    # Process data.
    #
    log.info("war.cow_mid: complete number of deaths with lower bounds")
    tb = complete_num_deaths(tb)

    log.info("war.cow_mid: sanity checks")
    _sanity_checks(tb)

    log.info("war.cow_mid: keep relevant columns")
    COLUMNS_RELEVANT = [
        "dispnum",
        "ccode",
        "stabb",
        "hostlev",
        "styear",
        "endyear",
        "fatalpre",
    ]
    tb = tb[COLUMNS_RELEVANT]

    log.info("war.cow_mid: add regions")
    tb = add_regions(tb)

    log.info("war.cow_mid: rename column")
    tb = tb.rename(columns={"hostlev": "hostility_level"})

    log.info("war.cow_mid: aggregate entries")
    tb = tb.groupby(["dispnum", "region", "styear", "endyear"], as_index=False).agg(
        {"fatalpre": "sum", "hostility_level": "max"}
    )

    log.info("war.cow_mid: expand observations")
    tb = expand_observations(tb)

    # Estimate metrics
    log.info("war.cow_mid: estimate metrics")
    tb = estimate_metrics(tb)

    # Rename hotility levels
    log.info("war.cow_mid: rename hostility_level")
    tb["hostility_level"] = tb["hostility_level"].map(HOSTILITY_LEVEL_MAP | {"all": "all"})
    assert tb["hostility_level"].isna().sum() == 0, "Unmapped regions!"
    # Add suffix with source name
    msk = tb["region"] != "World"
    tb.loc[msk, "region"] = tb.loc[msk, "region"] + " (COW)"

    log.info("war.cow_mid: replace NaNs with zeros where applicable")
    tb = replace_missing_data_with_zeros(tb)

    # Set index
    log.info("war.cow_mid: set index")
    tb = tb.set_index(["year", "region", "hostility_level"], verify_integrity=True)

    # Add short_name to table
    log.info("war.cow_mid: add shortname to table")
    tb = Table(tb, short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def complete_num_deaths(tb: Table) -> Table:
    """Complement `fatalpre` with information from `fatality`.

    `fatalpre` gives the precise number of deaths. However, this value is sometimes missing. In these cases, we can
    use the information from `fatality` to get a lower bound for the number of deaths.

    When information is missing in both fields, we set the value to 0.
    """
    # Reduce NaNs in `fatalpre`
    fatality_lower_bound = {
        0: 0,
        1: 1,
        2: 26,
        3: 101,
        4: 251,
        5: 501,
        6: 1000,
        -9: np.nan,
    }
    mask = tb["fatalpre"] == -9
    tb.loc[mask, "fatalpre"] = tb.loc[mask, "fatality"].map(fatality_lower_bound)

    assert tb["fatalpre"].isna().sum() == 599, "Different number of NaNs found. Expected was 599."
    tb["fatalpre"] = tb["fatalpre"].fillna(0)

    return tb


def _sanity_checks(tb: Table) -> Table:
    # sanity checks
    assert tb.groupby(["dispnum", "ccode", "styear", "endyear"]).size().max() == 1
    assert set(tb["hostlev"]) == {1, 2, 3, 4, 5}, "Set of hostlev values should be {1, 2, 3, 4, 5}."
    assert ((tb["ccode"] > 1) & (tb["ccode"] < 1000)).all(), "ccode should be between 1 and 1000."


def add_regions(tb: Table) -> Table:
    """Assign region to each dispute-country pair.

    The region is assigned based on the country code (ccode) of the participant.
    """
    ## COW uses custom country codes, so we need the following custom mapping.
    tb.loc[(tb["ccode"] >= 1) & (tb["ccode"] < 200), "region"] = "Americas"
    tb.loc[(tb["ccode"] >= 200) & (tb["ccode"] < 400), "region"] = "Europe"
    tb.loc[(tb["ccode"] >= 400) & (tb["ccode"] < 627), "region"] = "Africa"
    tb.loc[(tb["ccode"] >= 630) & (tb["ccode"] < 699), "region"] = "Middle East"
    tb.loc[(tb["ccode"] >= 700) & (tb["ccode"] < 1000), "region"] = "Asia"

    return tb


def expand_observations(tb: Table) -> Table:
    """Expand to have a row per (year, dispute).

    Parameters
    ----------
    tb : Table
        Original table, where each row is a dispute with its start and end year.

    Returns
    -------
    Table
        Here, each dispute has as many rows as years of activity. Its deaths have been uniformly distributed among the years of activity.
    """
    # For that we scale the number of deaths proportional to the duration of the dispute.
    tb[["fatalpre"]] = tb[["fatalpre"]].div(tb["endyear"] - tb["styear"] + 1, "index").round()

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
        - number_ongoing_disputes
        - number_new_disputes
        - number_deaths_ongoing_disputes

    Parameters
    ----------
    tb : Table
        Table with a row per dispute and year of observation.

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
    ## Aggregate by (dispute, region, year)
    ## The same dispute may appear more than once in the same region and year with a different hostility_level.
    ## We want a single hostility_level per dispute, region and year. Therefore, we assign the highest hostility level.
    tb = tb.groupby(["dispnum", "region", "year"], as_index=False).agg({"hostility_level": "max", "fatalpre": "sum"})

    ops = {"dispnum": "nunique"}
    ## By region and hostility_level
    tb_ongoing = tb.groupby(["year", "region", "hostility_level"], as_index=False).agg(ops)
    ## region='World' and by hostility_level
    tb_ = tb.groupby(["dispnum", "year"], as_index=False).agg({"hostility_level": max})
    tb_ongoing_world = tb_.groupby(["year", "hostility_level"], as_index=False).agg(ops)
    tb_ongoing_world["region"] = "World"

    ops = {"dispnum": "nunique", "fatalpre": sum}
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
            "dispnum": "number_ongoing_disputes",
            "fatalpre": "number_deaths_ongoing_disputes",
        }
    )

    return tb_ongoing


def _add_new_metrics(tb: Table) -> Table:
    # Operations
    ops = {"dispnum": "nunique"}

    # Regions
    ## Keep one row per (dispnum, region).
    tb_ = tb.sort_values("styear").drop_duplicates(subset=["dispnum", "region"], keep="first")
    ## By region and hostility_level
    tb_new = tb_.groupby(["styear", "region", "hostility_level"], as_index=False).agg(ops)
    ## By region and hostility_level='all'
    tb_new_alltypes = tb_.groupby(["styear", "region"], as_index=False).agg(ops)
    tb_new_alltypes["hostility_level"] = "all"

    # World
    ## Keep one row per (dispnum). Otherwise, we might count the same dispute in multiple years!
    tb_ = tb.groupby(["dispnum", "styear"], as_index=False).agg({"hostility_level": max})
    # tb_ = tb.sort_values("styear").drop_duplicates(subset=["dispnum"], keep="first")
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
            "dispnum": "number_new_disputes",
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
    ## For columns "number_ongoing_disputes", "number_new_disputes"
    columns = [
        "number_ongoing_disputes",
        "number_new_disputes",
    ]
    tb.loc[:, columns] = tb.loc[:, columns].fillna(0)

    # We are only reporting number of deaths for hostility_level='all'
    ## for hostility_level != 'all' we don't mind having NaNs
    mask = tb["hostility_level"] == "all"
    tb.loc[mask, "number_deaths_ongoing_disputes"] = tb.loc[mask, "number_deaths_ongoing_disputes"].fillna(0)

    # Drop all-NaN rows
    tb = tb.dropna(subset=columns, how="all")
    return tb
