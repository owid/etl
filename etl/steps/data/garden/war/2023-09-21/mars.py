"""Load a meadow dataset and create a garden dataset."""

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

# Define relevant columns
COLUMNS_CODES = ["warcode", "campcode", "ccode"]
COLUMNS_YEARS = ["yrstart", "yrend"]
COLUMNS_METRICS = ["kialow", "kiahigh"]
COLUMN_CIVIL_WAR = "civilwar"
# Regions
## Region name mapping
REGIONS_RENAME = {
    "asia": "Asia (Project Mars)",
    "eeurop": "Eastern Europe (Project Mars)",
    "lamerica": "Latin America (Project Mars)",
    "nafrme": "North Africa and the Middle East (Project Mars)",
    "namerica": "North America (Project Mars)",
    "ssafrica": "Sub-Saharan Africa (Project Mars)",
    "weurope": "Western Europe (Project Mars)",
}
## Columns containing FLAGs for regions
COLUMNS_REGIONS = list(REGIONS_RENAME.keys())
## All relevant columns
COLUMNS_RELEVANT = COLUMNS_CODES + COLUMNS_YEARS + COLUMNS_METRICS + [COLUMN_CIVIL_WAR] + COLUMNS_REGIONS


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("mars"))

    # Read table from meadow dataset.
    tb = ds_meadow["mars"].reset_index()

    #
    # Process data.
    #
    log.info("war.mars: clean table")
    tb = clean_table(tb)

    log.info("war.mars: format regions and conflict type")
    tb = format_region_and_type(tb)

    log.info("war.mars: de-duplicate triplets")
    tb = reduce_triplets(tb)

    log.info("war.mars: add all observation years")
    tb = expand_observations(tb)

    log.info("war.mars: aggregate numbers at warcode level")
    tb = aggregate_wars(tb)

    log.info("war.mars: estimate metrics")
    tb = estimate_metrics(tb)

    log.info("war.mars: replace NaNs with zeroes")
    tb = replace_missing_data_with_zeros(tb)

    # Rename regions
    log.info("war.mars: rename regions")
    tb["region"] = tb["region"].map(REGIONS_RENAME | {"World": "World"})
    assert tb["region"].isna().sum() == 0, "Unmapped regions!"

    # Dtypes
    log.info("war.mars: set dtypes")
    tb = tb.astype(
        {
            "year": "uint16",
            "region": "category",
            "conflict_type": "category",
            "number_ongoing_conflicts": "Int64",
            "number_new_conflicts": "Int64",
        }
    )

    # Set index
    log.info("war.mars: set index")
    tb = tb.set_index(["year", "region", "conflict_type"], verify_integrity=True).sort_index()

    # Add short_name to table
    log.info("war.mars: add shortname to table")
    tb = Table(tb, short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def clean_table(tb: Table) -> Table:
    """Clean the table.

    - Drops rows with all NaNs
    - Runs minimal sanity check
    - Keeps only relevant columns
    """
    ## Drop NaNs
    tb = tb.dropna(how="all")

    ## Check at least one and only one FLAG within each group is always activated
    assert (
        tb[COLUMNS_REGIONS].sum(axis=1) == 1
    ).all(), "Entry found with no region (one more than one region) assigned!"

    ## Keep only relevant columns
    tb = tb[COLUMNS_RELEVANT]

    return tb


def format_region_and_type(tb: Table) -> Table:
    """Unpivot and reshape table to have two new columns with region and conflict type information.

    - Originally, the information on the region comes encoded as multi-column flags. Instead, we change this to be a single column with the raw region names.
    - We encode the conflict type in two categories: 'civil war' and 'others (non-civil war)'.
    """
    ## Get region label
    tb = tb.melt(id_vars=COLUMNS_CODES + COLUMNS_YEARS + COLUMNS_METRICS + [COLUMN_CIVIL_WAR], var_name="region")
    tb = tb[tb["value"] == 1].drop(columns="value")

    ## Get conflict_type info
    tb[COLUMN_CIVIL_WAR] = tb[COLUMN_CIVIL_WAR].map({0: "others (non-civil)", 1: "civil war"})
    tb = tb.rename(columns={COLUMN_CIVIL_WAR: "conflict_type"})

    return tb


def reduce_triplets(tb: Table) -> Table:
    """Reduce table to have only one row per (warcode, campcode, ccode) triplet.

    - There are few instances where there are duplicate entries with the same triplet.
    - This function aggregates the number of deaths for these, and takes the start and end year of the triplet as the min and max, respectively.
    - If duplicated triplets present different regions or conflict types, this will raise an error!
    """
    ## Combine duplicated tripplets ("warcode", "campcode", "ccode")
    tb = tb.groupby(["warcode", "campcode", "ccode"], as_index=False).agg(
        {
            "yrstart": "min",
            "yrend": "max",
            "kialow": "sum",
            "kiahigh": "sum",
            "region": lambda x: list(set(x))[0] if len(set(x)) == 1 else np.nan,
            "conflict_type": lambda x: list(set(x))[0] if len(set(x)) == 1 else np.nan,
        }
    )
    assert tb.isna().sum().sum() == 0, "Unexpected NaNs were found!"

    return tb


def expand_observations(tb: Table) -> Table:
    """Add entries to each triplet for all its years of activity.

    - By default, a triplet only appears in a row, with the total number of fatalities and the year of start and end.
    - This function expands these single entries in order to obtain a row per triplet + year of occurence.
    - Deaths per year are estimated assuming uniform distribution.

    Example

        Input:

        | warcode | campcode | ccode | year_start | year_end | deaths |
        |---------|----------|-------|------------|----------|--------|
        | 1       | 1        | 1     | 1990       | 1993     | 12     |

        Output:

        |  year | warcode | campcode | ccode | deaths |
        |-------|---------|----------|-------|--------|
        |  1990 |    1    |    1     | 1     |   3    |
        |  1991 |    1    |    1     | 1     |   3    |
        |  1992 |    1    |    1     | 1     |   3    |
        |  1993 |    1    |    1     | 1     |   3    |

    """
    # Add an entry of the triplet ("warcode", "campcode", "ccode") per year
    # For that we scale the number of deaths proportional to the duration of the conflict.
    tb[["kialow", "kiahigh"]] = tb[["kialow", "kiahigh"]].div(tb["yrend"] - tb["yrstart"] + 1, "index").round()

    ## Add missing years for each triplet ("warcode", "campcode", "ccode")
    YEAR_MIN = tb["yrstart"].min()
    YEAR_MAX = tb["yrend"].max()
    tb_all_years = pd.DataFrame(pd.RangeIndex(YEAR_MIN, YEAR_MAX + 1), columns=["year"])
    tb = pd.DataFrame(tb)  # to prevent error "AttributeError: 'DataFrame' object has no attribute 'all_columns'"
    tb = tb.merge(tb_all_years, how="cross")
    ## Filter only entries that actually existed
    tb = tb[(tb["year"] >= tb["yrstart"]) & (tb["year"] <= tb["yrend"])]

    return tb


def aggregate_wars(tb: Table) -> Table:
    """Aggregate wars (over campaigns and actors).

    - Goes from triplets to single warcodes.
    - Preserving triplets until now helps us have better death estimates per year and regions.
    """
    tb = tb.groupby(["year", "warcode", "region"], as_index=False).agg(
        {
            "conflict_type": lambda x: "others (non-civil)" if "others (non-civil)" in set(x) else "civil war",
            "kialow": "sum",
            "kiahigh": "sum",
            "yrstart": lambda x: min(x),
        }
    )
    tb["yrstart"] = tb.groupby("warcode")["yrstart"].transform(min)

    return tb


def estimate_metrics(tb: Table) -> Table:
    """Estimate relevant metrics.

    Relevant metrics are:
        - Number of ongoing conflicts in a year
        - Number of deaths from ongoing conflicts in a given year (estimate)
        - Number of new conflicts starting in a year

    This function also renames columns to fit expected names.
    """
    # Add metrcs
    tb_ongoing = _create_ongoing_metrics(tb)
    tb_new = _create_metrics_new(tb)

    # Combine ongoing conflicts with new conflicts
    tb = tb_ongoing.merge(tb_new, on=["year", "conflict_type", "region"], suffixes=("_ongoing", "_new"), how="outer")

    # Rename colums
    tb = tb.rename(
        columns={
            "warcode_ongoing": "number_ongoing_conflicts",
            "warcode_new": "number_new_conflicts",
            "kiahigh": "number_deaths_ongoing_conflicts_high",
            "kialow": "number_deaths_ongoing_conflicts_low",
            "region": "region",
        }
    )

    return tb


def _create_ongoing_metrics(tb: Table) -> Table:
    # Check that for a given year and conflict, it only has one conflict type
    tb.groupby(["year", "warcode"])["conflict_type"].nunique().max()

    # Estimate number of ongoing conflicts
    agg_ops = {"warcode": "nunique", "kialow": "sum", "kiahigh": "sum"}
    ## Regions
    tb_ongoing_regions = tb.groupby(["year", "region", "conflict_type"], as_index=False).agg(agg_ops)
    ### All conflicts
    tb_ongoing_regions_all_conf = tb.groupby(["year", "region"], as_index=False).agg(agg_ops)
    tb_ongoing_regions_all_conf["conflict_type"] = "all"
    ## World
    tb_ongoing_world = tb.groupby(["year", "conflict_type"], as_index=False).agg(agg_ops)
    tb_ongoing_world["region"] = "World"
    ### All conflicts
    tb_ongoing_world_all_conf = tb.groupby(["year"], as_index=False).agg(agg_ops)
    tb_ongoing_world_all_conf["region"] = "World"
    tb_ongoing_world_all_conf["conflict_type"] = "all"

    ## Combine
    tb_ongoing = pd.concat(
        [
            tb_ongoing_regions,
            tb_ongoing_world,
            tb_ongoing_regions_all_conf,
            tb_ongoing_world_all_conf,
        ],
        ignore_index=True,
    )
    return tb_ongoing


def _create_metrics_new(tb: Table) -> Table:
    # Estimate number of new conflicts
    ## Regions
    ### For a region, the same conflict can only start once. We assign the conflict type of when it first started.
    tb_ = tb.sort_values("year").drop_duplicates(subset=["warcode", "region"], keep="first").copy()
    tb_new_regions = tb_.groupby(["yrstart", "region", "conflict_type"], as_index=False).agg({"warcode": "nunique"})
    tb_new_regions_all_conf = tb_.groupby(["yrstart", "region"], as_index=False).agg({"warcode": "nunique"})
    tb_new_regions_all_conf["conflict_type"] = "all"
    ## World
    ### We estimate the conflict_type='all' category separately for the World as otherwise we could be double-counting
    ### some conflicts. E.g., the same conflict
    tb_ = tb.sort_values("year").drop_duplicates(subset=["warcode"], keep="first").copy()
    tb_new_world = tb_.groupby(["yrstart", "conflict_type"], as_index=False).agg({"warcode": "nunique"})
    tb_new_world["region"] = "World"
    tb_new_world_all_conf = tb_.groupby("yrstart", as_index=False).agg({"warcode": "nunique"})
    tb_new_world_all_conf["conflict_type"] = "all"
    tb_new_world_all_conf["region"] = "World"
    ## Combine
    tb_new = pd.concat(
        [tb_new_regions, tb_new_regions_all_conf, tb_new_world, tb_new_world_all_conf], ignore_index=True
    )
    tb_new = tb_new.rename(columns={"yrstart": "year"})  # type: ignore

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
    ## For columns "number_ongoing_conflicts", "number_new_conflicts"; conflict_type="extrasystemic"
    columns = [
        "number_ongoing_conflicts",
        "number_new_conflicts",
        "number_deaths_ongoing_conflicts_high",
        "number_deaths_ongoing_conflicts_low",
    ]
    tb.loc[:, columns] = tb.loc[:, columns].fillna(0)

    return tb
