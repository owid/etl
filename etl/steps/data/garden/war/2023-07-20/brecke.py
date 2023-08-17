"""Process Brecke dataset.

This dataset provides data since 1400, where each row in it denotes a certain conflict and its fatalities.

Drawback of this dataset is that the field `name` encodes the conflict name and conflict type together as a flat string.

Conflicts in this dataset always occur in the same region, and have the same conflict type. Conflict type can either be "inter-state" or "intra-state".
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
# Region mapping
REGIONS_RENAME = {
    1: "America",  # "North America, Central America, and the Caribbean (Brecke)",
    2: "America",  # "South America (Brecke)",
    3: "Europe",  # "Western Europe (Brecke)",
    4: "Europe",  # "Eastern Europe (Brecke)",
    5: "Middle East (Brecke)",
    6: "Africa",  # "North Africa (Brecke)",
    7: "Africa",  # "West & Central Africa (Brecke)",
    8: "Africa",  # "East & South Africa (Brecke)",
    9: "Asia",  # "Central Asia (Brecke)",
    10: "Asia",  # "South Asia (Brecke)",
    11: "Asia",  # "Southeast Asia (Brecke)",
    12: "Asia",  # "East Asia (Brecke)",
    -9: -9,  # Unknown
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    log.info("war.brecke: start")
    ds_meadow = cast(Dataset, paths.load_dependency("brecke"))

    # Read table from meadow dataset.
    tb = ds_meadow["brecke"]

    #
    # Process data.
    #
    # Keep relevant columns
    log.info("war.brecke: keep relevant columns")
    COLUMNS_RELEVANT = ["name", "startyear", "endyear", "region", "totalfatalities"]
    tb = tb[COLUMNS_RELEVANT]

    # Create conflict code
    log.info("war.brecke: create conflict code")
    tb = tb.reset_index(names=["conflict_code"])

    # Add conflict type
    log.info("war.brecke: add conflict_type")
    tb = add_conflict_type(tb)

    # Sanity check
    log.info("war.brecke: sanity checks")
    assert tb.groupby(["name"]).region.nunique().max() == 1, "Wars with same name but different region!"
    assert tb.groupby(["name"]).conflict_type.nunique().max() == 1, "Wars with same name but different conflict_type!"

    # Add enyear 2000 where unknown
    ## This will help us expand the observations and distribute the number of deaths over the conflict years.
    log.info("war.brecke: set end year to 2000 where unknown")
    YEAR_LAST = 2000
    tb.loc[tb["endyear"].isna(), "endyear"] = YEAR_LAST

    # Add deaths where they are missing (instead of NaNs use a lower bound, specified by the source)
    log.info("war.brecke: add lower bound value of deaths")
    tb = add_lower_bound_deaths(tb)

    # Rename regions
    log.info("war.brecke: rename regions")
    tb["region"] = tb["region"].map(REGIONS_RENAME)
    assert tb["region"].isna().sum() == 0, "Unmapped regions!"

    # Expand observations
    log.info("war.brecke: expand observations")
    tb = expand_observations(tb)

    # Estimate metrics
    log.info("war.brecke: estimate metrics")
    tb = estimate_metrics(tb)

    # Some data is missing (NaNs for certain year-region-conflict_type triplets)
    # We do not apply this to the number of deaths, as this metric is sometimes reported as NaN in the dataset (meaning data is missing, not zero!)
    log.info("war.brecke: replace missing data with zeroes")
    tb = replace_missing_data_with_zeros(tb)

    # Distribute numbers for region -9
    log.info("war.brecke: distributing metrics for region -9")
    tb = distribute_metrics_m9(tb)
    assert (tb["region"] != -9).all(), "-9 region still detected!"

    # Set index
    log.info("war.brecke: set index")
    tb = tb.set_index(["year", "region", "conflict_type"], verify_integrity=True)

    # Add short_name to table
    log.info("war.brecke: add shortname to table")
    tb = Table(tb, short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
    log.info("war.brecke: end")


def add_conflict_type(tb: Table) -> Table:
    """Create conflict type.

    We identify the conflict type by checking the format of `name` column:
        - If it has a hyphen separating two entities, it is an interstate conflict.
        - Otherwise, it is an intra-state conflict.

    Before doing this check, we need to remove non-relevant hyphens (separating years, actual hyphen-words)

    Parameters
    ----------
    tb : Table
        Original table without conflict type

    Returns
    -------
    Table
        Table with new column `conflict_type`.
    """
    # Remove hyphen where not needed
    remove_hyphen_with_space = [
        "governor-Mochiuj",
        "governor-governo",
        "army-governor",
        "Hongan-Kaga",
        "governor-governor",
        "south-west",
        "peasants-Indians",
        "Miao-tseu",
        "Takaungu-Gazi",
        "Kwara-Gojjam",
        "inter-communal",
        "various left-wing",
    ]

    remove_hyphen_with_join = [
        "Austria-Hungary",
        "Guinea-Bissau",
    ]

    for text in remove_hyphen_with_space:
        tb["name"] = tb["name"].str.replace(text, text.replace("-", " "))

    for text in remove_hyphen_with_join:
        tb["name"] = tb["name"].str.replace(text, text.replace("-", ""))

    # Remove year part
    name_wo_year = tb["name"].apply(lambda x: ",".join(x.split(",")[:-1]))

    # Get mask
    ## Wars that are inter-state but don't have a hyphen, hence would be classified as internal
    wars_interstate = [
        "First World War, 1914-18",
        "Thirty Years' War, 1618-48",
        "Napoleonic Wars, 1803-15",
        "Wars of the French Revolution, 1791-1802",
        "Vietnam, 1964-75",
    ]
    mask_custom = tb["name"].isin(wars_interstate)
    assert mask_custom.sum() == len(
        wars_interstate
    ), "Some corrections can't be made, because some war names specified in `wars_interstate` are not found in the data!"
    ## Build final mask
    mask = name_wo_year.str.contains("-") | mask_custom

    # Set conflict type
    tb["conflict_type"] = "internal"
    tb.loc[mask, "conflict_type"] = "interstate"

    return tb


def add_lower_bound_deaths(tb: Table) -> Table:
    """Replace missing data on deaths for a lower bound.

    Brecke writes that he only includes major violent conflicts. Among other characteristics, this means for him that there were at least 32 deaths per year.
    So what we are doing for conflicts with missing death estimates is to create a (possibly very) lower-bound estimate of 32 deaths for conflicts that lasted one year, 64 for those lasting two years, and so on.
    This would take the source seriously, allow us to calculate aggregates while still including these conflicts, and we could use line charts to visualize the data.
    """
    mask = tb["totalfatalities"].isna()
    tb.loc[mask, "totalfatalities"] = 32 * (tb.loc[mask, "endyear"] - tb.loc[mask, "startyear"] + 1)
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
    tb[["totalfatalities"]] = tb[["totalfatalities"]].div(tb["endyear"] - tb["startyear"] + 1, "index").round()

    # Add missing years for each triplet ("warcode", "campcode", "ccode")
    YEAR_MIN = tb["startyear"].min()
    YEAR_MAX = tb["endyear"].max()
    tb_all_years = pd.DataFrame(pd.RangeIndex(YEAR_MIN, YEAR_MAX + 1), columns=["year"])
    tb = pd.DataFrame(tb)  # to prevent error "AttributeError: 'DataFrame' object has no attribute 'all_columns'"
    tb = tb.merge(tb_all_years, how="cross")
    # Filter only entries that actually existed
    tb = tb[(tb["year"] >= tb["startyear"]) & (tb["year"] <= tb["endyear"])]

    return tb


def estimate_metrics(tb: Table) -> Table:
    """Remix table to have the desired metrics.

    These metrics are:
        - number_ongoing_conflicts
        - number_new_conflicts
        - number_deaths_ongoing_conflicts

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
    columns_idx = ["year", "region", "conflict_type"]
    tb = tb_ongoing.merge(tb_new, on=columns_idx, how="outer").sort_values(columns_idx)

    return tb


def _add_ongoing_metrics(tb: Table) -> Table:
    # Get ongoing #conflicts and deaths, by region and conflict type.

    def sum_nan(x: pd.Series):
        # Perform summation if all numbers are notna; otherwise return NaN.
        if not x.isna().any():
            return x.sum()
        return np.nan

    ops = {
        "conflict_code": "nunique",
        "totalfatalities": sum_nan,
    }
    ## By region and conflict_type
    tb_ongoing = tb.groupby(["year", "region", "conflict_type"], as_index=False).agg(ops)

    ## All conflict types
    tb_ongoing_all_conf = tb.groupby(["year", "region"], as_index=False).agg(ops)
    tb_ongoing_all_conf["conflict_type"] = "all"

    ## World
    tb_ongoing_world = tb.groupby(["year", "conflict_type"], as_index=False).agg(ops)
    tb_ongoing_world["region"] = "World"

    ## World & all conflicts
    tb_ongoing_world_all_conf = tb.groupby(["year"], as_index=False).agg(ops)
    tb_ongoing_world_all_conf["region"] = "World"
    tb_ongoing_world_all_conf["conflict_type"] = "all"

    ## Add region=World
    tb_ongoing = pd.concat([tb_ongoing, tb_ongoing_all_conf, tb_ongoing_world, tb_ongoing_world_all_conf], ignore_index=True).sort_values(  # type: ignore
        by=["year", "region", "conflict_type"]
    )

    ## Rename columns
    tb_ongoing = tb_ongoing.rename(  # type: ignore
        columns={
            "conflict_code": "number_ongoing_conflicts",
            "totalfatalities": "number_deaths_ongoing_conflicts",
        }
    )

    return tb_ongoing


def _add_new_metrics(tb: Table) -> Table:
    # Get new #conflicts, by region and conflict type.
    ops = {"conflict_code": "nunique"}
    ## By region and conflict_type
    tb_new = tb.groupby(["startyear", "region", "conflict_type"], as_index=False).agg(ops)

    ## All conflicts
    tb_new_all_conf = tb.groupby(["startyear", "region"], as_index=False).agg(ops)
    tb_new_all_conf["conflict_type"] = "all"

    ## World
    tb_new_world = tb.groupby(["startyear", "conflict_type"], as_index=False).agg(ops)
    tb_new_world["region"] = "World"

    ## World + all conflicts
    tb_new_world_all_conf = tb.groupby(["startyear"], as_index=False).agg(ops)
    tb_new_world_all_conf["region"] = "World"
    tb_new_world_all_conf["conflict_type"] = "all"

    ## Combine
    tb_new = pd.concat([tb_new, tb_new_all_conf, tb_new_world, tb_new_world_all_conf], ignore_index=True).sort_values(  # type: ignore
        by=["startyear", "region", "conflict_type"]
    )

    ## Rename columns
    tb_new = tb_new.rename(  # type: ignore
        columns={
            "startyear": "year",
            "conflict_code": "number_new_conflicts",
        }
    )

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
    tb = tb.set_index(["year", "region", "conflict_type"], verify_integrity=True).reindex(new_idx).reset_index()

    # Change NaNs for 0 for specific rows
    ## For columns "number_ongoing_conflicts", "number_new_conflicts"
    columns = [
        "number_ongoing_conflicts",
        "number_new_conflicts",
        "number_deaths_ongoing_conflicts",
    ]
    tb.loc[:, columns] = tb.loc[:, columns].fillna(0)

    return tb


def distribute_metrics_m9(tb: Table) -> Table:
    """Integrate rows entries with region=-9 with the rest of the data.

    Region -9 is likely to be 'World'. Therefore, we add these numbers to the other regions and remove these entries. We add the numbers as follows:

    - `number_deaths_ongoing_conflicts`
        - For 'World', we simply add the numbers.
        - For other regions, we divide the number by the number of regions in the dataset and add the numbers.
    - `number_ongoing_conflicts` and `number_new_conflicts`
        - We add the numbers for all regions (including 'World')

    NOTE:
        - Metrics in region -9 were already accounted for when estimating the values for 'World' in function `estimate_metrics`. Hence, we
        don't need to add anything to entries with region='World'.
    """
    # Distribute data for region=-9
    ## Get rows with region -9
    mask = tb["region"] == -9
    tb_m9 = tb.loc[mask].drop(columns=["region"]).copy()

    # Get regions
    regions = set(tb.region) - {-9, "World"}

    # original columns
    columns_og = tb.columns

    # Merge
    tb = tb.merge(tb_m9, on=["year", "conflict_type"], how="left", suffixes=("", "_m9"))

    # Integrate numbers with main table
    # Add number of conflicts, add uniformly the number of deaths
    columns = [
        "number_ongoing_conflicts",
        "number_deaths_ongoing_conflicts",
        "number_new_conflicts",
    ]
    # World
    for column in columns:
        # Regions
        mask = tb["region"] != "World"
        if "deaths" in column:
            tb.loc[mask, column] += tb.loc[mask, f"{column}_m9"] / len(regions)
        else:
            tb.loc[mask, column] += tb.loc[mask, f"{column}_m9"]

    # Remove region -9
    tb = tb[-tb["region"].isin([-9])]

    # Keep original columns
    tb = tb[columns_og]

    return tb
