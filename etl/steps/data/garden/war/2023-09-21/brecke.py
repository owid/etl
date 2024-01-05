"""Process Brecke dataset.

This dataset provides data since 1400, where each row in it denotes a certain conflict and its fatalities.

Drawback of this dataset is that the field `name` encodes the conflict name and conflict type together as a flat string.

Conflicts in this dataset always occur in the same region, and have the same conflict type. Conflict type can either be "inter-state" or "intra-state".

Each entry in the dataset describes a conflict during all the years it lasted. For instance, if a conflict lasted 3 years, it reports the total number of fatalities during this year in a single row.

On regions:
    - This dataset provides more granularity in terms of regions. For instance, it includes "Eastern Europe" and "Western Europe".

    - We map these regions to more standardised regions (those used in other datasets, e.g. COW or UCDP). E.g. we map "Eastern Europe" to "Europe".
        For more details, see variable `REGIONS_RENAME`.

    - It is not evident which region accounts for data from countries in Oceania.

    - It is unclear from the source which countries are included in each region. Some details can be inferred from page 10 in Brecke's 1999 paper "Violent Conflicts 1400 A.D. to the Present in Different Regions of the World" (https://bpb-us-w2.wpmucdn.com/sites.gatech.edu/dist/1/19/files/2018/09/Brecke-PSS-1999-paper-Violent-Conflicts-1400-AD-to-the-Present.pdf).

        We have manually inferred the mappings using COW codes. Our current estimate for the original regions is as follows:

        1. North America, Central America, and the Caribbean: 2-95 (US-Panama)
        2. South America: 100-165 (Colombia-Uruguay)
        3. Western Europe: 200-280 (UK-Mecklenburg Schwerin), 305 (Autria), 325-338 (Italy-Malta), 380-395 (Sweden-Iceland)
        4. Eastern Europe: 290-300 (Poland - Austria-Hungary), 310-317 (Hungary-Slovakia), 339-375 (Albania-Finland), 640 (Turkey)
        5. Middle East: 630 (Iran), 645-698 (Iraq-Oman)
        6. North Africa: 600-626 (Morocco-South Sudan) 432 (Mali), 435-436 (Mauritania-Niger), 483 (Niger), 651 (Egypt)
        7. West & Central Africa: 402-420 (Cape Verde, Gambia), 433-434 (Senegal-Benin), 437-482 (Ivory Coast-Central African Republic), 484-490 (Congo-Democratic Republic of the Congo)
        8. East & South Africa: 500-591 (Uganda-Seychelles)
        9. Central Asia: 700-705 (Afghanistan-Kazakhstan)
        10. South Asia: 750-771 (India-Bangladesh), 780-790 (Sri Lanka-Nepal)
        11. Southeast Asia: 800-990 (Thailand-Samoa)
        12. East Asia: 710-740 (Taiwan-Japan)

        With the mapping done with `REGIONS_RENAME`, we have:

        - Americas (1, 2): 2-165
        - Europe (3, 4): 200-395 (UK-Iceland)
        - Middle East (5): 630-698 (Iran-Oman), includes Turkey
        - Africa (6, 7, 8): 402-626 (Cape Verde-South Sudan),
        - Asia and Oceania (9, 10, 11, 12): 700-990 (Afghanistan-Samoa)
"""

import numpy as np
import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table
from shared import add_indicators_extra, expand_observations
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Logger
log = get_logger()
# Region mapping
REGIONS_RENAME = {
    1: "Americas",  # "North America, Central America, and the Caribbean",
    2: "Americas",  # "South America",
    3: "Europe",  # "Western Europe",
    4: "Europe",  # "Eastern Europe",
    5: "Middle East",
    6: "Africa",  # "North Africa",
    7: "Africa",  # "West & Central Africa",
    8: "Africa",  # "East & South Africa",
    9: "Asia and Oceania",  # "Central Asia",
    10: "Asia and Oceania",  # "South Asia",
    11: "Asia and Oceania",  # "Southeast Asia",
    12: "Asia and Oceania",  # "East Asia",
    -9: -9,  # Unknown
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    log.info("war.brecke: start")
    ds_meadow = paths.load_dataset("brecke")
    # Read table from meadow dataset.
    tb = ds_meadow["brecke"]

    # Read table from COW codes
    ds_isd = paths.load_dataset("isd")
    tb_regions = ds_isd["isd_regions"].reset_index()

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
    tb = expand_observations(
        tb,
        col_year_start="startyear",
        col_year_end="endyear",
        cols_scale=["totalfatalities"],
    )

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

    # Add conflict rates
    paths.log.info("add conflict rate and conflict mortality indicators")
    tb_regions = tb_regions[~tb_regions["region"].isin(["Sub-Saharan Africa", "North Africa and the Middle East"])]
    tb = add_indicators_extra(
        tb,
        tb_regions,
        columns_conflict_rate=["number_ongoing_conflicts", "number_new_conflicts"],
        columns_conflict_mortality=["number_deaths_ongoing_conflicts"],
    )

    # Add suffix with source name
    msk = tb["region"] != "World"
    tb.loc[msk, "region"] = tb.loc[msk, "region"] + " (Brecke)"

    # HOTFIX: Remove datapoints related to death count (and rates) for regions other than 'World'
    tb.loc[msk, ["number_deaths_ongoing_conflicts", "number_deaths_ongoing_conflicts_per_capita"]] = np.nan

    # Set index
    log.info("war.brecke: set index")
    tb = tb.set_index(["year", "region", "conflict_type"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

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
    assert (
        mask_custom.sum() == len(wars_interstate)
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
    tb_ongoing = pr.concat(
        [tb_ongoing, tb_ongoing_all_conf, tb_ongoing_world, tb_ongoing_world_all_conf], ignore_index=True
    ).sort_values(  # type: ignore
        by=["year", "region", "conflict_type"]
    )

    ## Rename columns
    tb_ongoing = tb_ongoing.rename(  # type: ignore
        columns={
            "conflict_code": "number_ongoing_conflicts",
            "totalfatalities": "number_deaths_ongoing_conflicts",
        }
    )

    tb_ongoing["number_ongoing_conflicts"] = tb_ongoing["number_ongoing_conflicts"].copy_metadata(
        tb_ongoing["number_deaths_ongoing_conflicts"]
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
    tb_new = pr.concat([tb_new, tb_new_all_conf, tb_new_world, tb_new_world_all_conf], ignore_index=True).sort_values(  # type: ignore
        by=["startyear", "region", "conflict_type"]
    )

    ## Rename columns
    tb_new = tb_new.rename(  # type: ignore
        columns={
            "startyear": "year",
            "conflict_code": "number_new_conflicts",
        }
    )

    tb_new["number_new_conflicts"] = tb_new["number_new_conflicts"].copy_metadata(tb["totalfatalities"])
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
