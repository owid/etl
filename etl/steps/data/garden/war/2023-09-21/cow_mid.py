"""COW Militarised Inter-state Dispute dataset.


- This dataset only contains inter-state disputes.

- We use the "fatality" and "hostility" levels to differentiate different "types" of disputes.

    - The "fatality" level provides a range of fatalities (e.g. '1-25 deaths')

    - The "hostility" level provides a short summary of the hostility degree of the dispute ('Use of force', 'War', etc.)

- Each entry in the source dataset describes a dispute (its participants and period). Therefore we need to "explode" it to add observations
for each year of the dispute.

- Due to missing data in the number of deaths, we are not estimating this metric. Instead, we are using the "fatality" level to group by the different conflicts.

- The "number of ongoing disputes" for a particular fatality level can be understood as "the number of conflicts ongoing in a particular year that will have between X1-X2 fatalities over their complete lifetime globally".

- The "number of ongoing disputes" for a particular hostility level can be understood as "the number of conflicts ongoing in a particular year that will reach this hostility level over their complete lifetime globally".


On regions:

    - The information on regions is sourced from MIDB dataset. That's why we use it along with MIDA (our main source of data).

    - Note that we obtain the information on regions from the participants in the conflict (not the location of the conflict), e.g. `ccode`. This is the same approach as in MIE dataset.

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
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

from .shared import add_indicators_extra, expand_observations

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Log
log = get_logger()
# Mapping from fatality code to name
FATALITY_LEVEL_MAP = {
    0: "No deaths",
    1: "1-25 deaths",
    2: "26-100 deaths",
    3: "101-250 deaths",
    4: "251-500 deaths",
    5: "501-999 deaths",
    6: "> 999 deaths",
    -9: "Unknown",
}
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
    ds_meadow = paths.load_dataset("cow_mid")
    # Read table from meadow dataset.
    tb_a = ds_meadow["mida"].reset_index()
    tb_b = ds_meadow["midb"].reset_index()

    # Read table from COW codes
    ds_cow_ssm = paths.load_dataset("cow_ssm")
    tb_regions = ds_cow_ssm["cow_ssm_regions"].reset_index()

    #
    # Process data.
    #
    # MIDA contains data at conflict level
    log.info("war.cow_mid: read and process MIDA table")
    tb_a = process_mida_table(tb_a)

    # MIDB contains data at dispute-country level, and helps us identify the region of each dispute
    # by looking at the countries involved.
    log.info("war.cow_mid: read and process MIDB table")
    tb_b = process_midb_table(tb_b)

    log.info("war.cow_mid: combine tables")
    tb = combine_tables(tb_a, tb_b)

    # Estimate metrics
    log.info("war.cow_mid: estimate metrics")
    tb = estimate_metrics(tb)

    # Map fatality codes to names
    log.info("war.cow_mid: map fatality codes to names")
    tb["fatality"] = tb["fatality"].map(FATALITY_LEVEL_MAP | {"all": "all"})
    assert tb["fatality"].notna().all(), "Unmapped fatality codes!"

    # Map fatality codes to names
    log.info("war.cow_mid: map hostility codes to names")
    tb["hostility"] = tb["hostility"].map(HOSTILITY_LEVEL_MAP | {"all": "all"})
    assert tb["hostility"].notna().all(), "Unmapped hostility codes!"

    log.info("war.cow_mid: replace NaNs with zeros where applicable")
    tb = replace_missing_data_with_zeros(tb)

    # Add normalised indicators
    log.info("war.mie: add normalised indicators")
    tb = add_indicators_extra(tb, tb_regions, columns_conflict_rate=["number_ongoing_disputes", "number_new_disputes"])

    # Add suffix with source name
    msk = tb["region"] != "World"
    tb.loc[msk, "region"] = tb.loc[msk, "region"] + " (COW)"

    # Set index
    log.info("war.cow_mid: set index")
    tb = tb.set_index(["year", "region", "fatality", "hostility"], verify_integrity=True).sort_index()

    # Add short_name to table
    log.info("war.cow_mid: add shortname to table")
    tb.m.short_name = paths.short_name

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def process_mida_table(tb: Table) -> Table:
    """Process MIDA table.

    - Sanity checks
    - Keep relevant columns
    - Add observation per year
    """
    # Sanity checks
    assert tb["dispnum"].value_counts().max() == 1, "The same conflict (with same `dispnum`) appears multiple times"
    assert (tb["styear"] >= 0).all(), "NA values (or negative) found in `styear`"
    assert (tb["endyear"] >= 0).all(), "NA values (or negative) found in `endyear`"
    assert not set(tb["fatality"]) - set(FATALITY_LEVEL_MAP), "Unnexpected values for `fatality`!"
    assert not set(tb["hostlev"]) - set(HOSTILITY_LEVEL_MAP), "Unnexpected values for `fatality`!"

    # Keep relevant columns
    COLUMNS_RELEVANT = [
        "dispnum",
        "styear",
        "endyear",
        "fatality",
        "hostlev",
    ]
    tb = tb[COLUMNS_RELEVANT]

    # Add observation for each year
    tb = expand_observations(
        tb,
        col_year_start="styear",
        col_year_end="endyear",
    )

    # Drop columns
    tb = tb.drop(columns=["styear", "endyear"])

    return tb


def process_midb_table(tb: Table) -> Table:
    """Process MIDB table.

    - Sanity checks
    - Keep relevant columns
    - Add observation per year
    - Add regions
    """
    # Sanity checks
    assert (
        tb.groupby(["dispnum", "ccode", "styear", "endyear"]).size().max() == 1
    ), "Multiple entries for a conflict-country-start_year-end_year"
    assert tb["styear"].notna().all() and (tb["styear"] >= 0).all(), "NA values (or negative) found in `styear`"
    assert tb["endyear"].notna().all() and (tb["endyear"] >= 0).all(), "NA values (or negative) found in `endyear`"

    # Add regions
    tb = add_regions(tb)

    # Keep relevant columns
    COLUMNS_RELEVANT = [
        "dispnum",
        "styear",
        "endyear",
        "region",
    ]
    tb = tb[COLUMNS_RELEVANT]

    # Drop duplicates
    tb = tb.drop_duplicates()

    # Add observation for each year
    tb = expand_observations(
        tb,
        col_year_start="styear",
        col_year_end="endyear",
    )

    # Drop columns
    tb = tb.drop(columns=["styear", "endyear"])

    return tb


def combine_tables(tb_a: Table, tb_b: Table) -> Table:
    """Combine MIDA and MIDB processed tables.

    Basically, we add region information (from MIDB) to MIDA.
    """
    # Merge
    tb = tb_a.merge(tb_b, on=["dispnum", "year"], how="left")

    # Fill NaNs
    ## Some disputes (identified by codes) have no region information in MIDB. We fill them manually.
    ## Sanity check (1)
    dispnum_nans_expected = {2044, 2328, 4005}  # dispute codes with no region information
    dispnum_nans_found = set(tb.loc[tb["region"].isna()])
    dispnum_nans_unexpected = dispnum_nans_found - dispnum_nans_expected
    assert dispnum_nans_unexpected, f"Unexpected dispnum with NaN regions: {dispnum_nans_unexpected}"
    ## Sanity check (2)
    assert (
        tb_b[tb_b["dispnum"].isin(dispnum_nans_expected)].groupby("dispnum")["region"].nunique().max() == 1
    ), f"More than one region for some dispnum in {dispnum_nans_expected}"
    ## Actually fill NaNs
    tb.loc[tb["dispnum"] == 2044, "region"] = "Americas"
    tb.loc[tb["dispnum"] == 2328, "region"] = "Europe"
    tb.loc[tb["dispnum"] == 4005, "region"] = "Asia and Oceania"

    # Check there is no NaN!
    assert tb.notna().all().all(), "NaN in some field!"

    return tb


def add_regions(tb: Table) -> Table:
    """Assign region to each dispute-country pair.

    The region is assigned based on the country code (ccode) of the participant.
    """
    ## COW uses custom country codes, so we need the following custom mapping.
    tb.loc[(tb["ccode"] >= 1) & (tb["ccode"] <= 165), "region"] = "Americas"
    tb.loc[(tb["ccode"] >= 200) & (tb["ccode"] <= 395), "region"] = "Europe"
    tb.loc[(tb["ccode"] >= 402) & (tb["ccode"] <= 626), "region"] = "Africa"
    tb.loc[(tb["ccode"] >= 630) & (tb["ccode"] <= 698), "region"] = "Middle East"
    tb.loc[(tb["ccode"] >= 700) & (tb["ccode"] <= 999), "region"] = "Asia and Oceania"
    # tb.loc[(tb["ccode"] >= 700) & (tb["ccode"] <= 850), "region"] = "Asia"
    # tb.loc[(tb["ccode"] >= 860) & (tb["ccode"] <= 999), "region"] = "Oceania"

    # Sanity check: No missing regions
    assert tb["region"].notna().all(), f"Missing regions! {tb.loc[tb['region'].isna(), ['dispnum', 'ccode']]}"
    return tb


def estimate_metrics(tb: Table) -> Table:
    """Remix table to have the desired metrics.

    These metrics are:
        - number_ongoing_disputes

    Parameters
    ----------
    tb : Table
        Table with a row per dispute and year of observation. It also contains info on hostility, fatality and participant states.

    Returns
    -------
    Table
        Table with a row per year, and the corresponding metrics of interest.
    """
    # Estimate metrics broken down by hostility
    ## estimate `number_ongoing_disputes` and `number_new_disputes`
    tb_hostility = _estimate_metrics_hostility(tb.copy())
    # Estimate metrics broken down by fatality
    ## only estimate `number_ongoing_disputes`
    tb_fatality = _estimate_metrics_fatality(tb.copy())

    # Combine
    tb = pr.concat([tb_fatality, tb_hostility], ignore_index=False)

    return tb


def _estimate_metrics_fatality(tb: Table) -> Table:
    """Estimate metrics broken down by fatality level.

    Also include fatality="all".

    We assign hostility="all".
    """
    assert (
        tb.groupby(["dispnum"])["fatality"].nunique().max() == 1
    ), "The same conflict appears with multiple fatality levels!"

    # Operations to apply
    ops = {"dispnum": "nunique"}

    # By regions
    tb_regions = tb.groupby(["year", "fatality", "region"], as_index=False).agg(ops)
    # World
    tb_world = tb.groupby(["year", "fatality"], as_index=False).agg(ops)
    tb_world["region"] = "World"

    # Combine
    tb = pr.concat([tb_regions, tb_world], ignore_index=False)

    # Rename indicator column
    tb = tb.rename(columns={"dispnum": "number_ongoing_disputes"})

    # Add fatality="all"
    # ops = {"number_ongoing_disputes": "sum"}
    # tb_all = tb.groupby(["year", "region"], as_index=False).agg(ops)
    # tb_all["fatality"] = "all"
    # tb = pr.concat([tb, tb_all], ignore_index=False)

    # Add hostility level
    tb["hostility"] = "all"

    return tb


def _estimate_metrics_hostility(tb: Table) -> Table:
    """Estimate metrics broken down by hostility level.

    We assign fatality="all".
    """
    tb_ongoing = _estimate_metrics_hostility_ongoing(tb)
    tb_new = _estimate_metrics_hostility_new(tb)

    # Combine
    tb = tb_ongoing.merge(tb_new, on=["year", "region", "hostility"], how="outer")

    # Add hostility level
    tb["fatality"] = "all"

    return tb


def _estimate_metrics_hostility_ongoing(tb: Table) -> Table:
    assert (
        tb.groupby(["dispnum"])["hostlev"].nunique().max() == 1
    ), "The same conflict appears with multiple hostlev levels!"

    # Operations to apply
    ops = {"dispnum": "nunique"}

    ## By regions and hostility level
    tb_regions = tb.groupby(["year", "hostlev", "region"], as_index=False).agg(ops)

    ## region='World' and by hostility_level
    tb_world = tb.groupby(["year", "hostlev"], as_index=False).agg(ops)
    tb_world["region"] = "World"

    # hostility = "all"
    tb_allhost = tb.groupby(["year", "region"], as_index=False).agg(ops)
    tb_allhost["hostlev"] = "all"

    # hostility = "all" & region = 'World'
    tb_world_allhost = tb.groupby(["year"], as_index=False).agg(ops)
    tb_world_allhost["region"] = "World"
    tb_world_allhost["hostlev"] = "all"

    # Combine
    tb = pr.concat([tb_regions, tb_world, tb_allhost, tb_world_allhost], ignore_index=False)

    # Rename indicator column
    tb = tb.rename(
        columns={
            "dispnum": "number_ongoing_disputes",
            "hostlev": "hostility",
        }
    )

    return tb


def _estimate_metrics_hostility_new(tb: Table) -> Table:
    assert (
        tb.groupby(["dispnum"])["hostlev"].nunique().max() == 1
    ), "The same conflict appears with multiple hostlev levels!"
    # Operations to apply
    ops = {"dispnum": "nunique"}

    # By regions
    ## Drop 'duplicates'
    tb = tb.sort_values("year").drop_duplicates(subset=["dispnum", "region"], keep="first")
    tb_regions = tb.groupby(["year", "hostlev", "region"], as_index=False).agg(ops)
    ## By region and hostility level
    tb_allhost = tb.groupby(["year", "region"], as_index=False).agg(ops)
    tb_allhost["hostlev"] = "all"

    # World
    ## Drop 'duplicates'
    tb = tb.sort_values("year").drop_duplicates(subset=["dispnum"], keep="first")
    ## World and by region
    tb_world = tb.groupby(["year", "hostlev"], as_index=False).agg(ops)
    tb_world["region"] = "World"
    # hostility = "all" & region = 'World'
    tb_world_allhost = tb.groupby(["year"], as_index=False).agg(ops)
    tb_world_allhost["region"] = "World"
    tb_world_allhost["hostlev"] = "all"

    # Combine
    tb = pr.concat([tb_regions, tb_allhost, tb_world, tb_world_allhost], ignore_index=False)

    # Rename indicator column
    tb = tb.rename(
        columns={
            "dispnum": "number_new_disputes",
            "hostlev": "hostility",
        }
    )

    return tb


def replace_missing_data_with_zeros(tb: Table) -> Table:
    """Replace missing data with zeros.
    In some instances there is missing data. Instead, we'd like this to be zero-valued.
    """
    # Add missing (year, region, hostility_type) entries (filled with NaNs)
    years = np.arange(tb["year"].min(), tb["year"].max() + 1)
    regions = set(tb["region"])
    fatality_types = set(tb["fatality"])
    hostility_types = set(tb["hostility"])
    new_idx = pd.MultiIndex.from_product(
        [years, regions, fatality_types, hostility_types], names=["year", "region", "fatality", "hostility"]
    )
    # Only keep rows with "all" in fatality or hostility
    # That is, either break down indicators by fatality or by hostility
    new_idx = [i for i in new_idx if (i[2] == "all") or (i[3] == "all")]
    tb = tb.set_index(["year", "region", "fatality", "hostility"], verify_integrity=True).reindex(new_idx).reset_index()

    # Change NaNs for 0 for specific rows
    ## For columns "number_ongoing_disputes", "number_new_disputes"
    columns = [
        "number_ongoing_disputes",
        "number_new_disputes",
    ]
    tb.loc[:, columns] = tb.loc[:, columns].fillna(0)

    # Drop all-NaN rows
    tb = tb.dropna(subset=columns, how="all")
    return tb
