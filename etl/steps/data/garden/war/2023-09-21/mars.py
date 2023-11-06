"""Project Mars combines multiple sources to generate a more complete dataset.

It relies heavily on COW.

On regions:

    - Very little is said on regions in the source's documentation. I only found some in their "DividedArmies_CodebookV1.1.pdf" pdf (https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/DUO7IE), page 40, section 12 "Variables: Fixed Effects".
    - Regions in the source are, along with their COW codes (based on GW):
        - Asia and Oceania:
            700-990 (Afghanistan-Samoa)
        - Eastern Europe:
            200-280 (UK-Mecklenburg Schwerin)
            305 (Austria)
            325-338 (Italy-Malta)
            375-395 (Finland-Iceland)
        - Western Europe:
            290-300 (Poland - Austria-Hungary)
            310-317 (Hungary-Slovakia)
            339-373 (Albania-Azerbaijan),
        - North America:
            2-20 (US-Canada)
        - Latin America:
            31-165 (Bahamas-Uruguay)
        - North Africa and the Middle East:
            432 (Mali)
            435-436 (Mauritania-Niger)
            483 (Chad)
            520-531 (Somalia-Eritrea)
            600-698 (Morocco-Oman)
        - Sub-Saharan Africa:
            402-420 (Cape Verde-Gambia)
            433-434 (Senegal-Benin)
            437-482 (Ivory Coast-Central African Republic)
            484-517 (Congo-Rwanda)
            540-591 (Angola-Seychelles)

    - We mostly preserve the regions by the source with the exception of:
        - Eastern Europe, Western Europe -> Europe
        - North america, Latin America -> Americas

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
# Some codes in Project Mars do not correspond to those in ISD
# This is used to map codes to country participants
# See in `_get_country_name`
CCODE_MAPPING = {
    324: 325,
    1001: 8531,
    1004: 4521,
    1006: 672,
    1007: 373,
    1009: 4327,
    1014: 4763,
    1015: 7020,
    1022: 371,
    1023: 700,
    1024: 4768,
    1026: 452,
    1028: 7103,
    1029: 101,
    1030: 670,
    1033: 5516,
    1034: 100,
    1036: 7579,
    1037: 663,
    1038: 7003,
    1040: 670,
    1042: 700,
    1048: 160,
    1049: 7103,
    1050: 7030,
    1052: 811,
    1053: 434,
    1054: 700,
    1055: 329,
    1060: 8121,
    1067: 625,
    1068: 4393,
    1069: 750,
    1070: 580,
    1071: 341,
    1077: 4832,
    1078: 136,
    1084: 713,
    1085: 600,
    1086: 101,
    1089: 327,
    1088: 600,
    1090: 678,
    1093: 7103,
    1096: 800,
    1097: 7691,
    1098: 7693,
    1099: 680,
    1100: 4751,
    1106: 4841,
    1108: 7910,
    1111: 4321,
    1115: 816,
    1121: 100,
    1124: 5621,
}
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
    "asia": "Asia and Oceania",
    "eeurop": "Europe",  # "Eastern Europe (Project Mars)",
    "weurope": "Europe",  # "Western Europe (Project Mars)",
    "lamerica": "Americas",  # "Latin America (Project Mars)",
    "namerica": "Americas",  # "North America (Project Mars)",
    "nafrme": "North Africa and the Middle East",
    "ssafrica": "Sub-Saharan Africa",
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
    ds_meadow = paths.load_dataset("mars")
    # Read table from meadow dataset.
    tb = ds_meadow["mars"].reset_index()

    # Read table from COW codes
    ds_isd = paths.load_dataset("isd")
    tb_regions = ds_isd["isd_regions"].reset_index()
    tb_codes = ds_isd["isd_countries"]

    #
    # Process data.
    #
    paths.log.info("clean table")
    tb = clean_table(tb)

    log.info("format regions and conflict type")
    tb = format_region_and_type(tb)

    # Rename regions
    paths.log.info("rename regions")
    tb["region"] = tb["region"].map(REGIONS_RENAME)
    assert tb["region"].isna().sum() == 0, "Unmapped regions!"

    paths.log.info("de-duplicate triplets")
    tb = reduce_triplets(tb)

    paths.log.info("add all observation years")
    tb = expand_observations(
        tb,
        col_year_start="yrstart",
        col_year_end="yrend",
        cols_scale=["kialow", "kiahigh"],
    )

    # Get country-level data
    paths.log.info("getting country-level indicators")
    tb_country = estimate_metrics_country_level(tb, tb_codes)

    paths.log.info("aggregate numbers at warcode level")
    tb = aggregate_wars(tb)

    paths.log.info("estimate metrics")
    tb = estimate_metrics(tb)

    paths.log.info("replace NaNs with zeroes")
    tb = replace_missing_data_with_zeros(tb)

    # Add conflict rates
    log.info("war.cow: map fatality codes to names")
    tb_regions = tb_regions[~tb_regions["region"].isin(["Africa", "Middle East"])]
    tb = add_indicators_extra(
        tb,
        tb_regions,
        columns_conflict_rate=["number_ongoing_conflicts", "number_new_conflicts"],
        columns_conflict_mortality=[
            "number_deaths_ongoing_conflicts_high",
            "number_deaths_ongoing_conflicts_low",
        ],
    )

    # Add suffix with source name
    msk = tb["region"] != "World"
    tb.loc[msk, "region"] = tb.loc[msk, "region"] + " (Project Mars)"

    # Dtypes
    paths.log.info("set dtypes")
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
    paths.log.info("set index")
    tb = tb.set_index(["year", "region", "conflict_type"], verify_integrity=True).sort_index()

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
    tb_ongoing = pr.concat(
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
    tb_new = pr.concat(
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


def estimate_metrics_country_level(tb: Table, tb_codes: Table) -> Table:
    """Add country-level indicators."""
    ###################
    # Participated in #
    ###################
    # FLAG YES/NO (country-level)

    # Get table with [year, conflict_type, code]
    tb_country = tb[["year", "conflict_type", "ccode"]].rename(columns={"ccode": "id"})

    # Drop rows with code = NaN
    tb_country = tb_country.dropna(subset=["id"])
    # Drop duplicates
    tb_country = tb_country.drop_duplicates()
    # Ensure numeric type
    tb_country["id"] = tb_country["id"].astype(int)
    # Translate Mars -> ISD codes
    tb_country["id"] = tb_country["id"].replace(CCODE_MAPPING)
    tb_country = tb_country.drop_duplicates()

    # Sanity check
    assert not tb_country.isna().any(axis=None), "There are some NaNs!"

    # Only consider years with data in ISD table
    tb_country = tb_country[tb_country["year"] >= tb_codes.reset_index()["year"].min()]
    # Only consider codes present in ISD + custom mapping
    codes = set(tb_codes.reset_index()["id"]) | set(CCODE_MAPPING)
    tb_country = tb_country[tb_country["id"].isin(codes)]

    # Add country name
    tb_country["country"] = tb_country.apply(lambda x: _get_country_name(tb_codes, x["id"], x["year"]), axis=1)
    assert tb_country["country"].notna().all(), "Some countries were not found! NaN was set"

    # Add flag
    tb_country["participated_in_conflict"] = 1
    tb_country["participated_in_conflict"].m.origins = tb["ccode"].m.origins

    # Prepare codes table
    tb_alltypes = Table(pd.DataFrame({"conflict_type": tb_country["conflict_type"].unique()}))
    tb_codes = tb_codes.reset_index().merge(tb_alltypes, how="cross")
    tb_codes["country"] = tb_codes["country"].astype(str)

    # Combine all codes entries with MARS
    columns_idx = ["year", "country", "id", "conflict_type"]
    tb_country = tb_codes.merge(tb_country, on=columns_idx, how="outer")
    tb_country["participated_in_conflict"] = tb_country["participated_in_conflict"].fillna(0)
    tb_country = tb_country[columns_idx + ["participated_in_conflict"]]

    # Add all conflict types
    tb_country = aggregate_conflict_types(tb_country, "all")
    # Add state-based
    # tb_country = add_conflict_country_all_statebased(tb_country)

    # Only preserve years that make sense
    tb_country = tb_country[(tb_country["year"] >= tb["year"].min()) & (tb_country["year"] <= tb["year"].max())]

    ###################
    # Participated in #
    ###################
    # NUMBER COUNTRIES

    tb_num_participants = get_number_of_countries_in_conflict_by_region(tb_country, "conflict_type", "isd")

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
    tb_country = tb_country.set_index(["year", "country", "conflict_type"], verify_integrity=True).sort_index()
    return tb_country


def _get_country_name(tb_codes: Table, code: int, year: int) -> str:
    if code not in set(tb_codes.reset_index()["id"]):
        raise ValueError(f"Code {code} not found in ISD table!")
    else:
        country_name = ""
        try:
            country_name = tb_codes.loc[(code, year)].item()
        except KeyError:
            # Concrete cases
            match code:
                # Serbia
                case 345:
                    if (year < 1941) or (year >= 2006):
                        country_name = "Serbia"
                    elif year >= 1941 and year < 1992:
                        country_name = "Yugoslavia"
                    elif year >= 1992 and year < 2006:
                        country_name = "Serbia and Montenegro"
                case _:
                    countries = set(tb_codes.loc[code, "country"])
                    if len(countries) != 1:
                        raise ValueError(f"More than one country found for code {code} in year {year}")
                    country_name = list(countries)[0]
    if country_name == "":
        raise ValueError("`country_name` must be set to a value!")

    return country_name


def add_conflict_country_all_ctypes(tb: Table) -> Table:
    """Add metrics for conflict_type = 'state-based'."""
    tb_all = tb.groupby(["year", "country"], as_index=False).agg(
        {"participated_in_conflict": lambda x: min(x.sum(), 1)}
    )
    tb_all["conflict_type"] = "all"
    tb = pr.concat([tb, tb_all], ignore_index=True)
    return tb
