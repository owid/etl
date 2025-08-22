"""Regions dataset adjusted for Grapher.

This dataset should populate the charts of regions (defined by OWID and other institutions) published in:
https://ourworldindata.org/world-region-map-definitions

"""

import json
from typing import cast

# import pandas as pd
from owid.catalog import Origin, Table
from structlog import get_logger

from etl.helpers import PathFinder

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Get the current year from the step version.
# Note that this year will be ignored in the charts, but it is required by grapher.
CURRENT_YEAR = int(paths.version.split("-")[0])


# Institution mapping. Contains names, descriptions, origins, etc. TODO: Should probably live somewhere else (Garden? regions.yml?)
## Origins
DATE_ACCESSED = "2025-08-22"
OWID_ORIGIN = Origin(producer="Our World in Data", title="Regions")
ORIGIN_UN_M49 = Origin(
    producer="United Nations, Statistics Division",
    title="Standard country or area codes for statistical use (M49)",
    url_main="https://unstats.un.org/unsd/methodology/m49/",
    description="""The list of geographic regions presents the composition of geographical regions used by the Statistics Division in its publications and databases. Each country or area is shown in one region only. These geographic regions are based on continental regions; which are further subdivided into sub-regions and intermediary regions drawn as to obtain greater homogeneity in sizes of population, demographic circumstances and accuracy of demographic statistics.""",
    date_accessed=DATE_ACCESSED,
)
ORIGIN_WB = Origin(
    producer="World Bank",
    title="World Bank Country and Lending Groups",
    url_main="https://datahelpdesk.worldbank.org/knowledgebase/articles/906519-world-bank-country-and-lending-groups",
    date_accessed=DATE_ACCESSED,
)
ORIGINS_WHO = Origin(
    producer="World Health Organization",
    title="Countries/areas by WHO region",
    url_main="https://apps.who.int/violence-info/Countries%20and%20areas%20by%20WHO%20region%20-%2012bfe12.pdf",
    date_accessed=DATE_ACCESSED,
)
ORIGINS_SDG = Origin(
    producer="United Nations, Sustainable Development Goals",
    title="Regional groupings used in Report and Statistical Annex",
    url_main="https://unstats.un.org/sdgs/indicators/regional-groups/",
    date_accessed=DATE_ACCESSED,
)
## Reference dictionary
INSTITUTIONS = {
    "owid": {
        "name": "Our World in Data",
        "acronym": "OWID",
        "origins": [OWID_ORIGIN],
        "description": "Regions defined by Our World in Data, which are used in OWID charts and maps.",
    },
    "un_m49_1": {
        "name": "United Nations M49 (1)",
        "acronym": "UN M49 (1)",
        "description": "Level-1 broad regions defined by the United Nations.",
        "origins": [ORIGIN_UN_M49],
    },
    "un_m49_2": {
        "name": "United Nations M49 (2)",
        "acronym": "UN M49 (2)",
        "description": "Level-2 regions defined by the United Nations.",
        "origins": [ORIGIN_UN_M49],
    },
    "un_m49_3": {
        "name": "United Nations M49 (3)",
        "acronym": "UN M49 (3)",
        "description": "Level-3 (most granular) regions defined by the United Nations.",
        "origins": [ORIGIN_UN_M49],
    },
    "un": {
        "name": "United Nations",
        "acronym": "UN",
        "description": "Regions defined by the United Nations, which are used across the UN including UN WPP.",
        "origins": [ORIGIN_UN_M49],
    },
    "wb": {
        "name": "World Bank",
        "acronym": "WB",
        "description": "Regions as defined by the World Bank.",
        "origins": [ORIGIN_WB],
    },
    "who": {
        "name": "World Health Organization",
        "acronym": "WHO",
        "description": "Regions as defined by the World Health Organization.",
        "origins": [ORIGINS_WHO],
    },
    "unsdg": {
        "name": "United Nations Sustainable Development Goals",
        "acronym": "UN SDG",
        "description": "Regions as defined by the United Nations Sustainable Development Goals.",
        "origins": [ORIGINS_SDG],
    },
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("regions")
    tb_garden = ds_garden["regions"].reset_index()

    #
    # Process data.
    #
    # Select and rename columns.
    tb = tb_garden[["name", "code", "region_type", "is_historical", "defined_by", "members"]].rename(
        columns={"name": "country"}, errors="raise"
    )

    # Create a dictionary that maps codes to country names.
    code_to_country = tb.set_index("code")["country"].to_dict()

    # Create a new regions table with a more convenient format.
    tb_regions = tb[(tb["region_type"] == "country")].copy()
    for institution in set(tb["defined_by"]):
        if institution == "owid":
            region_type = "continent"
        else:
            region_type = "aggregate"
        # Select rows of regions as defined by the current institution.
        region_members = (
            tb[(tb["defined_by"] == institution) & (tb["region_type"] == region_type)]
            .set_index("country")["members"]
            .to_dict()
        )
        # Create a dictionary mapping a region to the names of its member countries.
        region_to_countries = {
            region: [code_to_country[code] for code in json.loads(members)]
            for region, members in region_members.items()
        }
        # Invert that dictionary.
        countries_to_region = {}
        for region, countries in region_to_countries.items():
            for country in countries:
                if country in countries_to_region:
                    log.warning(f"Country {country} belongs to multiple regions.")
                countries_to_region[country] = region
        # Create a temporary table for the current regions.
        _tb_regions = (
            Table.from_dict(countries_to_region, orient="index", columns=[f"{institution}_region"])
            .reset_index()
            .rename(columns={"index": "country"})
        )
        # Add a column with the region that each country belongs to, according to the current institution.
        tb_regions = tb_regions.merge(_tb_regions, on="country", how="left", validate="one_to_one")

        # Add metadata for the new column.
        tb_regions = _add_metadata(
            cast(Table, tb_regions),
            institution,
        )

    # Remove unnecessary columns.
    tb_regions = tb_regions.drop(
        columns=["code", "is_historical", "region_type", "defined_by", "members"], errors="raise"
    )

    # Add a year column
    tb_regions.loc[:, "year"] = CURRENT_YEAR

    # Downstream
    tb_regions = process_un_definitions(tb_regions)
    # tb_regions.loc[:, "un_m49_2_region"] = tb_regions.loc[:, "un_m49_2_region"].fillna(tb_regions["un_m49_1_region"])
    # tb_regions["un_m49_3_region"] = tb_regions["un_m49_3_region"].fillna(tb_regions["un_m49_2_region"])
    # tb_regions.loc[:, "un_m49_3_region"] = "lal"
    # tb_regions.loc[:, "un_m49_3_region"].fillna("la")

    # Update the table's metadata
    # NOTE: It would be better to do this in garden.
    tb_regions.metadata.title = "Definitions of world regions"

    # Format table conveniently.
    tb_regions = tb_regions.format()

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = paths.create_dataset(tables=[tb_regions])
    ds_grapher.save()


def _add_metadata(tb: Table, institution_alias: str) -> Table:
    assert institution_alias in INSTITUTIONS, f"Unknown institution: {institution_alias}"
    institution = INSTITUTIONS[institution_alias]

    # Get short name
    assert "acronym" in institution, f"Missing short_name for institution: {institution_alias}"
    tb[f"{institution_alias}_region"].metadata.origins = institution.get("origins", [OWID_ORIGIN])
    tb[f"{institution_alias}_region"].metadata.title = f"World regions according to {institution['acronym']}"
    tb[f"{institution_alias}_region"].metadata.description = institution.get("description", "")
    tb[f"{institution_alias}_region"].metadata.unit = ""
    return tb


def process_un_definitions(tb) -> Table:
    """UN provides various definitions of regions, which we need to process.

    - Level 1: High-level, broad regions. E.g. "Americas"
    - Level 2: More granular regions. E.g. "Latin America and the Caribbean", "Northern America"
    - Level 3: Even more granular regions. E.g. "Caribbean", "Central America"

    Problem: Not all regions are broken down into all three levels. E.g. "Europe" is a level 1 region, which has level 2 breakdown, but no 3 breakdown.

    Solution: Propagate definitions downstream when missing.
    """
    # Propagate definitions downstream.

    for i in range(1, 3):
        mask = tb[f"un_m49_{i+1}_region"].isna()
        tb.loc[mask, f"un_m49_{i+1}_region"] = tb.loc[mask, f"un_m49_{i}_region"]


    # Create new definition
    ## Get rows where "Americas" should be replaced with "Latin America and the Caribbean" and "Northern America"
    mask = tb["un_m49_2_region"].str.contains("America", na=False)
    assert tb.loc[mask, "un_m49_2_region"].nunique() == 2, "There should be only two Americas in UN M49 level 2."
    ## Create new column
    tb.loc[:, "un_region"] = tb.loc[:, "un_m49_1_region"].copy()
    tb.loc[mask, "un_region"] = tb.loc[mask, "un_m49_2_region"].copy()
    tb = _add_metadata(tb, "un")
    return cast(Table, tb)
