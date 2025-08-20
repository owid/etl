"""Regions dataset adjusted for Grapher.

This dataset should populate the charts of regions (defined by OWID and other institutions) published in:
https://ourworldindata.org/world-region-map-definitions

"""

import json
from typing import cast

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

# Define a common origin for all columns in the output table.
COMMON_ORIGIN = Origin(producer="Our World in Data", title="Regions")

# Institution mapping. TODO: Should be moved to regions.yml
INSTITUTION_MAPPING = {
    "owid": "OWID",
    "un_m49_1": "UN M49 (1)",
    "un_m49_2": "UN M49 (2)",
    "un_m49_3": "UN M49 (3)",
    "un": "UN",
    "wb": "WB",
    "who": "WHO",
    "unsdg": "UN SDG",
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
    tb_regions["year"] = CURRENT_YEAR

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


def _add_metadata(tb: Table, institution: str) -> Table:
    institution_name = INSTITUTION_MAPPING.get(institution, institution)
    tb[f"{institution}_region"].metadata.origins = [COMMON_ORIGIN]
    tb[f"{institution}_region"].metadata.title = f"World regions according to {institution_name}"
    tb[f"{institution}_region"].metadata.unit = ""
    return tb


def process_un_definitions(tb: Table) -> Table:
    """UN provides various definitions of regions, which we need to process.

    - Level 1: High-level, broad regions. E.g. "Americas"
    - Level 2: More granular regions. E.g. "Latin America and the Caribbean", "Northern America"
    - Level 3: Even more granular regions. E.g. "Caribbean", "Central America"

    Problem: Not all regions are broken down into all three levels. E.g. "Europe" is a level 1 region, which has level 2 breakdown, but no 3 breakdown.

    Solution: Propagate definitions downstream when missing.
    """
    # Propagate definitions downstream.
    tb["un_m49_2_region"] = tb["un_m49_2_region"].fillna(tb["un_m49_1_region"])
    tb["un_m49_3_region"] = tb["un_m49_3_region"].fillna(tb["un_m49_2_region"])
    return tb
