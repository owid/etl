"""Regions dataset adjusted for Grapher.

This dataset should populate the charts of regions (defined by OWID and other institutions) published in:
https://ourworldindata.org/world-region-map-definitions

"""

import json
from typing import cast

# import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Get the current year from the step version.
# Note that this year will be ignored in the charts, but it is required by grapher.
CURRENT_YEAR = int(paths.version.split("-")[0])


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
    ds_grapher = paths.create_dataset(
        tables=[tb_regions], default_metadata=ds_garden.metadata, check_variables_metadata=True
    )
    ds_grapher.save()


def process_un_definitions(tb) -> Table:
    """This functions will be deprecated.

    Its tasks were:
    - [no longer doing] Propagate definitions downstream. (Let's not do it, might be confusing)
    - [will stop doing soon] Create new definition "un_region" that replaces "Americas" with "Latin America and the Caribbean" and "Northern America".
    """
    # Propagate definitions downstream. (Let's not do it, might be confusing)
    # for i in range(1, 3):
    #     mask = tb[f"un_m49_{i+1}_region"].isna()
    #     tb.loc[mask, f"un_m49_{i+1}_region"] = tb.loc[mask, f"un_m49_{i}_region"]

    # Create new definition
    ## Get rows where "Americas" should be replaced with "Latin America and the Caribbean" and "Northern America"
    mask = tb["un_m49_2_region"].str.contains("America", na=False)
    assert tb.loc[mask, "un_m49_2_region"].nunique() == 2, "There should be only two Americas in UN M49 level 2."
    ## Create new column
    tb.loc[:, "un_region"] = tb.loc[:, "un_m49_1_region"].copy()
    tb.loc[mask, "un_region"] = tb.loc[mask, "un_m49_2_region"].copy()
    return cast(Table, tb)
