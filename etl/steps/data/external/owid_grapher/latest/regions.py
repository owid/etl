"""Regions dataset adjusted for Grapher. It is published as CSV so that it can
be easily loaded in Grapher codebase.

This dataset is not meant to be imported to MySQL and is excluded from automatic deployment.
"""

import ast
import re

import pandas as pd
from owid.catalog import Table
from owid.catalog.processing import concat

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

INCOME_GROUPS_ENTITY_CODES = {
    "Low-income countries": "OWID_LIC",
    "Lower-middle-income countries": "OWID_LMC",
    "Upper-middle-income countries": "OWID_UMC",
    "High-income countries": "OWID_HIC",
}


# Countries that are mappable in Grapher.
MAPPABLE_COUNTRIES = [
    "AFG",
    "AGO",
    "ALB",
    "AND",
    "ARE",
    "ARG",
    "ARM",
    "ATF",
    "ATG",
    "AUS",
    "AUT",
    "AZE",
    "BDI",
    "BEL",
    "BEN",
    "BFA",
    "BGD",
    "BGR",
    "BHR",
    "BHS",
    "BIH",
    "BLR",
    "BLZ",
    "BOL",
    "BRA",
    "BRB",
    "BRN",
    "BTN",
    "BWA",
    "CAF",
    "CAN",
    "CHE",
    "CHL",
    "CHN",
    "CIV",
    "CMR",
    "COD",
    "COG",
    "COL",
    "COM",
    "CPV",
    "CRI",
    "CUB",
    "CYP",
    "CZE",
    "DEU",
    "DJI",
    "DMA",
    "DNK",
    "DOM",
    "DZA",
    "ECU",
    "EGY",
    "ERI",
    "ESH",
    "ESP",
    "EST",
    "ETH",
    "FIN",
    "FJI",
    "FRA",
    "FSM",
    "GAB",
    "GBR",
    "GEO",
    "GHA",
    "GIN",
    "GMB",
    "GNB",
    "GNQ",
    "GRC",
    "GRD",
    "GRL",
    "GTM",
    "GUF",
    "GUY",
    "HND",
    "HRV",
    "HTI",
    "HUN",
    "IDN",
    "IND",
    "IRL",
    "IRN",
    "IRQ",
    "ISL",
    "ISR",
    "ITA",
    "JAM",
    "JOR",
    "JPN",
    "KAZ",
    "KEN",
    "KGZ",
    "KHM",
    "KIR",
    "KNA",
    "KOR",
    "KWT",
    "LAO",
    "LBN",
    "LBR",
    "LBY",
    "LCA",
    "LIE",
    "LKA",
    "LSO",
    "LTU",
    "LUX",
    "LVA",
    "MAR",
    "MCO",
    "MDA",
    "MDG",
    "MDV",
    "MEX",
    "MHL",
    "MKD",
    "MLI",
    "MLT",
    "MMR",
    "MNE",
    "MNG",
    "MOZ",
    "MRT",
    "MUS",
    "MWI",
    "MYS",
    "NAM",
    "NCL",
    "NER",
    "NGA",
    "NIC",
    "NLD",
    "NOR",
    "NPL",
    "NRU",
    "NZL",
    "OMN",
    "OWID_KOS",
    "PAK",
    "PAN",
    "PER",
    "PHL",
    "PLW",
    "PNG",
    "POL",
    "PRI",
    "PRK",
    "PRT",
    "PRY",
    "PSE",
    "QAT",
    "ROU",
    "RUS",
    "RWA",
    "SAU",
    "SDN",
    "SEN",
    "SGP",
    "SLB",
    "SLE",
    "SLV",
    "SMR",
    "SOM",
    "SRB",
    "SSD",
    "STP",
    "SUR",
    "SVK",
    "SVN",
    "SWE",
    "SWZ",
    "SYC",
    "SYR",
    "TCD",
    "TGO",
    "THA",
    "TJK",
    "TKM",
    "TLS",
    "TON",
    "TTO",
    "TUN",
    "TUR",
    "TUV",
    "TWN",
    "TZA",
    "UGA",
    "UKR",
    "URY",
    "USA",
    "UZB",
    "VCT",
    "VEN",
    "VNM",
    "VUT",
    "WSM",
    "YEM",
    "ZAF",
    "ZMB",
    "ZWE",
]

# Countries with no country page in Grapher.
NO_COUNTRY_PAGE = [
    "ALA",
    "ANT",
    "ATF",
    "BES",
    "BVT",
    "CCK",
    "COK",
    "CUW",
    "ESH",
    "GGY",
    "GLP",
    "HMD",
    "IOT",
    "MAF",
    "MSR",
    "SGS",
    "SHN",
    "SJM",
    "UMI",
    "WLF",
    "PS_GZA",
    "SXM",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load regions dataset.
    ds_garden = paths.load_dataset("regions")
    # Read tables from regions dataset.
    regions = ds_garden["regions"][["name", "short_name", "region_type", "is_historical", "defined_by"]]
    members = ds_garden["regions"][["members"]]
    legacy_codes = ds_garden["regions"][
        [
            "cow_code",
            "cow_letter",
            "imf_code",
            "iso_alpha2",
            "iso_alpha3",
            "kansas_code",
            "legacy_country_id",
            "legacy_entity_id",
            "marc_code",
            "ncd_code",
            "penn_code",
            "unctad_code",
            "wikidata_code",
        ]
    ]

    # Load income groups dataset and read table of latest income group classifications.
    ds_income_groups = paths.load_dataset("income_groups")
    tb_income_groups = ds_income_groups["income_groups_latest"].reset_index()

    #
    # Process data.
    #
    # Create slugs for all countries and keep track of legacy slugs.
    regions["slug"] = regions["name"].astype(str).map(slugify)

    # Countries that are mappable in Grapher.
    regions["is_mappable"] = regions.index.isin(MAPPABLE_COUNTRIES)

    # Countries without country page in Grapher.
    regions["is_unlisted"] = regions.index.isin(NO_COUNTRY_PAGE)

    # Add ISO alpha 2 codes.
    regions["short_code"] = legacy_codes["iso_alpha2"]

    # Add members
    regions["members"] = (
        members["members"].astype(object).apply(lambda x: ";".join(ast.literal_eval(x)) if pd.notna(x) else "")
    )

    # Create a map: Region name -> Region code
    regions_by_name = regions.reset_index().set_index("name")["code"]

    # Add entity codes to countries in tb_income_groups.
    tb_income_groups["code"] = tb_income_groups["country"].map(regions_by_name.to_dict())

    # Check that all countries in the WB dataset have a code.
    assert len(tb_income_groups[tb_income_groups["code"].isna()]) == 0

    # Check that there are exactly these 4 income groups.
    assert set(tb_income_groups["classification"].unique()) == set(INCOME_GROUPS_ENTITY_CODES.keys())

    # Create a regions-like table for income groups.
    income_group_rows = []
    for income_group_name, income_group_code in INCOME_GROUPS_ENTITY_CODES.items():
        income_group_rows.append(
            {
                "code": income_group_code,
                "name": income_group_name,
                "short_name": income_group_name,
                "region_type": "income_group",
                "is_historical": False,
                "defined_by": "owid",
                "slug": slugify(income_group_name),
                "is_mappable": False,
                "is_unlisted": False,
                "short_code": None,
                "members": ";".join(tb_income_groups[tb_income_groups["classification"] == income_group_name]["code"]),
            }
        )
    tb_income_groups_as_regions = Table.from_records(income_group_rows)

    # Add rows for income groups to the regions table.
    tb = concat([regions.reset_index(), tb_income_groups_as_regions])

    # Set an appropriate index.
    tb = tb.set_index("code", verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new external dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb], formats=["csv"])
    ds_grapher.save()


def slugify(name):
    return re.sub(r"[^\w\-]", "", name.lower().replace(" ", "-"))
