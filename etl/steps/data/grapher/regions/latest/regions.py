"""Regions dataset adjusted for Grapher. It is published as CSV so that it can
be easily loaded in Grapher codebase.

This dataset is not meant to be imported to MySQL and is excluded from automatic deployment.
"""

import ast
import re

import pandas as pd
from owid.catalog import Dataset, Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


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
    # Load garden dataset.
    ds_garden: Dataset = paths.load_dependency("regions")

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

    #
    # Process data.
    #
    # Drop unneeded columns
    regions: Table = regions.drop(columns=["defined_by"])  # type: ignore

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

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[regions], default_metadata=ds_garden.metadata, formats=["csv"], run_grapher_checks=False
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()


def slugify(name):
    return re.sub(r"[^\w\-]", "", name.lower().replace(" ", "-"))
