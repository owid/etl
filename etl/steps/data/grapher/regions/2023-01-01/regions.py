"""Regions dataset adjusted for Grapher. It is published as CSV so that it can
be easily loaded in Grapher codebase.

This dataset is not meant to be imported to MySQL and is excluded from automatic deployment.
"""

import re

from owid.catalog import Dataset, Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Legacy slugs for countries that have changed their name.
LEGACY_SLUGS = {
    "CZE": "czech-republic",
    "MKD": "macedonia",
    "SWZ": "swaziland",
    "TLS": "timor",
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
    "ATA",
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
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden: Dataset = paths.load_dependency("regions")

    # Read tables from regions dataset.
    regions = ds_garden["definitions"]
    legacy_codes = ds_garden["legacy_codes"]
    members_expanded = ds_garden["members"]

    #
    # Process data.
    #

    # Rename definitions to regions.
    regions.metadata.short_name = "regions"

    # Drop unneeded columns
    regions: Table = regions.drop(columns=["is_historical", "defined_by"])  # type: ignore

    # Create slugs for all countries and keep track of legacy slugs.
    is_country = regions["region_type"] == "country"
    regions.loc[is_country, "slug"] = regions.loc[is_country, "name"].astype(str).map(slugify)
    regions["slug"].update(LEGACY_SLUGS)

    # Countries that are mappable in Grapher.
    regions["is_mappable"] = regions.index.isin(MAPPABLE_COUNTRIES)

    # Countries that have a country page in Grapher.
    regions["omit_country_page"] = regions.index.isin(NO_COUNTRY_PAGE)

    # Add ISO alpha 2 codes.
    regions["short_code"] = legacy_codes["iso_alpha2"]

    # Add members
    regions["members"] = members_expanded.groupby("code").agg(list)

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[regions], default_metadata=ds_garden.metadata, formats=["csv"])

    # Save changes in the new grapher dataset.
    ds_grapher.save()


def slugify(name):
    return re.sub(r"[^\w\-]", "", name.lower().replace(" ", "-"))
