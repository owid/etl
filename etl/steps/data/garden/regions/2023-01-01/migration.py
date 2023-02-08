"""Migrate countries-regions data from the garden/reference dataset to a new dataset.

This script needs to be used only once before creating the regions dataset for the first time.
It will generate:
* A yaml file of region definitions.
* A csv file of additional region codes.

Once those files exist, subsequent versions of the dataset should simply duplicate those yaml and csv files, and make
manual modifications and additions to those files.

"""

import json
from pathlib import Path

import pandas as pd
from owid import catalog

from etl.paths import DATA_DIR

# Path to current folder.
CURRENT_DIR = Path(__file__).parent

# Paths to output files.
REGION_DEFINITIONS_OUTPUT_FILE = CURRENT_DIR / "regions.yml"
REGION_CODES_OUTPUT_FILE = CURRENT_DIR / "regions.codes.csv"

# Manually define the list of continents.
continents = [
    "Africa",
    "Antarctica",
    "Asia",
    "Europe",
    "North America",
    "Oceania",
    "South America",
]

# Manually define the list of other special regions and aggregates.
aggregates = [
    # Important aggregates.
    "European Union (27)",
    "World",
    # Geographical aggregates.
    "Channel Islands",
    "Melanesia",
    "Polynesia",
    # Other special regions.
    "Serbia excluding Kosovo",
    "Svalbard and Jan Mayen",
    "United States Minor Outlying Islands",
]

# Manually list regions whose status is contested or are not considered official countries.
contested_regions = [
    "Nagorno-Karabakh",
    "Northern Cyprus",
    "Abkhazia",
    "South Ossetia",
    "Transnistria",
    "Western Sahara",
    "Somaliland",
    "Kosovo",
]

# Manually list regions to remove.
regions_to_remove = [
    # Is there any reason to keep these historical regions?
    # Some of then correspond now to German and Italian sub-country regions.
    "Baden",
    "Bavaria",
    "Hanover",
    "Hesse Electoral",
    "Hesse Grand Ducal",
    "Mecklenburg Schwerin",
    "Saxony",
    "Wuerttemburg",
    "Modena",
    "Tuscany",
    "Two Sicilies",
    "Parma",
]

# Manually define historical regions, their end year, and successors.
historical_regions = {
    "Austria-Hungary": {
        "end_year": 1918,
        "successors": [
            "Austria",
            "Hungary",
            "Czechoslovakia",
            "Poland",
            "Ukraine",
            "Yugoslavia",
            "Romania",
            "Italy",
            "China",
        ],
    },
    "Czechoslovakia": {
        "end_year": 1992,
        "successors": [
            "Czechia",
            "Slovakia",
        ],
    },
    "East Germany": {
        "end_year": 1990,
        "successors": [
            "Germany",
        ],
    },
    "Eritrea and Ethiopia": {
        "end_year": 1993,
        "successors": [
            "Eritrea",
            "Ethiopia",
        ],
    },
    "Netherlands Antilles": {
        "end_year": 2010,
        "successors": [
            "Aruba",
            "Bonaire Sint Eustatius and Saba",
            "Curacao",
            "Sint Maarten (Dutch part)",
        ],
    },
    "Republic of Vietnam": {
        "end_year": 1975,
        "successors": [
            "Vietnam",
        ],
    },
    "Serbia and Montenegro": {
        "end_year": 2006,
        "successors": [
            "Serbia",
            "Montenegro",
        ],
    },
    "Sudan (former)": {
        "end_year": 2011,
        "successors": [
            "Sudan",
            "South Sudan",
        ],
    },
    "United Korea": {
        "end_year": 1948,
        "successors": [
            "North Korea",
            "South Korea",
        ],
    },
    "USSR": {
        "end_year": 1991,
        "successors": [
            "Lithuania",
            "Georgia",
            "Estonia",
            "Latvia",
            "Ukraine",
            "Moldova",
            "Kyrgyzstan",
            "Uzbekistan",
            "Tajikistan",
            "Armenia",
            "Azerbaijan",
            "Turkmenistan",
            "Belarus",
            "Russia",
            "Kazakhstan",
        ],
    },
    "West Germany": {
        "end_year": 1990,
        "successors": [
            "Germany",
        ],
    },
    "Yemen Arab Republic": {
        "end_year": 1990,
        "successors": [
            "Yemen",
        ],
    },
    "Yemen People's Republic": {
        "end_year": 1990,
        "successors": [
            "Yemen",
        ],
    },
    "Yugoslavia": {
        "end_year": 1992,
        "successors": [
            "Bosnia and Herzegovina",
            "Croatia",
            "Kosovo",
            "Montenegro",
            "North Macedonia",
            "Serbia",
            "Slovenia",
        ],
    },
}

# Manually define related territories, which includes all dependent and overseas territories.
# For more specific definitions, see: https://en.wikipedia.org/wiki/Dependent_territory
# The related territories will be considered countries unless stated otherwise later.
related_territories = {
    "Australia": [
        "Christmas Island",
        "Heard Island and McDonald Islands",
        "Norfolk Island",
    ],
    "China": [
        "Hong Kong",
        "Macao",
    ],
    "Denmark": [
        "Faeroe Islands",
    ],
    "Finland": [
        "Aland Islands",
    ],
    "France": [
        "French Guiana",
        "French Polynesia",
        "French Southern Territories",
        "Guadeloupe",
        "Martinique",
        "Mayotte",
        "New Caledonia",
        "Reunion",
        "Saint Barthelemy",
        "Saint Martin (French part)",
        "Saint Pierre and Miquelon",
        "Wallis and Futuna",
    ],
    "Netherlands": [
        "Aruba",
        "Curacao",
        "Sint Maarten (Dutch part)",
    ],
    "New Zealand": [
        "Tokelau",
    ],
    "Norway": [
        "Bouvet Island",
        "Svalbard and Jan Mayen",
    ],
    "Palestine": [
        "Gaza Strip",
    ],
    "Tanzania": [
        "Zanzibar",
    ],
    "United Kingdom": [
        "Akrotiri and Dhekelia",
        "Anguilla",
        "Bermuda",
        "British Indian Ocean Territory",
        "British Virgin Islands",
        "Cayman Islands",
        "Falkland Islands",
        "Gibraltar",
        "Guernsey",
        "Isle of Man",
        "Jersey",
        "Montserrat",
        "Pitcairn",
        "Saint Helena",
        "South Georgia and the South Sandwich Islands",
        "Turks and Caicos Islands",
    ],
    "United States": [
        "American Samoa",
        "Guam",
        "Northern Mariana Islands",
        "Puerto Rico",
        "United States Minor Outlying Islands",
        "United States Virgin Islands",
    ],
}

# Manually define additional possible overlaps between regions and members.
# The status of some of these regions are unclear or contested, and by defining these dependencies we are not
# making any political statement.
# We simply define possible overlaps between geographical regions that can be found in datasets, to ensure we never
# double-count the contribution from those regions when creating aggregate data.
region_members_contested = {
    "Azerbaijan": [
        "Nagorno-Karabakh",
    ],
    "Cyprus": [
        "Northern Cyprus",
    ],
    "Georgia": [
        "Abkhazia",
        "South Ossetia",
    ],
    "Moldova": [
        "Transnistria",
    ],
    "Morocco": [
        "Western Sahara",
    ],
    "Somalia": [
        "Somaliland",
    ],
    "Servia": [
        "Kosovo",
        "Serbia excluding Kosovo",
    ],
}

# Manually define other geographical regions.
geographical_regions = {
    "Channel Islands": [
        "Guernsey",
        "Jersey",
    ],
    "Melanesia": [
        "Fiji",
        "Papua New Guinea",
        "Solomon Islands",
        "Vanuatu",
    ],
    "Polynesia": [
        "American Samoa",
        "Cook Islands",
        "French Polynesia",
        "New Zealand",
        "Niue",
        "Norfolk Island",
        "Pitcairn",
        "Samoa",
        "Tokelau",
        "Tonga",
        "Tuvalu",
        "Wallis and Futuna",
    ],
}


def _create_yaml_content_from_df(df_main: pd.DataFrame) -> str:
    # Transform the rows in the dataframe into a good-looking yaml file (yaml_dump doesn't do a good enough job).
    text = ""
    for region in df_main.to_dict(orient="records"):
        field = "code"
        text += f'- code: "{region[field]}"\n'
        for field in ["name", "short_name", "region_type", "defined_by"]:
            text += f'  {field}: "{region[field]}"\n'
        text += f"  is_historical: {region['is_historical']}\n"
        if len(region["aliases"]) > 0:
            text += f"  aliases: {region['aliases']}\n"
        if len(region["members"]) > 0:
            text += f"  members: {region['members']}\n"
        if region["is_historical"] or len(region["successors"]) > 0:
            assert region["is_historical"]
            assert len(region["successors"]) > 0
            assert region["end_year"]
            text += f"  end_year: {region['end_year']}\n"
            text += f"  successors: {region['successors']}\n"
        text += "\n"

    return text


def main():
    # Load the old countries-regions table from the reference dataset.
    reference = catalog.Dataset(DATA_DIR / "garden/reference")

    # Initialise a dataframe that will gather all necessary data about countries and regions.
    df = reference["countries_regions"].reset_index()

    # Add new columns.
    # New column "short_name" will be a shorter version of a region's name, to be used for example in certain
    # visualizations with limited space.
    df["short_name"] = df["name"].copy()
    # New column "region_type" will help distinguish between countries, continents, sub-country regions, or other special
    # kinds of regions.
    # By default, assume all regions are countries (since that would be the majority of cases).
    df["region_type"] = "country"
    # New column "is_historical" will be True for those regions that do not exist today.
    # By default, assume all rows correspond to non-historical regions.
    df["is_historical"] = False
    # New column "defined_by" will help distinguish between regions defined by OWID, and by other institutions.
    df["defined_by"] = "owid"
    # New column "end_year" will not be empty for historical regions, and will inform of the last year of the region.
    df["end_year"] = pd.NA
    # New column "successors" will not be empty for historical regions, and will give the list of regions that occupied
    # the same geographical land after the region stoppted existing.
    df["successors"] = pd.NA

    # Apply some minor corrections to existing data.

    # Add a short name for DRC.
    df.loc[df["code"] == "COD", "short_name"] = "DR Congo"

    # Assign TLS the right name and short name, which is East Timor.
    df.loc[df["code"] == "TLS", "name"] = "East Timor"
    df.loc[df["code"] == "TLS", "short_name"] = "East Timor"

    # Add a short name for DRC.
    df.loc[df["code"] == "COD", "short_name"] = "DR Congo"

    # Assign TLS the right name and short name, which is East Timor.
    df.loc[df["code"] == "TLS", "name"] = "East Timor"
    df.loc[df["code"] == "TLS", "short_name"] = "East Timor"

    # Edit region type of continents.
    for continent in continents:
        df.loc[df["name"] == continent, "region_type"] = "continent"

    # Edit region type of special regions.
    for region in aggregates:
        df.loc[df["name"] == region, "region_type"] = "aggregate"

    # Edit region type of contested regions.
    for region in contested_regions:
        df.loc[df["name"] == region, "region_type"] = "contested"

    # Remove unnecessary regions.
    df = df[~df["name"].isin(regions_to_remove)].reset_index(drop=True)

    # Edit the rows corresponding to historical regions.
    for historical_region in historical_regions:
        end_year = historical_regions[historical_region]["end_year"]
        successors = historical_regions[historical_region]["successors"]
        successor_codes = sorted([df[df["name"] == successor]["code"].item() for successor in successors])
        region_sel = df["name"] == historical_region
        df.loc[region_sel, "end_year"] = end_year
        df.loc[region_sel, "successors"] = json.dumps(successor_codes)
        df.loc[region_sel, "is_historical"] = True

    # Add members, meaning any related territories, geographical members and contested regions.
    all_members = {**related_territories, **region_members_contested, **geographical_regions}
    for region in all_members:
        members = all_members[region]
        member_codes = sorted([df[df["name"] == member]["code"].item() for member in members])
        df.loc[df["name"] == region, "members"] = json.dumps(member_codes)

    # Convert the columns of strings into columns of lists of strings.
    df["aliases"] = df["aliases"].fillna("[]").apply(eval)
    df["members"] = df["members"].fillna("[]").apply(eval)
    df["successors"] = df["successors"].fillna("[]").apply(eval)

    # Define an additional dataset of codes.
    df_codes = df[
        [
            "code",
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
            "wikidata_uri",
        ]
    ].reset_index(drop=True)

    # Ensure numeric codes are integer.
    df_codes = df_codes.astype(
        {code: pd.Int64Dtype() for code in ["cow_code", "imf_code", "legacy_country_id", "legacy_entity_id"]}
    )

    # All wikidata URIs start with http://www.wikidata.org/entity/ (except one, https://www.wikidata.org/wiki/Q39760,
    # but https://www.wikidata.org/entity/Q39760 leads to the same page). Therefore, we can simply store the code.
    df_codes["wikidata_code"] = df_codes["wikidata_uri"].str.split("/").str[-1]
    df_codes = df_codes.drop(columns=["wikidata_uri"])

    # Select the main columns to keep for the main definitions dataset.
    df_main = df[
        [
            "code",
            "name",
            "short_name",
            "aliases",
            "members",
            "region_type",
            "is_historical",
            "end_year",
            "successors",
            "defined_by",
        ]
    ]

    # Create output file of region definitions.
    text = _create_yaml_content_from_df(df_main=df_main)
    with open(REGION_DEFINITIONS_OUTPUT_FILE, "w") as _output_file:
        _output_file.write(text)

    # Create output file of region codes.
    pd.DataFrame(df_codes).to_csv(str(REGION_CODES_OUTPUT_FILE), index=False)


if __name__ == "__main__":
    main()
