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

# Possible region types considered.
REGION_TYPES = ["country", "continent", "aggregate", "other"]

# Manually list all inhabited continents and their member countries.
# This is not a trivial task, since there are many regions that are unclear whether they are independent or whether
# they should already be counted within another country.
# This list is mainlly used to create aggregates (i.e. to add up data from different countries to obtain data for
# continents).
continent_members = {
    "Africa": [
        "Algeria",
        "Angola",
        "Benin",
        "Botswana",
        "Burkina Faso",
        "Burundi",
        "Cameroon",
        "Cape Verde",
        "Central African Republic",
        "Chad",
        "Comoros",
        "Congo",
        "Cote d'Ivoire",
        "Democratic Republic of Congo",
        "Djibouti",
        "Egypt",
        "Equatorial Guinea",
        "Eritrea",
        "Eritrea and Ethiopia",
        "Eswatini",
        "Ethiopia",
        "Gabon",
        "Gambia",
        "Ghana",
        "Guinea",
        "Guinea-Bissau",
        "Kenya",
        "Lesotho",
        "Liberia",
        "Libya",
        "Madagascar",
        "Malawi",
        "Mali",
        "Mauritania",
        "Mauritius",
        "Mayotte",
        "Morocco",
        "Mozambique",
        "Namibia",
        "Niger",
        "Nigeria",
        "Reunion",
        "Rwanda",
        "Saint Helena",
        "Sao Tome and Principe",
        "Senegal",
        "Seychelles",
        "Sierra Leone",
        "Somalia",
        # 'Somaliland',
        "South Africa",
        "South Sudan",
        "Sudan",
        "Tanzania",
        "Togo",
        "Tunisia",
        "Uganda",
        "Western Sahara",
        "Zambia",
        # 'Zanzibar',
        "Zimbabwe",
    ],
    "Asia": [
        # 'Abkhazia',
        "Afghanistan",
        # 'Akrotiri and Dhekelia',
        "Armenia",
        "Azerbaijan",
        "Bahrain",
        "Bangladesh",
        "Bhutan",
        "British Indian Ocean Territory",
        "Brunei",
        "Cambodia",
        "China",
        "Christmas Island",
        "Cocos Islands",
        "East Timor",
        "Georgia",
        "Hong Kong",
        "India",
        "Indonesia",
        "Iran",
        "Iraq",
        "Israel",
        "Japan",
        "Jordan",
        "Kazakhstan",
        "Kuwait",
        "Kyrgyzstan",
        "Laos",
        "Lebanon",
        "Macao",
        "Malaysia",
        "Maldives",
        "Mongolia",
        "Myanmar",
        # 'Nagorno-Karabakh',
        "Nepal",
        "North Korea",
        # 'Northern Cyprus',
        "Oman",
        "Pakistan",
        "Palestine",
        "Philippines",
        "Qatar",
        "Republic of Vietnam",
        "Saudi Arabia",
        "Singapore",
        "South Korea",
        # 'South Ossetia',
        "Sri Lanka",
        "Syria",
        "Taiwan",
        "Tajikistan",
        "Thailand",
        # 'Timor',
        "Turkey",
        "Turkmenistan",
        "United Arab Emirates",
        "United Korea",
        "Uzbekistan",
        "Vietnam",
        "Yemen",
        "Yemen Arab Republic",
        "Yemen People's Republic",
    ],
    "Europe": [
        "Aland Islands",
        "Albania",
        "Andorra",
        "Austria",
        "Austria-Hungary",
        # 'Baden',
        # 'Bavaria',
        "Belarus",
        "Belgium",
        "Bosnia and Herzegovina",
        "Bulgaria",
        # 'Channel Islands',
        "Croatia",
        "Czechia",
        "Czechoslovakia",
        "Denmark",
        "East Germany",
        "Estonia",
        "Faeroe Islands",
        "Finland",
        "France",
        "Germany",
        "Gibraltar",
        "Greece",
        "Guernsey",
        # 'Hanover',
        # 'Hesse Electoral',
        # 'Hesse Grand Ducal',
        "Hungary",
        "Iceland",
        "Ireland",
        "Isle of Man",
        "Italy",
        "Jersey",
        "Kosovo",
        "Latvia",
        "Liechtenstein",
        "Lithuania",
        "Luxembourg",
        "Malta",
        # 'Mecklenburg Schwerin',
        # 'Modena',
        "Moldova",
        "Monaco",
        "Montenegro",
        "Netherlands",
        "North Macedonia",
        "Norway",
        # 'Parma',
        "Poland",
        "Portugal",
        "Romania",
        "Russia",
        "San Marino",
        # 'Saxony',
        "Serbia",
        "Slovakia",
        "Slovenia",
        "Spain",
        # 'Svalbard and Jan Mayen',
        "Sweden",
        "Switzerland",
        # 'Tuscany',
        # 'Two Sicilies',
        "Ukraine",
        "United Kingdom",
        "Vatican",
        "West Germany",
        # 'Wuerttemburg',
        "Cyprus",
        "USSR",
        "Serbia and Montenegro",
        # 'Transnistria',
        "Yugoslavia",
        "Serbia excluding Kosovo",
    ],
    "North America": [
        "Anguilla",
        "Antigua and Barbuda",
        "Aruba",
        "Bahamas",
        "Barbados",
        "Belize",
        "Bermuda",
        "Bonaire Sint Eustatius and Saba",
        "British Virgin Islands",
        "Canada",
        "Cayman Islands",
        "Costa Rica",
        "Cuba",
        "Curacao",
        "Dominica",
        "Dominican Republic",
        "El Salvador",
        "Greenland",
        "Grenada",
        "Guadeloupe",
        "Guatemala",
        "Haiti",
        "Honduras",
        "Jamaica",
        "Martinique",
        "Mexico",
        "Montserrat",
        "Netherlands Antilles",
        "Nicaragua",
        "Panama",
        "Puerto Rico",
        "Saint Barthelemy",
        "Saint Kitts and Nevis",
        "Saint Lucia",
        "Saint Martin (French part)",
        "Saint Pierre and Miquelon",
        "Saint Vincent and the Grenadines",
        "Sint Maarten (Dutch part)",
        "Trinidad and Tobago",
        "Turks and Caicos Islands",
        "United States",
        "United States Virgin Islands",
    ],
    "Oceania": [
        "American Samoa",
        "Australia",
        "Cook Islands",
        "Fiji",
        "French Polynesia",
        "Guam",
        "Kiribati",
        "Marshall Islands",
        "Micronesia (country)",
        "Nauru",
        "New Caledonia",
        "New Zealand",
        "Niue",
        "Norfolk Island",
        "Northern Mariana Islands",
        "Palau",
        "Papua New Guinea",
        "Pitcairn",
        "Samoa",
        "Solomon Islands",
        "Tokelau",
        "Tonga",
        "Tuvalu",
        "United States Minor Outlying Islands",
        "Vanuatu",
        "Wallis and Futuna",
    ],
    "South America": [
        "Argentina",
        "Bolivia",
        "Brazil",
        "Chile",
        "Colombia",
        "Ecuador",
        "Falkland Islands",
        "French Guiana",
        "Guyana",
        "Paraguay",
        "Peru",
        "South Georgia and the South Sandwich Islands",
        "Suriname",
        "Uruguay",
        "Venezuela",
    ],
}

# Define the list of continents.
# These regions will be assigned the region type "continent".
continents = sorted(continent_members)

# Manually define other aggregate regions.
aggregate_regions = {
    "Channel Islands": [
        "Guernsey",
        "Jersey",
    ],
    "European Union (27)": [
        "Austria",
        "Belgium",
        "Bulgaria",
        "Croatia",
        "Cyprus",
        "Czechia",
        "Denmark",
        "Estonia",
        "Finland",
        "France",
        "Germany",
        "Greece",
        "Hungary",
        "Ireland",
        "Italy",
        "Latvia",
        "Lithuania",
        "Luxembourg",
        "Malta",
        "Netherlands",
        "Poland",
        "Portugal",
        "Romania",
        "Slovakia",
        "Slovenia",
        "Spain",
        "Sweden",
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
    "World": [
        "Africa",
        # 'Antarctica',
        "Asia",
        "Europe",
        "North America",
        "Oceania",
        "South America",
    ],
}

# Define the list of continents.
# These regions will be assigned the region type "aggregate".
aggregates = sorted(aggregate_regions)

# Manually list regions whose official definition is unclear.
# They will be assigned the region type "other".
other_regions = [
    "Abkhazia",
    "Akrotiri and Dhekelia",
    "Nagorno-Karabakh",
    "Northern Cyprus",
    "Serbia excluding Kosovo",
    "Somaliland",
    "South Ossetia",
    "Svalbard and Jan Mayen",
    "Transnistria",
    "United States Minor Outlying Islands",
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

# Manually define "related member" territories, which includes all dependent and overseas territories.
# Here, the definition of member is vague.
# The status of some of these regions are unclear or contested, and by defining these dependencies we are not
# making any political statement.
# This list will only be used to raise warnings on potential overlaps in the data.
# For example, if there is data for both China and Hong Kong, it would be good to raise the warning that we may
# be double-counting Hong Kong's data (as some data providers include Hong-Kong as part of China).
related_territories = {
    "Australia": [
        "Christmas Island",
        "Heard Island and McDonald Islands",
        "Norfolk Island",
    ],
    "Azerbaijan": [
        "Nagorno-Karabakh",
    ],
    "China": [
        "Hong Kong",
        "Macao",
    ],
    "Cyprus": [
        "Northern Cyprus",
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
    "Servia": [
        "Kosovo",
        "Serbia excluding Kosovo",
    ],
    "Somalia": [
        "Somaliland",
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


def _explode_region_list(region, column):
    text = f"  {column}:\n"
    for member in sorted(region[column]):
        text += f"    - {json.dumps(member)}\n"

    return text


def _create_yaml_content_from_df(df_main: pd.DataFrame) -> str:
    # Transform the rows in the dataframe into a good-looking yaml file (yaml_dump doesn't do a good enough job).
    text = f"""\
# Region definitions (see more details in the description of the 'regions' garden step).
# Each region must contain the following fields:
# code: Region code (unique for each region).
# name: Region name.
#
# Additionally, each region can contain the following fields:
# short_name: Short version of the region name. If not given, 'name' will be used.
# region_type: Region type ({', '.join(REGION_TYPES)}). If not given, 'country' will be used.
# defined_by: Institution that used the region in a dataset. If not given, 'owid' will be used.
# is_historical: True if region does not exist anymore. If not given, False will be used.
# end_year: Last year when a historical region existed. If not given, pd.NA will be used.
# successors: List of successors of a historical region. If not given, an empty list will be used.
# members: List of members of the region. If not given, an empty list will be used.
# aliases: List of alternative names for the region. If not given, an empty list will be used.
# related: List of related members of the region. If not given, an empty list will be used.\n"""
    for region in df_main.sort_values("name").to_dict(orient="records"):
        # Fill in mandatory fields.
        text += f'- code: "{region["code"]}"\n'
        text += f'  name: "{region["name"]}"\n'

        # Add short name only if different from name.
        if region["name"] != region["short_name"]:
            text += f'  short_name: "{region["short_name"]}"\n'

        # Add region_type only if different from the most commont one (country).
        if region["region_type"] != "country":
            text += f'  region_type: "{region["region_type"]}"\n'

        # Add defined_by only if different from the most commont one (owid).
        if region["defined_by"] != "owid":
            text += f'  defined_by: "{region["defined_by"]}"\n'

        # Add is_historical only if it's True.
        if region["is_historical"]:
            text += f"  is_historical: {region['is_historical']}\n"

        # Add aliases only if there is any.
        if len(region["aliases"]) > 0:
            text += _explode_region_list(region, "aliases")

        # Add members only if there is any.
        if len(region["members"]) > 0:
            text += _explode_region_list(region, "members")

        # Add successors only if there is any.
        if region["is_historical"] or len(region["successors"]) > 0:
            assert region["is_historical"]
            assert len(region["successors"]) > 0
            assert region["end_year"]
            text += f"  end_year: {region['end_year']}\n"
            text += _explode_region_list(region, "successors")

        # Add related members only if there is any.
        if len(region["related"]) > 0:
            text += _explode_region_list(region, "related")

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
    # New column "related" will not be empty if there are related member regions that, for some datasets, could lead
    # to double-counting. For example, "China" has the related member "Hong-Kong", since some data providers include
    # Hong-Kong's data in China, and also provide data for Hong-Kong.
    df["related"] = pd.NA
    # Empty the existing column for members.
    df["members"] = pd.NA

    # Apply some minor corrections to existing data.

    # Add a short name for DRC.
    df.loc[df["code"] == "COD", "short_name"] = "DR Congo"

    # Assign TLS the right name and short name, which is East Timor.
    df.loc[df["code"] == "TLS", "name"] = "East Timor"
    df.loc[df["code"] == "TLS", "short_name"] = "East Timor"

    # Add "Caribbean Netherlands" to the list of existing aliases of the BES islands.
    df.loc[df["code"] == "BES", "aliases"] = json.dumps(
        json.loads(df.loc[df["code"] == "BES", "aliases"].item()) + ["Caribbean Netherlands"]
    )

    # Remove entry for "Caribbean Netherlands".
    df = df[df["code"] != "OWID_NLC"].reset_index(drop=True)

    # Edit region type of continents.
    for continent in continents:
        df.loc[df["name"] == continent, "region_type"] = "continent"

    # Edit region type of special regions.
    for region in aggregates:
        df.loc[df["name"] == region, "region_type"] = "aggregate"

    # Edit region type of other regions.
    for region in other_regions:
        df.loc[df["name"] == region, "region_type"] = "other"

    # Remove unnecessary regions.
    df = df[~df["name"].isin(regions_to_remove)].reset_index(drop=True)

    # Sanity check.
    error = "There are multiple regions with the same name (which is possible, but at least for now it shouldn't be)."
    assert df[df["name"].duplicated()].empty, error

    # Edit the rows corresponding to historical regions.
    for historical_region in historical_regions:
        end_year = historical_regions[historical_region]["end_year"]
        successors = historical_regions[historical_region]["successors"]
        successor_codes = sorted([df[df["name"] == successor]["code"].item() for successor in successors])
        region_sel = df["name"] == historical_region
        df.loc[region_sel, "end_year"] = end_year
        df.loc[region_sel, "successors"] = json.dumps(successor_codes)
        df.loc[region_sel, "is_historical"] = True

    # Add members to continents and geographical regions.
    all_members = {**continent_members, **aggregate_regions}
    for region in all_members:
        members = all_members[region]
        member_codes = sorted([df[df["name"] == member]["code"].item() for member in members])
        df.loc[df["name"] == region, "members"] = json.dumps(member_codes)

    # Add any related territories.
    all_members = {**related_territories}
    for region in all_members:
        members = all_members[region]
        member_codes = sorted([df[df["name"] == member]["code"].item() for member in members])
        df.loc[df["name"] == region, "related"] = json.dumps(member_codes)

    # Convert the columns of strings into columns of lists of strings.
    df["aliases"] = df["aliases"].fillna("[]").apply(eval)
    df["members"] = df["members"].fillna("[]").apply(eval)
    df["related"] = df["related"].fillna("[]").apply(eval)
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
            "related",
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
