"""Script to create a snapshot of dataset 'Energy production from fossil fuels'."""

import json
import re
import sys
from pathlib import Path
from time import sleep
from typing import List

import click
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from owid.datautils import dataframes
from tqdm.auto import tqdm

from etl.snapshot import add_snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Time (in seconds) to wait between consecutive queries.
TIME_BETWEEN_QUERIES = 1
# Maximum number of countries to fetch in each query.
MAX_NUM_COUNTRIES_PER_QUERY = 10

# Parameters for query.
SHIFT_URL = "https://www.theshiftdataportal.org/"
ENERGY_UNIT = "TWh"
# First year with data (make it any older year than 1900, in case they have data before this year).
START_YEAR = 1900
# Last year with data (make it an arbitrary future year, in case they have recent data).
END_YEAR = 2100
# List of energy sources.
ENERGY_SOURCES = ["coal", "gas", "oil"]
# List of countries and regions.
SHIFT_COUNTRIES = [
    "Afghanistan",
    "Africa",
    "Albania",
    "Algeria",
    "American Samoa",
    "Angola",
    "Antigua and Barbuda",
    "Argentina",
    "Armenia",
    "Aruba",
    "Asia and Oceania",
    "Australia",
    "Austria",
    "Azerbaijan",
    "Bahamas",
    "Bahrain",
    "Bangladesh",
    "Barbados",
    "Belarus",
    "Belgium",
    "Belize",
    "Benin",
    "Bermuda",
    "Bhutan",
    "Bolivia",
    "Bosnia and Herzegovina",
    "Botswana",
    "Brazil",
    "British Virgin Islands",
    "Brunei Darussalam",
    "Bulgaria",
    "Burkina Faso",
    "Burma",
    "Burundi",
    "Cambodia",
    "Cameroon",
    "Canada",
    "Cape Verde",
    "Cayman Islands",
    "Central African Republic",
    "Central and South America",
    "Chad",
    "Chile",
    "China",
    "Colombia",
    "Comoros",
    "Congo",
    "Cook Islands",
    "Costa Rica",
    "Croatia",
    "Cuba",
    "Cyprus",
    "Czech republic",
    "Czechia",
    "Czechoslovakia",
    "Democratic Republic of the Congo",
    "Denmark",
    "Djibouti",
    "Dominica",
    "Dominican Republic",
    "EU28",
    "Ecuador",
    "Egypt",
    "El Salvador",
    "Equatorial Guinea",
    "Eritrea",
    "Estonia",
    "Ethiopia",
    "Eurasia",
    "Europe",
    "Faeroe Islands",
    "Falkland Islands (Malvinas)",
    "Fiji",
    "Finland",
    "France",
    "French Guiana",
    "French Polynesia",
    "Gabon",
    "Gambia",
    "Georgia",
    "Germany",
    "Ghana",
    "Gibraltar",
    "Greece",
    "Greenland",
    "Grenada",
    "Guadeloupe",
    "Guam",
    "Guatemala",
    "Guinea",
    "Guinea-Bissau",
    "Guyana",
    "Haiti",
    "Honduras",
    "Hong Kong Special Administrative Region (China)",
    "Hungary",
    "Iceland",
    "Inde",
    "India",
    "Indonesia",
    "Iran",
    "Iraq",
    "Ireland",
    "Israel",
    "Italy",
    "Ivory Coast",
    "Jamaica",
    "Japan",
    "Jordan",
    "Kazakhstan",
    "Kenya",
    "Kiribati",
    "Kosovo",
    "Kuwait",
    "Kyrgyzstan",
    "Laos",
    "Latvia",
    "Lebanon",
    "Lesotho",
    "Liberia",
    "Libya",
    "Lithuania",
    "Luxembourg",
    "Macao Special Administrative Region (China)",
    "Macedonia",
    "Madagascar",
    "Malawi",
    "Malaysia",
    "Maldives",
    "Mali",
    "Malta",
    "Martinique",
    "Mauritania",
    "Mauritius",
    "Mexico",
    "Middle East",
    "Moldova",
    "Mongolia",
    "Montenegro",
    "Montserrat",
    "Morocco",
    "Mozambique",
    "NZ",
    "Namibia",
    "Nauru",
    "Nepal",
    "Netherlands",
    "Netherlands Antilles",
    "New Caledonia",
    "New Zealand",
    "Nicaragua",
    "Niger",
    "Nigeria",
    "Niue",
    "North America",
    "North Korea",
    "Northern Mariana Islands",
    "Norway",
    "OECD",
    "OPEC",
    "Oman",
    "Pakistan",
    "Palestinian Territories",
    "Panama",
    "Papua New Guinea",
    "Paraguay",
    "Persian Gulf",
    "Peru",
    "Philippines",
    "Poland",
    "Portugal",
    "Puerto Rico",
    "Qatar",
    "Reunion",
    "Romania",
    "Russian Federation & USSR",
    "Rwanda",
    "Saint Helena",
    "Saint Kitts and Nevis",
    "Saint Lucia",
    "Saint Pierre and Miquelon",
    "Saint Vincent and the Grenadines",
    "Samoa",
    "Sao Tome and Principe",
    "Saudi Arabia",
    "Senegal",
    "Serbia",
    "Seychelles",
    "Sierra Leone",
    "Singapore",
    "Slovakia",
    "Slovenia",
    "Solomon Islands",
    "Somalia",
    "South Africa",
    "South Korea",
    "South Sudan",
    "Spain",
    "Sri Lanka",
    "Sudan",
    "Suriname",
    "Swaziland",
    "Sweden",
    "Switzerland",
    "Syria",
    "Taiwan",
    "Tajikistan",
    "Tanzania",
    "Thailand",
    "Timor-Leste",
    "Togo",
    "Tonga",
    "Trinidad and Tobago",
    "Tunisia",
    "Turkey",
    "Turkmenistan",
    "Turks and Caicos Islands",
    "U.S. Pacific Islands",
    "U.S. Territories",
    "Uganda",
    "Ukraine",
    "United Arab Emirates",
    "United Kingdom",
    "United States Virgin Islands",
    "United States of America",
    "Uruguay",
    "Uzbekistan",
    "Vanuatu",
    "Venezuela",
    "Viet Nam",
    "Wake Island",
    "Western Sahara",
    "World",
    "Yemen",
    "Yugoslavia",
    "Zambia",
    "Zimbabwe",
]


def prepare_query_url(energy_source: str, countries: List[str]) -> str:
    """Prepare a query URL to request data for a specific energy source and a list of countries.

    Parameters
    ----------
    energy_source : str
        Name of energy source (e.g. "coal").
    countries : list
        Countries to include in the query.

    Returns
    -------
    query_url : str
        Query URL to use to request data.

    """
    # Prepare a query url for request.
    query_url = (
        f"{SHIFT_URL}energy/{energy_source}?chart-type=line&chart-types=line&chart-types=ranking&"
        f"disable-en=false&energy-unit={ENERGY_UNIT}"
    )
    # Add each country to the url.
    for country in countries:
        query_url += f"&group-names={country.replace(' ', '%20').replace('&', '%26')}"
    # Add some conditions to the query (not all of them may be necessary).
    query_url += (
        f"&is-range=true&dimension=total&end={END_YEAR}&start={START_YEAR}&multi=true&type=Production&"
        f"import-types=Imports&import-types=Exports&import-types=Net%20Imports"
    )

    return query_url


def fetch_data_for_energy_source_and_a_list_of_countries(energy_source: str, countries: List[str]) -> pd.DataFrame:
    """Fetch data from Shift for a specific energy source and a list of countries.

    Parameters
    ----------
    energy_source : str
        Name of energy source (e.g. "coal").
    countries : list
        Countries to include in the query.

    Returns
    -------
    df : pd.DataFrame
        Shift data.

    """
    query_url = prepare_query_url(energy_source=energy_source, countries=countries)
    soup = BeautifulSoup(requests.get(query_url).content, "html.parser")
    data = json.loads(
        soup.find(
            "script",
            {"type": "application/json", "id": re.compile(r"^((?!tb-djs).)*$")},
        ).string  # type: ignore
    )

    fields = data["props"]["apolloState"]
    elements = {}  # type: ignore
    years = []
    for key in list(fields):
        if (ENERGY_UNIT in key) and ("name" in fields[key]) and ("data" in fields[key]):
            if fields[key]["name"] in countries:
                elements[fields[key]["name"]] = fields[key]["data"]["json"]
        if (ENERGY_UNIT in key) and ("categories" in fields[key]):
            years = fields[key]["categories"]["json"]

    assert all([len(elements[country]) == len(years) for country in elements])
    # Use years as index and elements (data for each country) as columns.
    df = pd.DataFrame(elements, index=years)

    # Rearrange dataframe for convenience.
    df = df.reset_index().rename(columns={"index": "year"}).astype({"year": int})

    return df


def fetch_all_data_for_energy_source(energy_source: str) -> pd.DataFrame:
    """Fetch all data for a specific energy source and all countries.

    The list of countries is defined above, in SHIFT_COUNTRIES.

    Parameters
    ----------
    energy_source : str
        Name of energy source (e.g. "coal").

    Returns
    -------
    combined : pd.DataFrame
        Data for a specific energy source and all countries.

    """
    # Split list of countries in smaller chunks to avoid errors when requesting data.
    n_chunks = int(len(SHIFT_COUNTRIES) / MAX_NUM_COUNTRIES_PER_QUERY) + 1
    # Create chunks of country names.
    countries_chunks = np.array_split(SHIFT_COUNTRIES, n_chunks)
    dfs = []
    for countries_chunk in tqdm(countries_chunks, desc="Subset of countries", file=sys.stdout):
        # Fetch data for current chunk of countries and specified energy source.
        df = fetch_data_for_energy_source_and_a_list_of_countries(
            energy_source=energy_source, countries=countries_chunk  # type: ignore
        )
        # Wait between consecutive requests.
        sleep(TIME_BETWEEN_QUERIES)
        # Collect data for current chunk of countries.
        dfs.append(df)

    # Combine dataframes of all chunks of countries into one dataframe.
    combined = dataframes.multi_merge(dfs=dfs, on="year", how="outer")
    # Restructure dataframe conveniently.
    combined = combined.melt(id_vars="year", value_name=energy_source, var_name="country")
    combined = combined.sort_values(["country", "year"]).reset_index(drop=True)

    return combined


def fetch_all_data_for_all_energy_sources() -> pd.DataFrame:
    """Fetch all Shift data for all energy sources and all countries.

    The list of energy sources and countries are defined above, in ENERGY_SOURCES and SHIFT_COUNTRIES, respectively.

    Returns
    -------
    energy_data : pd.DataFrame
        Energy data for all energy sources and countries specified above.

    """
    energy_dfs = []
    for energy_source in tqdm(ENERGY_SOURCES, desc="Energy source", file=sys.stdout):
        # Fetch all data for current energy source.
        energy_df = fetch_all_data_for_energy_source(energy_source=energy_source)
        energy_dfs.append(energy_df)

    # Combine data from different energy sources.
    energy_data = dataframes.multi_merge(energy_dfs, on=["country", "year"], how="outer")

    # Create index.
    energy_data = energy_data.set_index(["country", "year"], verify_integrity=True).sort_index()

    return energy_data


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    # Download all data from Shift as a dataframe.
    energy_data = fetch_all_data_for_all_energy_sources()

    # Create a new snapshot.
    add_snapshot(
        uri=f"shift/{SNAPSHOT_VERSION}/energy_production_from_fossil_fuels.csv", dataframe=energy_data, upload=upload
    )


if __name__ == "__main__":
    main()
