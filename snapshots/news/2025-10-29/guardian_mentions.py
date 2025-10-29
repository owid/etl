"""OBtain country mentions from The Guardian.


PRELIMINARY NOTES:
==================
- Getting the snapshot data requires interaction with The Guardian's Open Platform API (https://open-platform.theguardian.com/access/).
- This script contains all the necessary functions to get the data, which is a manual process (continue reading to understand how to use it).
- 29th October 2025: I manually obtained data for all countries 2013-2024, and consolidated into a Google Spreadsheet here https://docs.google.com/spreadsheets/d/1MPWQeKy7w_BZy3rXEuup9lKWUcjHAQPbGuOnrx75G7k/edit?usp=sharing
- After consolidation, you need to download as CSV file, and this snapshot step as: `etls news/2025-10-29/guardian_mentions -f path/to/file`

INTRODUCTION
============
Getting the number of articles/entries talking about a certain country has no straightforward answer, since there can be different strategies. Our strategy is based on "raw mentions" This means getting the number of articles that mention a particular country.

This strategy is not perfect, and has some limitations:
- Ambiguity: Names like Turkey, Georgia, Jordan, Chad (state/person/common noun vs country), and “Congo” (two countries) need disambiguation; demonyms (French, American, etc.) can refer to people/products rather than country-focused coverage.
- Prominence: We count presence, not whether the country is the central subject; a fleeting reference and a lead story currently weigh the same.
- Nationality adjectives around individuals (e.g., “Brazilian striker”) can overstate attention to some countries.
- Region terms (“West Africa”, “Balkans”) aren't yet expanded into constituent countries—this leads to undercounts for countries in these regions.
- Historical name changes and synonyms (Burma/Myanmar, Swaziland/Eswatini, Czech Republic/Czechia) require a time-aware mapping.
- Multi-country phrases (“US–China relations”) treat countries equally, even if the focus is asymmetric.

However, all of these can be mitigated with careful query design and post-processing.

In the past, we've used an alternative strategy based on tags associated to each country. That strategy was discarded (https://github.com/owid/owid-issues/issues/1445), but is still available in news/2024-05-07/guardian_mentions* files.


METHODOLOGY
===========

We get all pages that mention a country. That is, we use '?q=' parameter. We exclude certain words sometimes to avoid false positives (i.e. exclude 'guinea-bissau' when searching for 'guinea').

1) COUNTRY QUERIES
------------------

We have drafted queries for each country, based on our regions.yml file. These queries are stored in country_queries.yaml file. We have used get_country_queries_from_scratch() function to generate a first approximation of the queries, and then edited them manually to improve accuracy.

>>> get_country_queries_from_scratch()
>>> # Manual edits


2) GET DATA
------------
To get data for all countries, using the drafted queries, you can refer to the function `demo_get_data_historical`.


3) WHAT IF THERE IS MISSING DATA?
------------------------------

* CERTAIN COUNTRY-YEARS ARE MISSING

>>> # Get list of country-year pairs that are missing in the data (API rate limit exceeded most probably)
>>> missing_entries = get_missing_entries("news_yearly.csv")

>>> # Get data for the missing country-year pairs
>>> get_data_by_raw_mention_from_tuples(missing_entries, "news_yearly-W.csv")


* CERTAIN COUNTRIES ARE MISSING

>>> # Define dictionary mapping country to tags. Only data for countries listed will be retrieved.
>>> get_data_by_raw_mention(output_file="news_yearly-X.csv", country_names=["country_1", "country_2", ...])

OR

>>> # Get current tags for subset of countries.
>>> country_names = get_country_name_variations(country_names={"country1", "country2"})
>>> get_data_by_raw_mention(output_file="news_yearly-Y.csv", country_names=country_names)


* CERTAIN YEAR IS MISSING

>>> # Use 'year_range' to get data for a specific year(s)
>>> get_data_by_raw_mention(output_file="news-yearly-Z.csv", year_range=[2023])

4) COMBINE FILES
-----------------
You can combine various exported files into one using combine_files() function.

>>> combine_files(["news_yearly-123.csv", "news_yearly-124.csv"], "news_yearly_combined.csv")

FINAL NOTES
===========
- We haven't included the following countries/territories in our queries:
    - England
    - Wales
    - Scotland
    - Northern Ireland
- We combine Palestine = Palestine + Gaza
"""

import ast
import os
import pathlib
import time
from pathlib import Path

import click
import numpy as np
import pandas as pd
import requests
import yaml
from owid.catalog import Dataset
from structlog import get_logger

from etl.helpers import PathFinder
from etl.paths import DATA_DIR

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
paths = PathFinder(__file__)

# FILE TO COUNTRY QUERIES
COUNTRY_QUERIES_FILE = pathlib.Path(__file__).parent / "country_queries.yaml"

# Year range
YEAR_START = 2013
YEAR_END = 2025

# Guardian API
API_KEY = os.environ.get("GUARDIAN_API_KEY")
# API_KEY = ""
API_CONTENT_URL = "https://content.guardianapis.com/search"
API_TAGS_URL = "https://content.guardianapis.com/tags"


# Logger
log = get_logger()


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", "-f", prompt=True, type=str, help="Path to local data file.")
def main(upload: bool, path_to_file: str) -> None:
    # Init Snapshot object
    snap = paths.init_snapshot()

    # Save snapshot from local file.
    snap.create_snapshot(filename=path_to_file, upload=upload)


##############################################################################
# PREPARE QUERIES                                                            #
##############################################################################

# Define exceptions (words not include, words to exclude)
# For each country, we use various country name variations, based on our regions.yml file. These variations may not be enough, or might include unwanted keywords. To control this we define:
## add: additional keywords to include in the query (collect also pages with these keywords)
## remove: keywords from the original list that should not be used in the search
# Finally, there are sometimes words that we DO NOT WANT to include in the search, since some countries might have similar names (e.g. 'Guinea' and 'Guinea-Bissau').
## avoid: exclude pages WITH this word
COUNTRY_EXCEPTIONS = {
    "Central African Republic": {
        "remove": {"CAR"},
    },
    "Cocos Islands": {
        "add": {"Cocos Island"},
    },
    "Congo": {
        "avoid": {
            "Kinshasa",
            "Democratic Republic of Congo",
            "Democratic Republic of the Congo",
            "DR Congo",
        }  # No news with Congo & DR Congo
    },
    "Cote d'Ivoire": {"remove": {"CÃ´te D'Ivoire", "Côte d'Ivoire", "Côte d’Ivoire", "Cote d'Ivoire"}},
    "Curacao": {"remove": {"Curaçao", "CuraÃ§ao"}},
    "Falkland Islands": {"add": {"Malvinas"}},
    "Georgia": {
        "avoid": {"United States", "US", "State", "America"},  # No news with Georgia & America
    },
    "Guinea": {
        "avoid": {
            "Papua New Guinea",
            "Papua New Guinea",
            "Guinea-Bissau",
            "Guinea-Bissau",
            "Eq. Guinea",
            "Equatorial Guinea",
        }
    },
    "Ireland": {"remove": {"Northern Ireland"}},
    "Jersey": {
        "avoid": {"United States", "US", "State", "America", "New Jersey"},  # No news with Jersey & America
    },
    "Jordan": {
        "avoid": {"Michael Jordan"},
    },
    "Northern Mariana Islands": {"add": {"Northern Marianas Islands"}},
    "Niger": {"avoid": {"Nigeria"}},
    "Palestine": {"add": ["Gaza"]},
    "Saint Helena": {
        "add": {"St Helena"},
    },
    "Saint Kitts and Nevis": {"add": {"Kitts and Nevis"}},
    "Saint Lucia": {
        "add": {"St Lucia"},
    },
    "Saint Martin (French part)": {
        "add": {"St Martin", "Saint Martin", "St. Martin"},
    },
    "Samoa": {"avoid": {"American Samoa"}},
    "Sint Maarten (Dutch part)": {
        "add": {"Sint Maarten", "St Maarten", "St. Maarten"},
    },
    "Sudan": {"avoid": {"South Sudan"}},
    "Tanzania": {"remove": {"U.R. of Tanzania: Mainland"}},
    "United States Virgin Islands": {"add": {"US Virgin Islands"}},
}


def get_country_queries_from_scratch(country_names=None):
    """Use this method to generate a first approximation of country queries."""
    if country_names is None:
        country_names = get_country_name_variations()

    queries = []
    for country, names_all in country_names.items():
        avoid = set()
        if country in COUNTRY_EXCEPTIONS:
            add = COUNTRY_EXCEPTIONS[country].get("add", set())
            remove = COUNTRY_EXCEPTIONS[country].get("remove", set())
            avoid = COUNTRY_EXCEPTIONS[country].get("avoid", set())

            country_names_ = [c for c in set(set(names_all) | set(add)) if c not in remove]
        else:
            country_names_ = names_all

        query = _list_of_items_to_or_strict(country_names_)

        if avoid != set():
            query += f" AND NOT {_list_of_items_to_or_strict(avoid)}"

        query = {
            "country": country,
            "query": query,
        }
        queries.append(query)

    return queries


def _list_of_items_to_or_strict(values) -> str:
    # Remove those with parents
    values = [c for c in values if not (("(" in c) | (")" in c))]
    values = [f'"{v}"' for v in values]
    return f"({' '.join(values)})"


def get_country_name_variations(country_names: set[str] | None = None):
    # Load regions table from disk
    tb_regions = Dataset(DATA_DIR / "garden/regions/2023-01-01/regions")["regions"]
    # Extract list with country names
    tb_regions = tb_regions[~tb_regions["is_historical"] & (tb_regions["region_type"] == "country")]
    # Prettify
    tb_regions["aliases"] = tb_regions["aliases"].astype(str).apply(lambda x: ast.literal_eval(x) if x != "nan" else [])
    mapping = tb_regions.set_index("name")["aliases"].to_dict()
    name_variations = {k: set(v + [k]) for k, v in mapping.items()}

    # Certain re-organisations
    name_variations["Palestine"] |= name_variations["Gaza Strip"]
    _ = name_variations.pop("Gaza Strip", None)

    # Country name changes
    country_names_guardian = {
        "East Timor": "Timor-Leste",
        "Cote d'Ivoire": "Ivory Coast",
        "Czechia": "Czech Republic",
        "Democratic Republic of Congo": "Democratic Republic of the Congo",
        "Cocos Islands": "Cocos Island",
        "Gaza strip": "Gaza",
        "Macao": "Macau",
        "Micronesia (country)": "Micronesia",
        "Northern Mariana Islands": "Northern Marianas Islands",
        "Saint Helena": "St Helena",
        "Saint Lucia": "St Lucia",
        "Saint Martin (French part)": "Saint Martin",
        "Sint Maarten (Dutch part)": "Sint Maarten",
        "United States Virgin Islands": "Us Virgin Islands",
        "United Kingdom": "UK",
        "United States": "US",
    }
    name_variations = {country_names_guardian.get(c, c): names for c, names in name_variations.items()}  # type: ignore[reportCallIssue]

    # Sort
    names_sorted = sorted(name_variations)  # type: ignore
    name_variations = {k: name_variations[k] for k in names_sorted}

    if country_names is not None:
        name_variations = {c: t for c, t in name_variations.items() if c in country_names}
    return name_variations


##############################################################################
# GET DATA BY RAW MENTIONS                                                   #
##############################################################################


def get_data_by_raw_mention(country_names=None, year_range=None, output_file=None, output_file_base_year=None):
    """Get data for country-year using mentions.

    It loops over countries and years.
    """
    # Get query
    queries = get_country_queries(country_names)
    if year_range is None:
        year_range = range(YEAR_START, YEAR_END)

    DATA = []
    for year in year_range:
        print(f"-- {year}")
        for i, country in enumerate(queries):
            data_ = {
                "country": country["country"],
                "year": year,
            }
            num_pages = get_pages_from_mentions(country["query"], year)
            if num_pages:
                data_["num_pages"] = num_pages
                # if i % 10 == 0:
                print(country["country"], num_pages)
            else:
                print(f"> ERROR. No data for {country['country']}")
            DATA.append(data_)

            time.sleep(1)
        time.sleep(5)

        if output_file_base_year:
            pd.DataFrame(DATA).to_csv(f"{output_file_base_year}-{year}.csv", index=False)

    if output_file:
        pd.DataFrame(DATA).to_csv(output_file, index=False)

    return DATA


def get_country_queries(country_names=None):
    """Get country queries from COUNTRY_QUERIES_FILE."""
    with open(COUNTRY_QUERIES_FILE, "r") as file:
        country_queries = yaml.safe_load(file)
    if "queries" not in country_queries:
        raise KeyError(f"No 'queries' key found in {COUNTRY_QUERIES_FILE}")
    country_queries = country_queries["queries"]
    if country_names is not None:
        return [c for c in country_queries if c["country"] in country_names]
    return country_queries


def get_pages_from_mentions(query, year):
    params = {
        "api-key": API_KEY,
        "q": query,
        "page-size": 200,
        "from-date": f"{year}-01-01",
        "to-date": f"{year}-12-31",
    }
    data = requests.get(API_CONTENT_URL, params=params).json()
    if "response" in data:
        if "total" in data["response"]:
            return data["response"]["total"]
        else:
            print(f"> Error {query}!")


##############################################################################
# GET TOTAL NUMBER PAGES
##############################################################################


def get_number_pages(year_range=None, output_file=None, output_file_base_year=None):
    """Get total number of pages."""
    # Get year range
    if year_range is None:
        year_range = range(YEAR_START, YEAR_END)

    DATA = []
    for year in year_range:
        data_ = {
            "country": "Total",
            "year": year,
        }
        num_pages = get_pages_from_mentions("", year)
        if num_pages:
            data_["num_pages"] = num_pages
        DATA.append(data_)

        if output_file_base_year:
            pd.DataFrame(DATA).to_csv(f"{output_file_base_year}-{year}.csv", index=False)

    if output_file:
        pd.DataFrame(DATA).to_csv(output_file, index=False)

    return DATA


##############################################################################
# GET MISSING DATA
# We might reach rate limits and need to fill in the gaps.
##############################################################################


def get_missing_entries(df=None, input_file=None):
    """Get missing entries in input_file.

    That is, obtain the country-year pairs that have NaN values in the number of pages.
    """
    if df is None:
        if input_file is None:
            raise ValueError("Either df or input_file must be provided.")
        # Read collected data
        df = pd.read_csv(input_file)

    # Get regions
    queries = get_country_queries()
    regions = set(q["country"] for q in queries)

    # Make sure we have an entry for each country-year
    years = np.arange(df["year"].min(), df["year"].max() + 1)
    new_idx = pd.MultiIndex.from_product([years, regions], names=["year", "country"])
    df = df.set_index(["year", "country"]).reindex(new_idx).reset_index()
    df["year"] = df["year"].astype("int")

    # Get missing country-year pairs
    missing_entries = df.loc[df.num_pages.isna(), ["country", "year"]].values

    return missing_entries


def get_data_by_raw_mention_from_tuples(country_year_pairs, output_file) -> None:
    """Get data for the given country-year pairs.

    Country-year pairs are given as a list of (country, year) tuples.

    Get data for a country using 'raw mentions' (i.e. ?q=... parameter in the API call)
    """
    # Get query
    queries_all = get_country_queries()
    queries = {c["country"]: c["query"] for c in queries_all}

    # Get missing data
    data = []
    for missing in country_year_pairs:
        country = missing[0]
        year = missing[1]

        print(f"{country} @ {year}")

        # init
        data_ = {
            "country": country,
            "year": year,
        }

        # get query
        query = queries.get(country)
        if query is not None:
            # get num pages
            num_pages = get_pages_from_mentions(query, year)
            if num_pages:
                data_["num_pages"] = num_pages
            else:
                print(f"> Error: No query found! {country}")
        else:
            print(f"> Error: No query found! {country}")
        data.append(data_)

    # Export data
    df_data = pd.DataFrame(data)
    df_data.to_csv(output_file, index=False)


##########################################
# OTHER UTILS                            #
##########################################


def combine_files(input_files, output_file):
    """Combine multiple files into one.

    Drop NaN and keep the latest entry if there are duplicates for any country-year pair.
    """
    df = pd.concat([pd.read_csv(f) for f in input_files], ignore_index=True)
    df = df.dropna(subset="num_pages").drop_duplicates(subset=["country", "year"], keep="last")

    df = df.sort_values(by=["country", "year"])

    df.to_csv(output_file, index=False)


def demo_get_data_historical():
    """Run year by year, track errors with logging messages."""
    YEAR_1 = 2022
    YEAR_2 = 2023
    OUTPUT_PATH = pathlib.Path(__file__).parent.parent.parent.parent / f"guardian_mentions_raw_{YEAR_1}-{YEAR_2}.csv"
    data = get_data_by_raw_mention(year_range=[YEAR_1, YEAR_2])

    # Export
    df = pd.DataFrame(data)
    df.to_csv(OUTPUT_PATH, index=False)


########################################
# MAIN                                 #
########################################
# if __name__ == "__main__":
#     main()
# df_old = pd.read_csv("/home/lucas/repos/etl/Media Attention Raw Mentions.csv")
# dff = df.merge(df_old, on=["country", "year"], how="outer")
# dff["dif"] = (dff.num_pages_x - dff.num_pages_y).abs()
# dff.sort_values("dif", ascending=False)
