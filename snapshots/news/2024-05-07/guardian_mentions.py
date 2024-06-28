"""Getting the snapshot data requires interaction with The Guardian's Open Platform API (https://open-platform.theguardian.com/access/).

NOTE: The data has been cosolidated into a Google Spreadsheet here https://docs.google.com/spreadsheets/d/18xjcsEjT7zTGTSNffju_zoD95RFxYl0vYKXc8jErKGk/edit#gid=0

MAIN STRATEGY
=============
Getting the number of articles/entries talking about a certain country has no straightforward answer, since there can be different strategies. Our strategy has revolved around first getting all the tags for a country, and then getting the number of articles that have those tags. We have an alternative strategy, detailed in "ALTERNATIVE STRATEGY" section below.


Our strategy in detail:


1. Get all tags that concern a country. Steps:
    - Obtain all the tag pages that start with the country name: a query like "https://content.guardianapis.com/tags?web-title=spain", for Spain. As a result we obtain a mapping that tells us for each country the list of tags (e.g. "Spain: [world/spain, travel/spain, etc.]") in use.
    - We work with a list of ~240 countries, so this has a cost of 240 calls.

2. For each country, get the number of pages using each set of tags. Steps:
    - For each country and year (currently working for range 2016-2023) we get all content metadata: a query like "https://content.guardianapis.com/search?tags=...&from-date=2020-01-01&to-date=2020-12-31". And then I save the value from response.total.
    - Since the year range is of 8 years, this has a cost of ca. 8 x 240 = 1920 calls
    - Year range is controlled by YEAR_START and YEAR_END variables.

I see two main downsides from my current approach:
    - The number of calls is above your limit
    - Getting the value for web-title for each country needs some manual work, since country names have often different variations. For instance, we use "Saint Helena" and "Czechia but you use "St Helena" and "Czech Republic", respectively.


CODE TO GET THE DATA
--------------------

1) GET SUMMARY DATA: Get tags and number of pages for each country over year period.

# Get summary data, save it
>>> get_summary_data(output_file="news.csv")

# Load tags per country
>>> df = pd.read_csv("news.csv")
>>> df["tags"] = df["tags"].apply(ast.literal_eval)
>>> COUNTRY_TAGS = df.set_index("country")["tags"].to_dict()

NOTE: Need to manually review the country tags and make sure they are correct. For instance, "ukraine"-related tags appear for "UK".

2) GET DATA FOR EACH COUNTRY AND YEAR

# Get values for each country and year
>>> get_data(output_file="news_yearly.csv")


WHAT IF SOME DATA IS MISSING?
-----------------------------

The call to `get_data` might exceed the API rate limits. You will see this bc you will start to get Error messages printed. If you get them, don't stop the script! Otherwise you will loose all the data. Instead, wait until it finishes and then try running it again for the failed countries (or years)

To get the missing data, there are various strategies depending on what is missing.

1) CERTAIN COUNTRY-YEARS ARE MISSING

>>> # Get list of country-year pairs that are missing in the data (API rate limit exceeded most probably)
>>> missing_entries = get_missing_entries("news_yearly.csv")

>>> # Get data for the missing country-year pairs
>>> get_data_by_tags_from_tuples(missing_entries, "news_yearly-1.csv")

2) CERTAIN COUNTRIES ARE MISSING

>>> # Define dictionary mapping country to tags. Only data for countries listed will be retrieved.
>>> country_tags = {"country": ["tag1", "tag2", ...], ...}
>>> get_data(output_file="news_yearly-X.csv", country_tags=country_tags)

OR

>>> # Get current tags for subset of countries.
>>> country_tags = {c: t for c, t in COUNTRY_TAGS.items() if c in {"country1", "country2"}}
>>> get_data(output_file="news_yearly-Y.csv", country_tags=country_tags)

3) CERTAIN YEAR IS MISSING

>>> # Use 'year_range' to get data for a specific year(s)
>>> get_data(output_file="news-yearly-Z.csv", year_range=[2023])

LAST) COMBINE MULTIPLE FILES

>>> combine_files(["news_yearly-123.csv", "news_yearly-124.csv"], "news_yearly_combined.csv")

COMMENTS ON THE TITLES USED TO GET THE TAGS
-------------------------------------------

We have used OWID's standard names with minor changes:

East Timor -> Timor-Leste
Cote d'Ivoire -> Ivory Coast
Czechia -> Czech Republic
Gaza strip -> Gaza
Cocos Islands -> Cocos Island
Macao -> Macau
Micronesia (country) -> Micronesia
Northern Mariana Islands -> Northern Marianas Islands
Saint Helena -> St Helena
Saint Lucia -> St Lucia
Saint Martin (French part) -> Saint Martin
Sint Maarten (Dutch part) -> Sint Maarten
United States Virgin Islands -> Us Virgin Islands
United Kingdom -> UK (watch for 'Ukraine' tags and remove them)
United States -> US

Not using but could:

England
Wales
Scotland
Northern Ireland

OTHER COMMENTS ON TAGS PER COUNTRY AND MODIFICATIONS
----------------------------------------------------

- Congo, DR Congo: Needed manual review to avoid duplicates and ensure tags are correct for both entities.
- Palestine: Integrate tags from "Gaza"

Tags removal (wrongly assigned to the country):
- Benin: "film/annette-bening"
- Chad: "film/chadwick-boseman", "film/gurinder-chadha"
- France: "books/francesca-simon", "business/air-france-klm", "childrens-books-site/frances-hardinge", "film/journal-de-france", "film/frances-ha", "film/frances-mcdormand", "politics/frances-o-grady", "sport/francesco-molinari", "stage/francesca-hayward", "stage/francesca-moody", "world/francesco-schettino"
- Georgia: "culture/georgia-o-keeffe", "film/the-haunting-of-connecticut-2-the-ghosts-of-georgia", "music/georgia", "sport/georgia-hall", "us-news/state-of-georgia"
- Guinea: "football/equitorial-guinea", "football/guinea-bissau-football-team", "sport/1000-guineas", "sport/2000-guineas", "sport/papua-new-guinea-rugby-league-team", "travel/guinea-bissau", "travel/papuanewguinea", "weather/guineabissau", "weather/papuanewguinea", "world/equatorial-guinea", "world/guinea-bissau", "world/papua-new-guinea"
- Haiti: "music/bernard-haitink",
- India: Remove: "culture/indiana-jones", "film/india-s-daughter", "film/indiana-jones-and-the-kingdom-of-the-crystal-skull", "sport/force-india", "weather/indianapolis", "us-news/indiana","us-news/indianapolis", "sport/indiana-pacers", "sport/indianapolis-colts", "campaign/callout/callout-indiana-teachers"
- Ireland: "campaign/callout/callout-northern-ireland-trade-deal", "extra/scotland-northern-ireland-extra", "football/northern-ireland-womens-football-team", "football/northernireland", "uk/northernireland", "travel/northern-ireland", "society/series/abortion-in-northern-ireland", "healthcare-network/northern-ireland", "social-care-network/northern-ireland", "travel/series/visit-britain-and-northern-ireland"
- Jersey: "film/jersey-boys", "film/jersey-girl", "sport/new-jersey-devils", "uk-news/new-jersey", "us-news/new-york-and-new-jersey-bombings"
- Jordan: Remove: "books/jordan-peterson", "film/jordan-peele", "film/michael-b-jordan", "film/neil-jordan", "football/jordan-pickford", "sport/jordan-spieth", "sport/michael-jordan", "stage/jordan-brookes", "us-news/jim-jordan", "us-news/jordan-neely", "film/macaulay-culkin"
- Mali: "music/zayn-malik", "australia-news/peter-malinauskas', "politics/shahid-malik", "stage/russellmaliphant", "world/nouri-al-maliki", "film/terrence-malick"
- Niger: "football/nigeria-football-team", "football/nigeria-womens-football-team", "global-development/series/crisis-nigeria", "travel/nigeria", "weather/nigeria", "world/nigeria"
- Samoa: "travel/americansamoa"
- Sudan: "world/south-sudan", "global-development/series/south-sudan-one-year-on",

OTHER COMMENTS:
---------------

- We combine Palestine = Palestine + Gaza

ALTERNATIVE STRATEGY
====================

We get all pages that mention a country. That is, we use '?q=' parameter. We exclude certain words sometimes to avoid false positives (i.e. exclude 'guinea-bissau' when searching for 'guinea').

>>> data = get_data_by_raw_mention(year_range=[2016, 2017])

WHAT IF THERE IS MISSING DATA?
------------------------------

1) CERTAIN COUNTRY-YEARS ARE MISSING

>>> # Get list of country-year pairs that are missing in the data (API rate limit exceeded most probably)
>>> missing_entries = get_missing_entries("news_yearly.csv")

>>> # Get data for the missing country-year pairs
>>> get_data_by_raw_mention_from_tuples(missing_entries, "news_yearly-W.csv")


2) CERTAIN COUNTRIES ARE MISSING

>>> # Define dictionary mapping country to tags. Only data for countries listed will be retrieved.
>>> country_names = {"country": ["name_variation_1", "name_variation_2", ...], ...}
>>> get_data_by_raw_mention(output_file="news_yearly-X.csv", country_names=country_names)

OR

>>> # Get current tags for subset of countries.
>>> country_names = get_country_name_variations(country_names={"country1", "country2"})
>>> get_data_by_raw_mention(output_file="news_yearly-Y.csv", country_names=country_names)


OR

>>> # Get current tags for subset of countries.
>>> COUNTRY_NAMES = get_country_name_variations()
>>> country_names = {c: t for c, t in COUNTRY_NAMES.items() if c in {"country1", "country2"}}
>>> get_data_by_raw_mention(output_file="news_yearly-Y.csv", country_names=country_names)

3) CERTAIN YEAR IS MISSING

>>> # Use 'year_range' to get data for a specific year(s)
>>> get_data_by_raw_mention(output_file="news-yearly-Z.csv", year_range=[2023])


RUN SNAPSHOT STEP
=================

python snapshots/news/2024-05-07/guardian_mentions.py --tags snapshots/news/2024-05-07/news-yearly-combined.csv --mentions snapshots/news/2024-05-07/news-b-yearly-combined.csv
"""

import ast
import os
import pathlib
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import click
import numpy as np
import pandas as pd
import requests
import yaml
from owid.catalog import Dataset
from structlog import get_logger

from etl.paths import DATA_DIR
from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# FILE TO COUNTRY FILES
COUNTRY_TAGS_FILE = pathlib.Path(__file__).parent / "country_tags.yaml"

# Year range
YEAR_START = 2016
YEAR_END = 2024

# Guardian API
API_KEY = os.environ.get("GUARDIAN_API_KEY")
API_KEY = ""
API_CONTENT_URL = "https://content.guardianapis.com/search"
API_TAGS_URL = "https://content.guardianapis.com/tags"


# Load YAML file with country tags
with open(COUNTRY_TAGS_FILE, "r") as file:
    COUNTRY_TAGS = yaml.safe_load(file)
COUNTRIES = list(COUNTRY_TAGS.keys())

# Logger
log = get_logger()


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--tags", type=str, help="Path to local data file.")
@click.option("--mentions", type=str, help="Path to local data file.")
def main(tags: Optional[str], mentions: Optional[str], upload: bool) -> None:
    params = [
        (tags, f"news/{SNAPSHOT_VERSION}/guardian_mentions.csv"),
        (mentions, f"news/{SNAPSHOT_VERSION}/guardian_mentions_raw.csv"),
    ]
    for path_to_file, uri in params:
        if path_to_file is None:
            log.warning(f"Skipping {path_to_file}")
            continue
        log.info(f"{path_to_file}")

        # Create a new snapshot.
        snap = Snapshot(uri)

        # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
        snap.create_snapshot(filename=path_to_file, upload=upload)


def get_data(output_file, country_tags=COUNTRY_TAGS, year_range=None):
    """Get number of pages for each country and year.

    country_tags: tags used for each country. Only these countries will be downloaded!
        key: country name
        value: list of tags

    For a given year and country, we get the count of pages with any of the tags associated to the given country (by country_tags).

    We use time.sleep for caution and to avoid hitting the API rate limit.
    """
    data = []

    if year_range is None:
        year_range = range(YEAR_START, YEAR_END)
    # Iterate over years
    data = []
    for year in year_range:
        print(f"-- {year}")
        # Iterate over countries
        for i, (country, tags) in enumerate(country_tags.items()):
            # display info
            if i % 10 == 0:
                print(country)

            # init
            data_ = {
                "country": country,
                "year": year,
            }

            try:
                num_pages, pages_ids = get_pages_from_tags(tags, year=year)
            except KeyError:
                print(f"> Error for {country} (getting num_pages)")
            else:
                data_["num_pages"] = num_pages
                if pages_ids:
                    data_["page_ids"] = pages_ids

            data.append(data_)

            time.sleep(1)

        time.sleep(5)

    # Export data
    df_data = pd.DataFrame(data)
    df_data.to_csv(output_file, index=False)


def get_pages_from_tags(tags: List[str], lazy: bool = True, year: Optional[int] = None):
    """Get the pages categorised under certain tag(s) for a given year.

    Use lazy=False to get the actual list of page ids.

    May require several calls if the list of tags exceed LIM_TAGS.
    """
    LIM_TAGS = 50
    num_pages = 0
    page_ids = {}
    for i in range(0, len(tags), LIM_TAGS):
        tags_slice = tags[i : i + LIM_TAGS]
        num_pages_, page_ids_ = get_pages_from_tags_slice(tags_slice, lazy=lazy, year=year)
        num_pages += num_pages_
        if not lazy:
            page_ids |= page_ids_
    return num_pages, page_ids


def get_pages_from_tags_slice(tags: List[str], lazy: bool = True, year: Optional[int] = None) -> Tuple[int, Set[Any]]:
    """Get the pages categorised under certain tags. Use lazy=False to get the actual list of page ids."""
    # Get list of articles for each tag
    tag_or = "|".join(tags)
    if year:
        params = {
            "api-key": API_KEY,
            "tag": tag_or,
            "page-size": 200,
            "from-date": f"{year}-01-01",
            "to-date": f"{year}-12-31",
        }
    else:
        params = {"api-key": API_KEY, "tag": tag_or, "page-size": 200}
    data = requests.get(API_CONTENT_URL, params=params).json()

    # Sanity check
    if "response" not in data:
        raise KeyError("No response!")

    response = data["response"]

    # Number of pages with given tags
    num_pages = response["total"]

    # Get page IDs
    if not lazy:
        page_ids = _get_page_ids(API_CONTENT_URL, params, response)
    else:
        page_ids = set()
    return num_pages, page_ids


def _get_page_ids(api_url: str, params: Dict[str, Any], response: Dict["str", Any]) -> set[Any]:
    """Get the page ids with a certain tag."""
    # Sanity check
    assert "results" in response, "'results' not found in response"
    results = response["results"]

    # Initialise set of IDs
    page_ids = {r["id"] for r in results}

    # Get IDs for remaining pages
    if response["pages"] > 1:
        for page in range(2, response["pages"] + 1):
            data = requests.get(api_url, params=params | {"page": page}).json()
            assert "response" in data, "'response' missing"
            page_ids |= {r["id"] for r in data["response"]["results"]}
    return page_ids


###########################################################################
# Get general details: tags + number of pages over the whole year period. #
###########################################################################


def get_country_tags(output_file: str | Path = COUNTRY_TAGS_FILE):
    """Get and save the list of tags by country."""
    # Get summary data (tags and number of pages for each country over year period)
    get_summary_data(output_file="news.csv")

    # Load tags per country
    df = pd.read_csv("news.csv")
    df["tags"] = df["tags"].apply(ast.literal_eval)
    country_tags = df.set_index("country")["tags"].to_dict()

    # Save mapping as a YAML
    with open(output_file, "w") as file:
        yaml.dump(country_tags, file, default_flow_style=False, sort_keys=False)


def get_summary_data(countries: List[str] = COUNTRIES, output_file: str | None = None) -> pd.DataFrame:
    """Get tags and number of pages for each country over year period."""
    data_all = []
    # For each country, get tags and number of pages
    for i, country in enumerate(countries):
        data_: Dict[str, Any] = {
            "country": country,
        }
        if i % 10 == 0:
            print(country)

        # Get tags
        try:
            tags = get_tags_for_title(country)
        except KeyError:
            print(f"> Error for {country} (getting tags)")
        else:
            data_["tags"] = tags

            # Get number of pages
            try:
                num_pages, pages_ids = get_pages_from_tags(tags)
            except KeyError:
                print(f"> Error for {country} (getting num_pages)")
            else:
                data_["num_pages"] = num_pages

                # Optionally: Get list of page ids with given tag
                if pages_ids:
                    data_["page_ids"] = pages_ids

        data_all.append(data_)

    # Build dataframe
    df = pd.DataFrame(data_all)
    df = df.drop_duplicates(subset="country", keep="last")
    df = df.sort_values("country")

    # Export
    if output_file:
        df.to_csv(output_file, index=False)
    return df


def get_tags_for_title(title: str, verbose: bool = False, year: Optional[int] = None) -> List[str]:
    """Get the list of tags for a given title.

    For our purposes, "title" is the name of a country. For instance, for "Spain" there are various tags like "world/spain", "travel/spain", etc.

    Part of our efforts are in getting the right list of names for countries (e.g. "Czech Republic" instead of "Czechia"). The way we have done this is by using our standard country names and then checking if we get any tags for them.
    """
    LIM_PAGE_SIZE = 1000

    if year:
        params = {
            "api-key": API_KEY,
            "web-title": title,
            "page-size": LIM_PAGE_SIZE,
            "from-date": f"{year}-01-01",
            "to-date": f"{year}-12-31",
        }
    else:
        params = {"api-key": API_KEY, "web-title": title, "page-size": LIM_PAGE_SIZE}
    data = requests.get(API_TAGS_URL, params=params).json()

    if "response" not in data:
        raise KeyError("No response!")
    if "results" not in data["response"]:
        raise KeyError("No results!")

    response = data["response"]
    results = response["results"]

    # Get num tags and tags (no contributor tags)
    num_tags = response["total"]
    tags = [t["id"] for t in results if t["type"] not in {"contributor"}]

    assert num_tags < LIM_PAGE_SIZE, "Too many tags!"

    ## Display
    tags_blist = "\n- ".join(tags)

    if verbose:
        print(f"There are {len(tags)} for country {title}:\n- {tags_blist}")

    return tags


def get_guardian_country_names():
    """Get the list of country names used by OWID.

    NOTE: This is a hardcoded code snippet. Note that REGIONS is not listed (and should not be) as a dependency of this snapshot step.

    The items of this list are used to retrieve the list of tags associated to each country.
    """
    # Load regions table from disk
    tb_regions = Dataset(DATA_DIR / "garden/regions/2023-01-01/regions")["regions"]
    # Extract list with country names
    tb_regions = tb_regions[~tb_regions["is_historical"] & (tb_regions["region_type"] == "country")]
    tb = tb_regions.reset_index()
    countries = sorted(set(tb.name))

    # Country name changes
    country_names_guardian = {
        "East Timor": "Timor-Leste",
        "Cote d'Ivoire": "Ivory Coast",
        "Czechia": "Czech Republic",
        "Cocos Islands": "Cocos Island",
        "Democratic Republic of Congo": "Democratic Republic of the Congo",
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
    countries = [country_names_guardian.get(c, c) for c in countries]

    return countries


##############################################################################
# Get missing data: We might reach rate limits and need to fill in the gaps. #
##############################################################################


def get_missing_entries(input_file):
    """Get missing entries in input_file.

    That is, obtain the country-year pairs that have NaN values in the number of pages.
    """
    # Read collected data
    df = pd.read_csv(input_file)

    # Make sure we have an entry for each country-year
    regions = set(df["country"])
    years = np.arange(df["year"].min(), df["year"].max() + 1)
    new_idx = pd.MultiIndex.from_product([years, regions], names=["year", "country"])
    df = df.set_index(["year", "country"]).reindex(new_idx).reset_index()
    df["year"] = df["year"].astype("int")

    # Get missing country-year pairs
    missing_entries = df.loc[df.num_pages.isna(), ["country", "year"]].values

    return missing_entries


def get_data_by_tags_from_tuples(country_year_pairs, output_file) -> None:
    """Get data for the given country-year pairs.

    Country-year pairs are given as a list of (country, year) tuples.

    Get data for a country based on tags associated to it.
    """
    # Get country -> tags mapping
    with open(COUNTRY_TAGS_FILE, "r") as file:
        COUNTRY_TAGS = yaml.safe_load(file)

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

        tags = COUNTRY_TAGS.get(country, [])

        if tags != []:
            try:
                num_pages, pages_ids = get_pages_from_tags(tags, year=year)
            except KeyError:
                print(f"> Error for {country} (getting num_pages)")
            else:
                data_["num_pages"] = num_pages
                if pages_ids:
                    data_["page_ids"] = pages_ids

            data.append(data_)

        time.sleep(1)

    # Export data
    df_data = pd.DataFrame(data)
    df_data.to_csv(output_file, index=False)


def combine_files(input_files, output_file):
    """Combine multiple files into one.

    Drop NaN and keep the latest entry if there are duplicates for any country-year pair.
    """
    df = pd.concat([pd.read_csv(f) for f in input_files], ignore_index=True)
    df = df.dropna(subset="num_pages").drop_duplicates(subset=["country", "year"], keep="last")

    df.to_csv(output_file, index=False)


########################################
# GET DATA FROM RAW MENTIONS           #
########################################

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


def get_country_name_variations(country_names: Optional[Set[str]] = None):
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
    name_variations = {country_names_guardian.get(c, c): names for c, names in name_variations.items()}

    # Sort
    names_sorted = sorted(name_variations)  # type: ignore
    name_variations = {k: name_variations[k] for k in names_sorted}

    if country_names is not None:
        name_variations = {c: t for c, t in name_variations.items() if c in country_names}
    return name_variations


def _list_of_items_to_or_strict(values) -> str:
    # Remove those with parents
    values = [c for c in values if not (("(" in c) | (")" in c))]
    values = [f'"{v}"' for v in values]
    return f"({' '.join(values)})"


def get_country_queries(country_names=None):
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
        for i, country in enumerate(queries):
            if i % 10 == 0:
                print(country["country"])
            data_ = {
                "country": country["country"],
                "year": year,
            }
            num_pages = get_pages_from_mentions(country["query"], year)
            if num_pages:
                data_["num_pages"] = num_pages
            DATA.append(data_)

        if output_file_base_year:
            pd.DataFrame(DATA).to_csv(f"{output_file_base_year}-{year}.csv", index=False)

    if output_file:
        pd.DataFrame(DATA).to_csv(output_file, index=False)

    return DATA


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
        data.append(data_)

    # Export data
    df_data = pd.DataFrame(data)
    df_data.to_csv(output_file, index=False)


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


########################################
# MAIN                                 #
########################################
if __name__ == "__main__":
    main()
