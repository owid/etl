"""Getting the snapshot data requires interaction with The Guardian's Open Platform API (https://open-platform.theguardian.com/access/).

Getting the number of articles/entries talking about a certain country has no straightforward answer, since there can be different strategies. Our strategy has revolved around first getting all the tags for a country, and then getting the number of articles that have those tags. In detail:


1. Get all tags that concert a country.
    - For this, I get all the tag pages that start with the country name: something like "https://content.guardianapis.com/tags?web-title=spain", for Spain. As a result I obtain a mapping that tells me for each country the list of tags (e.g. "Spain: [world/spain, travel/spain, etc.]").
    - We work with a list of ~240 countries, so this has a cost of 240 calls.

2. For each country and year (currently working for range 2016-2023) I get all content: something like "https://content.guardianapis.com/search?tags=...&from-date=2020-01-01&to-date=2020-12-31". And then I save the value from response.total.
    - Since the year range is of 8 years, this has a cost of ca. 8 x 240 = 1920 calls
    - Year range is controlled by YEAR_START and YEAR_END variables.

I see two main downsides from my current approach:
    - The number of calls is above your limit
    - Getting the value for web-title for each country needs some manual work, since country names have often different variations. For instance, we use "Saint Helena" and "Czechia but you use "St Helena" and "Czech Republic", respectively.


CODE TO GET THE DATA
--------------------

# Get summary data, save it
>>> get_summary_data(output_file="news.csv")

# Load tags per country
>>> df = pd.read_csv("news.csv")
>>> df["tags"] = df["tags"].apply(ast.literal_eval)
>>> COUNTRY_TAGS = df.set_index("country")["tags"].to_dict()

# Get values for each country and year
>>> get_data(output_file="news_yearly.csv")


COMMENTS ON THE CODE
--------------------

The call to `get_data` might exceed the API rate limits. You will see this bc you will start to get Error messages printed. If you get them, don't stop the script! Otherwise you will loose all the data. Instead, wait until it finishes and then try running it again for the failed countries (or years)


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

?
United Kingdom -> UK
United States -> US

"""

import ast
import os
import pathlib
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import click
import pandas as pd
import requests
import yaml
from owid.catalog import Dataset

from etl.paths import DATA_DIR
from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# FILE TO COUNTRY FILES
COUNTRY_TAGS_FILE = pathlib.Path(__file__).parent / "countries_tags.yaml"

# Year range
YEAR_START = 2016
YEAR_END = 2023

# Guardian API
API_KEY = os.environ.get("GUARDIAN_API_KEY")
API_CONTENT_URL = "https://content.guardianapis.com/search"
API_TAGS_URL = "https://content.guardianapis.com/tags"


# Load YAML file with country tags
with open("data.yaml", "r") as file:
    COUNTRY_TAGS = yaml.safe_load(file)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def main(path_to_file: str, upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"news/{SNAPSHOT_VERSION}/guardian_mentions.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


def get_data(output_file, country_tags=COUNTRY_TAGS):
    """Get number of pages for each country and year.

    country_tags: tags used for each country
        key: country name
        value: list of tags

    For a given year and country, we get the count of pages with any of the tags associated to the given country (by country_tags).

    We use time.sleep for caution and to avoid hitting the API rate limit.
    """
    data = []

    # Iterate over years
    data = []
    for year in range(YEAR_START, YEAR_END):
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


def get_country_tags(output_file: str = COUNTRY_TAGS_FILE):
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
        data_ = {
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


def get_tags_for_title(title: str, verbose: bool = False, year: Optional[int] = None) -> List[Any]:
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
        "Gaza strip": "Gaza",
        "Cocos Islands": "Cocos Island",
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


if __name__ == "__main__":
    main()
