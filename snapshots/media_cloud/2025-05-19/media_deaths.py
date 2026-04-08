"""Script to create a snapshot of dataset."""

import datetime as dt
import os
from pathlib import Path

import click
import mediacloud.api
import pandas as pd
import structlog
from dotenv import load_dotenv
from media_deaths_queries import create_full_queries, create_queries
from owid.datautils.io import df_to_file

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

load_dotenv()

MC_API_TOKEN = os.getenv("MC_API_TOKEN")

NYT_ID = 1
WAPO_ID = 2
FOX_ID = 1092

US_COLLECTION_ID = 34412234

QUERIES = create_queries()
STR_QUERIES = create_full_queries()

# These are the causes of death we are using for the 2023 version.
# They are based on the 12 leading causes of death in the US for 2023, plus drug overdoses, homicides, and terrorism
CAUSES_OF_DEATH = [
    "heart disease",
    "cancer",
    "accidents",
    "stroke",
    "respiratory",
    "alzheimers",
    "diabetes",
    "kidney",
    "liver",
    "covid",
    "suicide",
    "influenza",
    "drug overdose",
    "homicide",
    "terrorism",
]

YEAR = 2023  # set to year you want to query

search_api = mediacloud.api.SearchApi(MC_API_TOKEN)
log = structlog.get_logger()


def get_start_end(year):
    return (dt.date(year, 1, 1), dt.date(year, 12, 31))


def query_results(query, source_ids, year, collection_ids=None):
    start_date, end_date = get_start_end(year)
    if collection_ids:
        results = search_api.story_count(
            query=query, start_date=start_date, end_date=end_date, collection_ids=collection_ids
        )
    else:
        results = search_api.story_count(query=query, start_date=start_date, end_date=end_date, source_ids=source_ids)
    return results["relevant"]


def get_mentions_from_source(
    source_ids: list,
    source_name: str,
    queries: dict,
    year=YEAR,
    collection_ids=None,
):
    """
    Get mentions of causes of death from a specific source.
    Args:
        source_ids (list): List of source IDs to query.
        source_name (str): Name of the source.
        queries (dict): Dictionary of queries to run.
        year (int): Year to query for.
        collection_ids (list): List of collection IDs to query.
    Returns:
        pd.DataFrame: DataFrame containing the results of the queries."""
    query_count = []
    for name, query in queries.items():
        cnt = query_results(query, source_ids, collection_ids=collection_ids, year=year)
        log.info("Querying:", keyword=name, query=query)
        log.info("Count:", count=cnt, source=source_name, year=year)
        query_count.append(
            {
                "cause": name,
                "mentions": cnt,
                "source": source_name,
                "year": year,
            }
        )
    return pd.DataFrame(query_count)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(upload: bool) -> None:
    # Query Media Cloud API for mentions of causes of death in media sources.
    assert MC_API_TOKEN is not None, "Get API key from https://www.mediacloud.org/ in order to access this data"
    source_ids = [NYT_ID, WAPO_ID, FOX_ID]
    sources = ["The New York Times", "The Washington Post", "Fox News"]

    mentions_ls = []

    queries_in_use = {q: q_str for q, q_str in STR_QUERIES.items() if q in CAUSES_OF_DEATH}

    for s_id, s_name in zip(source_ids, sources):
        mentions = get_mentions_from_source([s_id], s_name, queries_in_use, year=YEAR)
        mentions_ls.append(mentions.copy(deep=True))

    # add mentions for US collection
    collection_mentions = get_mentions_from_source(
        source_ids=[],
        source_name="US Collection",
        queries=queries_in_use,
        year=YEAR,
        collection_ids=[US_COLLECTION_ID],
    )
    mentions_ls.append(collection_mentions.copy(deep=True))

    # concatenate all mentions into a single DataFrame
    mentions_df = pd.concat(mentions_ls, ignore_index=True)

    # Initialize a new snapshot.
    snap = Snapshot(f"media_cloud/{SNAPSHOT_VERSION}/media_deaths.csv")

    df_to_file(df=mentions_df, file_path=snap.path)  # type: ignore

    # Save snapshot.
    snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    run()
