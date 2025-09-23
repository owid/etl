"""Metabase utils"""

import datetime
import json
import re
import urllib.parse
from io import BytesIO

import pandas as pd
import requests
from metabase_api import Metabase_API

from etl.config import (
    METABASE_API_KEY,
    METABASE_SEMANTIC_LAYER_DATABASE_ID,
    METABASE_URL,
    METABASE_URL_LOCAL,
)

# Config
COLLECTION_EXPERT_ID = 61  # Expert collection
DATABASE_ID = 2  # Semantic Layer database


def read_metabase(sql: str) -> pd.DataFrame:
    """Retrieve data from the Metabase API using an arbitrary sql query.

    NOTE: This function has been adapted from this example in the analytics repo:
    https://github.com/owid/analytics/blob/main/tutorials/metabase_data_download.py

    Parameters
    ----------
    sql : str
        SQL query to execute.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the results of the query.

    """
    # Prepare the header and body of the request to send to the Metabase API.
    headers = {
        "x-api-key": METABASE_API_KEY,
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
        "Accept": "application/json",
    }
    body = {
        "query": {
            # Database corresponding to the Semantic Layer (DuckDB).
            "database": METABASE_SEMANTIC_LAYER_DATABASE_ID,
            "type": "native",
            "native": {"query": re.sub(r"\s+", " ", sql.strip())},
        }
    }

    # Note (copied from Bobbie in the analytics repo):
    # Despite the documentation (https://www.metabase.com/docs/latest/api#tag/apidataset/POST/api/dataset/{export-format}),
    # I cannot get the /api/dataset/csv endpoint to work when sending a dict (or json.dumps(dict)) to the POST body,
    # so I instead urlencode the body. The url encoding is a little awkward – we cannot simply use urllib.parse.urlencode(body)
    # b/c python dict single quotes need to be changed to double quotes. But we can't naively change all single quotes to
    # double quotes b/c the sql query might include single quotes (and DuckDB doesn't allow double quotes). So the line below
    # executes the url encoding without replacing any quotes within the sql query.
    urlencoded = "&".join([f"{k}={urllib.parse.quote_plus(json.dumps(v))}" for k, v in body.items()])

    # Send request.
    response = requests.post(
        f"{METABASE_URL}/api/dataset/csv",
        headers=headers,
        data=urlencoded,
        timeout=30,
    )
    if not response.ok:
        raise RuntimeError(f"Metabase API request failed with status code {response.status_code}: {response.text}")

    # Create a dataframe with the returned data.
    df = pd.read_csv(BytesIO(response.content))

    return df


def bake_question_url(card: dict) -> str:
    assert "id" in card, "Card must have an 'id' field"
    card_id = card["id"]
    assert "name" in card, "Card must have an 'name' field"
    card_name = card["name"]

    url = f"{METABASE_URL_LOCAL}/question/{card_id}-{card_name.replace(' ', '-').lower()}"
    return url


def create_question(
    title: str,
    query: str,
    description: str | None = None,
    database_id: int = DATABASE_ID,
    **kwargs,
):
    # Define title
    QUESTION_TIMESTAMP = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    q_title = f"🤖 {title} ({QUESTION_TIMESTAMP})"

    # Init API client
    mb = Metabase_API(METABASE_URL_LOCAL, api_key=METABASE_API_KEY)

    # Create question
    question = mb.create_card(
        # card_name=f"{QUESTION_TITLE} (1)",
        collection_id=COLLECTION_EXPERT_ID,
        # If you are providing only this argument, the keys 'name', 'dataset_query' and 'display' are required (https://github.com/metabase/metabase/blob/master/docs/api-documentation.md#post-apicard).
        custom_json={
            "name": q_title,
            "description": description,
            "type": "question",
            "dataset_query": {
                "type": "native",
                "database": database_id,
                "native": {"query": query},
            },
            "display": "table",
        },
        return_card=True,
        **kwargs,
    )

    return question
