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
    METABASE_API_KEY_ADMIN,
    METABASE_SEMANTIC_LAYER_DATABASE_ID,
    METABASE_URL,
    METABASE_URL_LOCAL,
    OWID_ENV,
)

# Config
COLLECTION_EXPERT_ID = 61  # Expert collection
DATABASE_ID = 2  # Semantic Layer database


def mb_cli(domain: str | None = None, key: str | None = None):
    if domain is None:
        domain = METABASE_URL_LOCAL
    if key is None:
        key = METABASE_API_KEY
    return Metabase_API(domain, api_key=key)


def read_semantic_layer(sql: str) -> pd.DataFrame:
    """Retrieve data from the Semantic Layer via Metabase.


    To query another database via Metabase, use `read_metabase` instead.

    Parameters
    ----------
    sql : str
        SQL query to execute.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the results of the query.

    """
    return read_metabase(
        sql,
        METABASE_SEMANTIC_LAYER_DATABASE_ID,
    )


def read_metabase(
    sql: str,
    database_id: int,
    api_key: str | None = None,
    mb_url: str | None = None,
) -> pd.DataFrame:
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
    if api_key is None:
        api_key = METABASE_API_KEY
    headers = {
        "x-api-key": api_key,
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
        "Accept": "application/json",
    }
    body = {
        "query": {
            # Database corresponding to the Semantic Layer (DuckDB).
            "database": database_id,
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
    if mb_url is None:
        mb_url = METABASE_URL
    response = requests.post(
        f"{mb_url}/api/dataset/csv",
        headers=headers,
        data=urlencoded,
        timeout=30,
    )
    if not response.ok:
        raise RuntimeError(f"Metabase API request failed with status code {response.status_code}: {response.text}")

    # Create a dataframe with the returned data.
    df = pd.read_csv(BytesIO(response.content))

    return df


def _generate_question_url(card: dict) -> str:
    """
    Generate URL to access a Metabase question.

    Args:
        card (dict): Metabase card object as returned by the Metabase API.
    Returns:
        str: Link to the created question in Metabase. This link can be shared with others to access the question directly.

    """
    assert "id" in card, "Card must have an 'id' field"
    card_id = card["id"]
    assert "name" in card, "Card must have an 'name' field"
    card_name = card["name"]

    # Preliminary cleaning
    slug = card_name.lower().replace(" ", "-").replace("/", "-")
    # Use urllib.parse.quote to handle special characters properly
    slug = urllib.parse.quote(slug, safe="")

    if OWID_ENV.env_local == "production":
        url = f"{METABASE_URL}/question/{card_id}-{slug}"
    else:
        url = f"{METABASE_URL_LOCAL}/question/{card_id}-{slug}"

    return url


def create_question(
    title: str,
    query: str,
    description: str | None = None,
    database_id: int = DATABASE_ID,
    collection_id: int = COLLECTION_EXPERT_ID,
    **kwargs,
):
    """Create a question in Metabase with the given SQL query and title.

    This tool should be used once we are sure that the query is valid in Datasette.

    Args:
        query: Query user for Datasette/Metabase.
        title: Title that describes what the query does. Should be short, but concise.
    Returns:
        Question object from Metabase API.
    """
    # Define title
    QUESTION_TIMESTAMP = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    q_title = f"🤖 {title} ({QUESTION_TIMESTAMP})"

    # Init API client
    mb = mb_cli()

    # Create question
    question = mb.create_card(
        # card_name=f"{QUESTION_TITLE} (1)",
        collection_id=collection_id,
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


def _get_domain(prod: bool = False) -> str:
    if prod:
        domain = METABASE_URL
    else:
        domain = METABASE_URL_LOCAL
    return domain


def list_questions(prod: bool = False) -> list[dict]:
    domain = _get_domain(prod=prod)

    # Init API client
    mb = mb_cli(domain=domain)

    # Get cards
    cards = mb.get("/api/card/")

    # Ensure cards is a list
    if not isinstance(cards, list):
        cards = []

    # Filter from list only those with type="question"
    questions = [card for card in cards if card.get("type") == "question"]

    return questions


def get_question_info(question_id: int, prod: bool = False) -> dict:
    domain = _get_domain(prod=prod)

    # Init API client
    mb = mb_cli(domain=domain)

    # Get question
    question = mb.get_item_info(item_id=question_id, item_type="card")
    assert question is not None, f"No card found with id {question_id}"
    assert question.get("type") == "question", f"Card with id {question_id} is not a question"

    return question


def get_question_data(card_id: int, data_format: str = "csv", prod: bool = False) -> pd.DataFrame:
    domain = _get_domain(prod=prod)

    # Init API client
    mb = mb_cli(domain=domain)

    # Get card data
    data_str = mb.get_card_data(
        card_id=card_id,
        data_format=data_format,
    )
    if data_str is None:
        return pd.DataFrame()  # Return empty DataFrame if no data

    # Fix encoding: the library may return UTF-8 bytes decoded as Latin-1
    try:
        data_str = data_str.encode("latin-1").decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError):
        pass  # Already correct encoding

    # Parse raw data as dataframe
    df = pd.read_csv(BytesIO(initial_bytes=data_str.encode("utf-8")), encoding="utf-8")

    return df


def get_metabase_analytics(prod: bool = False):
    """Get views on Metabase questions."""
    domain = _get_domain(prod=prod)
    mb = mb_cli(key=METABASE_API_KEY_ADMIN, domain=domain)

    #########################
    # View counts
    #########################
    dfs = []
    # Get cards
    cards = mb.get("/api/card/")
    # Ensure cards is a list
    if not isinstance(cards, list):
        cards = []

    # Build cards dataframe
    cards = [{"id": c["id"], "type": c["type"], "name": c["name"], "views": c["view_count"]} for c in cards]
    df = pd.DataFrame(cards)
    dfs.append(df)

    # Get dashboards
    dashboards = mb.get("/api/dashboard/")
    # Ensure dashboards is a list
    if not isinstance(dashboards, list):
        dashboards = []

    # Build cards dataframe
    dashboards = [{"id": c["id"], "type": "dashboard", "name": c["name"], "views": c["view_count"]} for c in dashboards]
    df = pd.DataFrame(dashboards)
    dfs.append(df)

    # Combine dataframes
    df = pd.concat(dfs, ignore_index=True)

    # Sort dataframe
    df = df.sort_values(by="views", ascending=False).reset_index(drop=True)  # type: ignore

    #########################
    # Anonymous stats
    #########################
    # stats = mb.get("/api/analytics/anonymous-stats")
    return df
