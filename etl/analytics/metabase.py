"""Metabase utils"""

import json
import re
import urllib.parse
from io import BytesIO

import pandas as pd
import requests

from etl.config import (
    METABASE_API_KEY,
    METABASE_SEMANTIC_LAYER_DATABASE_ID,
    METABASE_URL,
)


def read_metabase(sql: str) -> pd.DataFrame:
    """Retrieve data from the Metabase API using an arbitrary sql query.

    NOTE: This function has been adapted from this example in the analytics repo:
    https://github.com/owid/analytics/blob/main/tutorials/metabase_data_download.py

    Parameters
    ----------
    sql : str
        SQL query to execute.
    force_datasette : bool, optional
        If True, use Datasette instead of Metabase. This is a fallback if Metabase API credentials are not available.

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
