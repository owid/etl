"""Metabase utils"""

import json
import re
import urllib.parse
from io import BytesIO

import pandas as pd
import requests
from metabase_api import Metabase_API
from structlog import get_logger
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from etl.config import (
    METABASE_API_KEY,
    METABASE_API_KEY_ADMIN,
    METABASE_SEMANTIC_LAYER_DATABASE_ID,
    METABASE_URL,
    METABASE_URL_LOCAL,
)

log = get_logger()

# HTTP status codes that indicate Metabase is temporarily unavailable rather than a permanent error.
# Metabase is restarted by the analytics pipeline whenever the DuckDB mirrors are rebuilt (daily, plus
# on every push to the analytics / owid-grapher main branches), so its nginx front-end briefly returns
# 502/503/504 while the JVM reboots. These are worth retrying; a 4xx is not.
RETRYABLE_STATUS_CODES = frozenset({502, 503, 504})


class MetabaseTransientError(RuntimeError):
    """Metabase upstream was temporarily unavailable (e.g. mid-restart). Safe to retry."""


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
            # Database corresponding to the Semantic Layer (BigQuery).
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
    # double quotes b/c the sql query might include single quotes (and double quotes mean identifiers, not strings). So the line below
    # executes the url encoding without replacing any quotes within the sql query.
    urlencoded = "&".join([f"{k}={urllib.parse.quote_plus(json.dumps(v))}" for k, v in body.items()])

    # Send request.
    if mb_url is None:
        mb_url = METABASE_URL
    url = f"{mb_url}/api/dataset/csv"

    # Retry transient failures with exponential backoff. Metabase is restarted regularly by the
    # analytics pipeline (see RETRYABLE_STATUS_CODES), which can otherwise hard-fail an entire owidbot
    # run on a single 502 that resolves within a minute. Connection/timeout errors are retried too;
    # a permanent error (4xx, parse failure) is reraised immediately without retrying.
    def _request() -> requests.Response:
        response = requests.post(url, headers=headers, data=urlencoded, timeout=30)
        if response.status_code in RETRYABLE_STATUS_CODES:
            raise MetabaseTransientError(
                f"Metabase API temporarily unavailable (status code {response.status_code}). "
                "It is likely being restarted; retrying."
            )
        if not response.ok:
            raise RuntimeError(f"Metabase API request failed with status code {response.status_code}: {response.text}")
        return response

    def _log_retry(retry_state) -> None:
        # Logged at debug level on purpose: a transient 502/503/504 mid-restart is expected and
        # resolves on retry, so it shouldn't surface in Sentry. If all attempts are exhausted,
        # reraise=True propagates the exception, which is a genuine failure worth alerting on.
        log.debug(
            "metabase.request_retry",
            attempt=retry_state.attempt_number,
            error=str(retry_state.outcome.exception()),
        )

    for attempt in Retrying(
        retry=retry_if_exception_type(
            (MetabaseTransientError, requests.exceptions.ConnectionError, requests.exceptions.Timeout)
        ),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        before_sleep=_log_retry,
        reraise=True,
    ):
        with attempt:
            response = _request()

    # Create a dataframe with the returned data.
    df = pd.read_csv(BytesIO(response.content))

    return df


def _get_domain(prod: bool = False) -> str:
    if prod:
        domain = METABASE_URL
    else:
        domain = METABASE_URL_LOCAL
    return domain


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
    df = df.sort_values(by="views", ascending=False).reset_index(drop=True)  # ty: ignore

    #########################
    # Anonymous stats
    #########################
    # stats = mb.get("/api/analytics/anonymous-stats")
    return df
