"""The indicators the Search API ranks, and their published copy in R2.

The Search API embeds every non-archived indicator at startup. It reads them from
MySQL by default; when `SEARCH_INDICATORS_URL` is set it reads the published copy
instead, so a server that only serves search needs no database credentials. The
production ETL deploy publishes that copy after it updates the database:

    .venv/bin/python -m api_search.indicators
"""

import io
import tempfile
from pathlib import Path

import click
import pandas as pd
import structlog
from owid.catalog import s3_utils

from etl import config
from etl.db import read_sql
from etl.http import session as http_session

log = structlog.get_logger()

KEY = "search_indicators.parquet"
S3_URL = f"s3://owid-public/{KEY}"
PUBLIC_URL = f"https://files.ourworldindata.org/{KEY}"

# Everything in it comes from tables in the public metadata export (owid-grapher's db/exportMetadataTables.ts).
QUERY = """
SELECT
    v.id as variableId,
    v.name,
    COALESCE(v.description, v.descriptionShort, '') as description,
    COALESCE(v.catalogPath, CONCAT('indicator/', v.id)) AS catalogPath,
    COALESCE(cd_counts.n_charts, 0) as n_charts,
    COALESCE(ap.popularity, 0) as popularity
FROM datasets d
INNER JOIN variables v ON d.id = v.datasetId
LEFT JOIN (
    SELECT
        variableId,
        COUNT(DISTINCT chartId) as n_charts
    FROM chart_dimensions
    GROUP BY variableId
) cd_counts ON v.id = cd_counts.variableId
LEFT JOIN analytics_popularity ap ON ap.type = 'indicator' AND ap.slug = v.catalogPath
WHERE d.isArchived = 0
"""


def indicators_from_db() -> pd.DataFrame:
    return read_sql(QUERY)


def indicators_from_url(url: str) -> pd.DataFrame:
    response = http_session.get(url, timeout=120)
    response.raise_for_status()
    return pd.read_parquet(io.BytesIO(response.content))


def load_indicators() -> pd.DataFrame:
    """One row per indicator, with the fields of `apps.wizard.app_pages.indicator_search.data.Indicator`."""
    # This module doesn't build the `Indicator`s itself: their module imports the embedding libraries, which the
    # publishing CLI doesn't need.
    return indicators_from_url(config.SEARCH_INDICATORS_URL) if config.SEARCH_INDICATORS_URL else indicators_from_db()


def publish(df: pd.DataFrame) -> None:
    # The published copy replaces what production search ranks on its next restart, so an empty one would empty
    # every search result.
    assert not df.empty, "The query returned no indicators."
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir) / KEY
        df.to_parquet(path, index=False)
        s3_utils.upload(S3_URL, path, public=True, content_type="application/vnd.apache.parquet")
    log.info("search_indicators.published", url=PUBLIC_URL, indicators=len(df))


@click.command()
def main() -> None:
    """Publish the indicators from the database to R2, where production search reads them."""
    # There is one published copy and production search reads it, so a copy of a local or staging database would
    # replace production's list until the next deploy.
    if config.DATA_API_ENV != "production":
        raise click.ClickException(f"Only production publishes this copy, and DATA_API_ENV is {config.DATA_API_ENV!r}.")
    publish(indicators_from_db())


if __name__ == "__main__":
    main()
