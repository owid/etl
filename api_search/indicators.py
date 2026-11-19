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
import requests
import structlog
from owid.catalog import s3_utils

from apps.wizard.app_pages.indicator_search.data import Indicator
from etl import config
from etl.db import read_sql

log = structlog.get_logger()

S3_URL = "s3://owid-public/search_indicators.parquet"
PUBLIC_URL = "https://files.ourworldindata.org/search_indicators.parquet"

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
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    return pd.read_parquet(io.BytesIO(response.content))


def load_indicators() -> list[Indicator]:
    df = indicators_from_url(config.SEARCH_INDICATORS_URL) if config.SEARCH_INDICATORS_URL else indicators_from_db()
    return [Indicator(**row) for row in df.to_dict(orient="records")]  # ty: ignore


def publish(df: pd.DataFrame) -> None:
    # The published copy replaces what production search ranks on its next restart, so an empty one would empty
    # every search result.
    assert not df.empty, "The query returned no indicators."
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir) / "search_indicators.parquet"
        df.to_parquet(path, index=False)
        s3_utils.upload(S3_URL, path, public=True, content_type="application/vnd.apache.parquet")
    log.info("search_indicators.published", url=PUBLIC_URL, indicators=len(df))


@click.command()
def main() -> None:
    """Publish the indicators from the database to R2, where production search reads them."""
    publish(indicators_from_db())


if __name__ == "__main__":
    main()
