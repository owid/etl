import os
import tempfile
import time
import datetime as dt
from typing import Optional, cast

import click
import pandas as pd
import structlog
from owid.catalog.utils import validate_underscore
from owid.walden import Catalog as WaldenCatalog
from owid.walden.catalog import Dataset as WaldenDataset
from owid.walden.ingest import add_to_catalog
from sqlalchemy.engine import Engine

import concurrent.futures

from .backport import backport

from owid.catalog.utils import underscore

from etl.db import get_engine
from etl.files import checksum_str
from etl.grapher_model import (
    GrapherConfig,
    GrapherDatasetModel,
    GrapherSourceModel,
    GrapherVariableModel,
)
from etl.steps import WaldenStep

from etl.reindex import reindex
from etl.publish import publish
from etl.command import main as etl
from etl.command import run_dag

log = structlog.get_logger()

SLEEP_BETWEEN_RUNS = 1


@click.command()
@click.option(
    "--dry-run/--no-dry-run",
    default=False,
    type=bool,
    help="dry-run is applied only to publishing, other steps are executed without dry-run",
)
@click.option(
    "--dt-start",
    default=dt.datetime.utcnow(),
    type=click.DateTime(),
    help="Datetime to start updating datasets from",
)
@click.option(
    "--batch-size",
    default=10,
    type=int,
    help="How many datasets to process in parallel",
)
def fasttrack(
    dry_run: bool = False,
    dt_start: dt.datetime = dt.datetime.utcnow(),
    batch_size: int = 10,
) -> None:
    engine = get_engine()

    while True:
        # TODO: ORM might be nicer
        q = """
        select
            id as dataset_id,
            name as dataset_name,
            -- NOTE: updatedAt might be unnecessary
            GREATEST(updatedAt, metadataEditedAt, dataEditedAt) as latest_timestamp
        from datasets
        -- this assumes there are no ties if we are processing a lot of datasets
        where GREATEST(updatedAt, metadataEditedAt, dataEditedAt) > %(start)s
        order by latest_timestamp asc
        limit %(batch_size)s
        """
        df = pd.read_sql(
            q, engine, params={"start": dt_start, "batch_size": batch_size}
        )

        # wait if no results
        if df.empty:
            log.info("fasttrack.waiting", dt_start=dt_start)
            time.sleep(SLEEP_BETWEEN_RUNS)
            continue
        else:
            log.info("fasttrack.processing", dt_start=dt_start, datasets=len(df))

        # use latest timestamp of processed datasets as start for next batch
        dt_start = df.latest_timestamp.max().to_pydatetime()

        # run walden
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(
                lambda r: process_dataset(r.dataset_id, r.dataset_name, dry_run=False),
                df.itertuples(),
            )

        # refresh walden catalog manually
        log.info("fasttrack.refresh_walden", dt_start=dt_start)
        WaldenStep._walden_catalog.refresh()

        # run ETL
        log.info("fasttrack.etl", dt_start=dt_start)
        etl(
            steps=[f"dataset_{r.dataset_id}" for r in df.itertuples()],
            dry_run=False,
            force=True,
            private=True,
            backport=True,
            workers=1,
            walden_catalog=WaldenStep._walden_catalog,
        )

        # reindex and publish catalog
        reindex(channel=["backport"])
        log.info("fasttrack.end", dt_start=dt_start)
        publish(
            dry_run=dry_run,
            private=True,
            bucket="owid-catalog-staging",
            channel=["backport"],
        )


def process_dataset(dataset_id: int, dataset_name: str, dry_run: bool) -> None:
    log.info("process_dataset.start", dataset_id=dataset_id)
    # NOTE: we are not commiting changes to walden repo, this could be problematic
    # if the other processes are trying to rebase the repo
    backport(
        dataset_id=dataset_id,
        short_name=underscore(dataset_name),  # type: ignore
        dry_run=dry_run,
        force=True,
        upload=True,
    )
    log.info("process_dataset.end", dataset_id=dataset_id)


if __name__ == "__main__":
    fasttrack()
