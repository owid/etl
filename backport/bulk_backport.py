import re

import click
import pandas as pd
import structlog
from owid.catalog.utils import underscore

from etl.db import get_engine
from etl.steps import load_dag
from etl import config

from .backport import backport

config.enable_bugsnag()

log = structlog.get_logger()


@click.command()
@click.option("--dataset-ids", "-d", type=int, multiple=True)
@click.option(
    "--dry-run/--no-dry-run",
    default=False,
    type=bool,
    help="Do not add dataset to a catalog on dry-run",
)
@click.option(
    "--limit",
    default=1000000,
    type=int,
)
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Walden",
)
@click.option(
    "--force/--no-force",
    default=False,
    type=bool,
    help="Force overwrite even if checksums match",
)
def bulk_backport(
    dataset_ids: list[int], dry_run: bool, limit: int, upload: bool, force: bool
) -> None:
    engine = get_engine()

    dag_backported_ids = _backported_ids_in_dag()

    # NOTE: pd.read_sql needs at least one value in a list, use arbitrary 0
    if not dag_backported_ids:
        dag_backported_ids = [0]

    q = """
    select
        id, name, dataEditedAt, metadataEditedAt, isPrivate
    from datasets
    where
    (
        -- must be used in at least one chart
        id in (
            select distinct v.datasetId from chart_dimensions as cd
            join variables as v on cd.variableId = v.id
        )
        -- or be used in DAG
        or id in %(dataset_ids)s
    )
    -- and must not come from ETL
    and sourceChecksum is null
    order by rand()
    limit %(limit)s
    """

    # ignore limit if using dataset ids
    if dataset_ids:
        limit = 1000000

    df = pd.read_sql(
        q,
        engine,
        params={"limit": limit, "dataset_ids": dag_backported_ids},
    )

    if dataset_ids:
        df = df[df.id.isin(dataset_ids)]

    df["short_name"] = df.name.map(underscore)

    log.info("bulk_backport.start", n=len(df))

    for i, ds in enumerate(df.itertuples()):
        log.info(
            "bulk_backport",
            dataset_id=ds.id,
            name=ds.name,
            private=ds.isPrivate,
            progress=f"{i + 1}/{len(df)}",
        )
        backport(
            dataset_id=ds.id,
            short_name=ds.short_name,
            dry_run=dry_run,
            upload=upload,
            force=force,
        )

    log.info("bulk_backport.finished")


def _backported_ids_in_dag() -> list[int]:
    """Get all backported dataset ids used in DAG. This is helpful if someone uses backported
    dataset without any charts."""
    dag = load_dag()

    all_steps = list(dag.keys()) + [x for vals in dag.values() for x in vals]

    out = set()
    for s in all_steps:
        match = re.search(r"backport\/owid\/latest\/dataset_(\d+)_", s)
        if match:
            out.add(int(match.group(1)))

    return list(out)


if __name__ == "__main__":
    # Example (run against staging DB):
    #   bulk_backport -d 20 -d 21 -d 5426 --dry-run
    bulk_backport()
