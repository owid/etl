import concurrent.futures
import re
from typing import Any, cast

import pandas as pd
import rich_click as click
import structlog
from owid.catalog.utils import underscore
from sqlalchemy.engine import Engine

from etl import config
from etl.db import get_engine
from etl.snapshot import snapshot_catalog
from etl.steps import load_dag

from . import utils
from .backport import backport as backport_step

config.enable_bugsnag()

log = structlog.get_logger()


@click.command(name="bulk", deprecated=True)
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
    help="Upload dataset to S3",
)
@click.option(
    "--force/--no-force",
    default=False,
    type=bool,
    help="Force overwrite even if checksums match",
)
@click.option(
    "--prune/--no-prune",
    default=False,
    type=bool,
    help="Prune datasets from local snapshots that are not in DB anymore",
)
@click.option(
    "--backport/--skip-backport",
    default=True,
    type=bool,
    help="Backport datasets, can be skipped if you only want to prune",
)
@click.option(
    "--data-metadata/--skip-data-metadata",
    default=False,
    type=bool,
    help="Upload data & metadata JSON of variable to R2",
)
@click.option(
    "--all/--no-all",
    default=False,
    type=bool,
    help="Backport all datasets, even those without charts or archived",
)
@click.option(
    "--workers",
    type=int,
    help="Thread workers to parallelize which steps need rebuilding (steps execution is not parallelized)",
    default=1,
)
def bulk_backport(
    dataset_ids: tuple[int],
    dry_run: bool,
    limit: int,
    upload: bool,
    force: bool,
    prune: bool,
    backport: bool,
    data_metadata: bool,
    all: bool,
    workers: int,
) -> None:
    """Backport in bulk."""
    engine = get_engine()

    if backport:
        df = _active_datasets(engine, dataset_ids=list(dataset_ids), limit=limit, all=all)

        if dataset_ids:
            df = df.loc[df.id.isin(dataset_ids)]

        log.info("bulk_backport.start", n=len(df))

        if workers == 1:
            ds_row: Any
            for i, ds_row in enumerate(df.itertuples()):
                log.info(
                    "bulk_backport",
                    dataset_id=ds_row.id,
                    name=ds_row.name,
                    private=ds_row.isPrivate,
                    progress=f"{i + 1}/{len(df)}",
                )
                backport_step(
                    dataset_id=ds_row.id,
                    dry_run=dry_run,
                    upload=upload,
                    force=force,
                    engine=engine,
                    data_metadata=data_metadata,
                )
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                results = executor.map(
                    lambda ds_row: backport_step(
                        dataset_id=ds_row.id,
                        dry_run=dry_run,
                        upload=upload,
                        force=force,
                        engine=engine,
                        data_metadata=data_metadata,
                    ),
                    df.itertuples(),
                )

                # raise errors from futures, otherwise they'd fail silently
                list(results)

    if prune:
        _prune_snapshots(engine, dataset_ids, dry_run, all=all)

    log.info("bulk_backport.finished")


def _active_datasets(
    engine: Engine, dataset_ids: list[int] = [], all: bool = False, limit: int = 1000000
) -> pd.DataFrame:
    """Return dataframe of datasets with at least one chart that should be backported."""

    dag_backported_ids = _backported_ids_in_dag()

    # NOTE: pd.read_sql needs at least one value in a list, use arbitrary 0
    if not dag_backported_ids:
        dag_backported_ids = [0]

    if dataset_ids:
        q = """
        select
            id, name, dataEditedAt, metadataEditedAt, isPrivate
        from datasets
        where
            -- must not come from ETL
            sourceChecksum is null and
            id in %(dataset_ids)s
        """
    elif all:
        q = """
        select
            id, name, dataEditedAt, metadataEditedAt, isPrivate
        from datasets
        where
        -- and must not come from ETL, unless they're in DAG
        (sourceChecksum is null or id in %(dataset_ids)s)
        order by rand()
        limit %(limit)s
        """
    else:
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
            -- or be used in DAG or specified in CLI
            or id in %(dataset_ids)s
        )
        -- and must not come from ETL, unless they're in DAG
        and (sourceChecksum is null or id in %(dataset_ids)s)
        -- and must not be archived
        and not isArchived
        order by rand()
        limit %(limit)s
        """

    df = pd.read_sql(
        q,
        engine,
        params={
            "limit": limit,
            "dataset_ids": dag_backported_ids + dataset_ids,
        },
    )

    df["short_name"] = df.name.map(underscore)

    return cast(pd.DataFrame, df)


def _active_datasets_names(engine: Engine, all: bool) -> set[str]:
    """Load all active datasets from grapher and return dataset names of their config
    and value files."""
    active_datasets_df = _active_datasets(engine, all=all)
    names = set(active_datasets_df.apply(lambda r: utils.create_short_name(r["id"], r["name"]), axis=1))
    return {n + "_config" for n in names} | {n + "_values" for n in names}


def _prune_snapshots(engine: Engine, dataset_ids: tuple[int], dry_run: bool, all: bool) -> None:
    active_dataset_names = _active_datasets_names(engine, all=all)

    # load all backport snapshots
    snapshots = snapshot_catalog("backport/.*")

    # if given dataset ids, only prune those
    if dataset_ids:
        snapshots = [
            snap for snap in snapshots if utils.extract_id_from_short_name(snap.metadata.short_name) in dataset_ids
        ]

    # datasets that are not among active datasets
    # NOTE: it is important to compare not just dataset id, but the whole name as dataset name
    # can be changed by the user
    snapshots_to_delete = [snap for snap in snapshots if snap.metadata.short_name not in active_dataset_names]

    log.info("bulk_backport.delete", n=len(snapshots_to_delete))

    for snap in snapshots_to_delete:
        log.info("bulk_backport.delete_dataset", short_name=snap.metadata.short_name)

        if not dry_run:
            snap.delete_local()


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
