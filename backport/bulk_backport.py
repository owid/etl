import click
import pandas as pd
import structlog
from owid.catalog.utils import underscore

from etl.db import get_engine

from .backport import backport

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
def bulk_backport(
    dataset_ids: list[int], dry_run: bool, limit: int, upload: bool
) -> None:
    engine = get_engine()

    q = """
    select
        id, name, dataEditedAt, metadataEditedAt, isPrivate
    from datasets
    where id in (
        select distinct v.datasetId from chart_dimensions as cd
        join variables as v on cd.variableId = v.id
    )
    order by rand()
    limit %(limit)s
    """
    df = pd.read_sql(q, engine, params={"limit": limit})

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
        )

    log.info("bulk_backport.finished")


if __name__ == "__main__":
    # Example (run against staging DB):
    #   bulk_backport -d 20 -d 21 -d 5426 --dry-run
    bulk_backport()
