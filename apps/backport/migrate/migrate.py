import shutil
from pathlib import Path
from typing import Optional, cast

import rich_click as click
import structlog
from owid.catalog.utils import underscore
from rich import print
from sqlalchemy.engine import Engine

from apps.backport.backport import PotentialBackport
from apps.utils.files import add_to_dag, generate_step
from etl import config
from etl.backport_helpers import create_dataset
from etl.db import get_engine
from etl.files import yaml_dump
from etl.metadata_export import metadata_export
from etl.paths import DAG_DIR, SNAPSHOTS_DIR, STEP_DIR

config.enable_bugsnag()

log = structlog.get_logger()

CURRENT_DIR = Path(__file__).parent

DAG_MIGRATED_PATH = DAG_DIR / "migrated.yml"


@click.command()
@click.option(
    "--dataset-id",
    type=int,
    required=True,
    show_default=True,
    help="Dataset ID to migrate",
)
@click.option(
    "--namespace",
    type=str,
    required=True,
    help="New namespace",
    show_default=True,
)
@click.option(
    "--version",
    type=str,
    required=False,
    default="latest",
    help="New version",
)
@click.option(
    "--short-name",
    type=str,
    required=False,
    show_default=True,
    help="New short name to use, underscored dataset name by default",
)
@click.option(
    "--backport/--no-backport",
    default=True,
    show_default=True,
    type=bool,
    help="Backport dataset before migrating",
)
@click.option(
    "--force/--no-force",
    default=False,
    show_default=True,
    type=bool,
    help="Force overwrite even if checksums match",
)
@click.option(
    "--dry-run/--no-dry-run",
    default=False,
    show_default=True,
    type=bool,
    help="Do not add dataset to a catalog on dry-run",
)
@click.option(
    "--upload/--skip-upload",
    default=True,
    show_default=True,
    type=bool,
    help="Upload dataset to S3 as snapshot",
)
def cli(
    dataset_id: int,
    namespace: str,
    version: str,
    short_name: Optional[str] = None,
    backport: bool = True,
    force: bool = False,
    dry_run: bool = False,
    upload: bool = True,
) -> None:
    """Migrate existing dataset from MySQL into ETL.

    # Description

    It imports datasets into ETL from MySQL by creating:
    - Ingest script that downloads data from S3 backport
    - Garden step with YAML metadata
    - Grapher step

    ## Example

    ```
    ENV=.env.live etlcli b migrate --dataset-id 5205 --namespace covid --short-name hospital__and__icu --no-backport
    ```

    # Reference
    """
    return migrate(
        dataset_id=dataset_id,
        namespace=namespace,
        version=version,
        short_name=short_name,
        backport=backport,
        force=force,
        dry_run=dry_run,
        upload=upload,
    )


def migrate(
    dataset_id: int,
    namespace: str,
    version: str = "latest",
    short_name: Optional[str] = None,
    backport: bool = True,
    force: bool = False,
    dry_run: bool = False,
    upload: bool = True,
    engine: Optional[Engine] = None,
) -> None:
    lg = log.bind(dataset_id=dataset_id)

    engine = engine or get_engine()

    # load metadata from MySQL
    pb = PotentialBackport(dataset_id)
    pb.load(engine)

    if not short_name:
        short_name = underscore(pb.ds.name)
    short_name = cast(str, short_name)

    if backport:
        lg.info("migrate.backport_dataset")
        # backport to refresh snapshots in S3
        if force or pb.needs_update():
            pb.upload(upload, dry_run, engine)

    # load both snapshots and recreate Dataset from it
    _generate_metadata_yaml(namespace, version, short_name, pb.short_name)

    # create step files
    _generate_step_files(namespace, version, short_name, pb.short_name)

    # add steps to DAG
    _add_to_migrated_dag(namespace, version, short_name)

    # Print instructions
    print("\n[bold yellow]Follow-up instructions:[/bold yellow]")
    print(
        f"[green]1.[/green] Execute snapshot with [bold]`python snapshots/{namespace}/{version}/{short_name}.py`[/bold]"
    )
    print(f"[green]2.[/green] Import dataset with [bold]`etl {namespace}/{version}/{short_name} --grapher`[/bold]")
    print(
        f"[green]3.[/green] Merge changes or run it directly against production with [bold]`ENV=.env.prod.write etl {namespace}/{version}/{short_name} --grapher`[/bold]"
    )
    print("[green]4.[/green] Run chart revisions with [bold]`ENV=.env.prod.write etlwiz charts`[/bold]")
    print("[green]5.[/green] [bold]Delete[/bold] or archive the old dataset")


def _add_to_migrated_dag(namespace: str, version: str, short_name: str):
    add_to_dag(
        {f"data://grapher/{namespace}/{version}/{short_name}": [f"data://garden/{namespace}/{version}/{short_name}"]},
        dag_path=DAG_MIGRATED_PATH,
    )
    add_to_dag(
        {
            f"data://garden/{namespace}/{version}/{short_name}": [
                f"snapshot://{namespace}/{version}/{short_name}.feather"
            ]
        },
        dag_path=DAG_MIGRATED_PATH,
    )


def _generate_step_files(namespace: str, version: str, short_name: str, backport_short_name: str):
    cookiecutter_data = {
        "namespace": namespace,
        "version": version,
        "short_name": short_name,
        "backport_short_name": backport_short_name,
    }

    generate_step(CURRENT_DIR / "snapshot_cookiecutter/", cookiecutter_data, SNAPSHOTS_DIR)
    generate_step(CURRENT_DIR / "garden_cookiecutter/", cookiecutter_data, STEP_DIR / "data" / "garden")
    generate_step(CURRENT_DIR / "grapher_cookiecutter/", cookiecutter_data, STEP_DIR / "data" / "grapher")


def _generate_metadata_yaml(namespace: str, version: str, short_name: str, backport_short_name: str) -> None:
    # NOTE: loading values is wasteful, we only need metadata
    shutil.rmtree("/tmp/migrate", ignore_errors=True)
    ds = create_dataset("/tmp/migrate", backport_short_name, new_short_name=short_name)

    ds.metadata.namespace = namespace
    ds.metadata.channel = "garden"
    ds.metadata.version = version
    ds.metadata.short_name = short_name

    meta = metadata_export(ds)

    # remove source and description which is already in snapshot
    meta["dataset"].pop("sources")
    meta["dataset"].pop("description")

    yml_path = STEP_DIR / f"data/{ds.metadata.uri}.meta.yml"

    yml_path.parent.mkdir(parents=True, exist_ok=True)

    with open(yml_path, "w") as f:
        f.write(yaml_dump(meta))  # type: ignore


if __name__ == "__main__":
    cli()
