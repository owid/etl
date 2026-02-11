import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional, cast

import rich_click as click
import structlog
from owid.catalog.utils import underscore
from rich import print
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from apps.backport.backport import PotentialBackport
from apps.utils.files import add_to_dag, generate_step
from etl import config
from etl.backport_helpers import create_dataset
from etl.db import get_engine
from etl.files import yaml_dump
from etl.metadata_export import metadata_export
from etl.paths import DAG_DIR, SNAPSHOTS_DIR, STEP_DIR

config.enable_sentry()

log = structlog.get_logger()

CURRENT_DIR = Path(__file__).parent

DAG_MIGRATED_PATH = DAG_DIR / "migrated.yml"


@click.command(name="migrate")
@click.option(
    "--dataset-id",
    type=int,
    required=True,
    help="Dataset ID to migrate",
)
@click.option(
    "--namespace",
    type=str,
    required=True,
    help="New namespace",
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
    help="New short name to use, underscored dataset name by default",
)
@click.option(
    "--backport/--no-backport",
    default=True,
    type=bool,
    help="Backport dataset before migrating",
)
@click.option(
    "--force/--no-force",
    default=False,
    type=bool,
    help="Force overwrite even if checksums match",
)
@click.option(
    "--dry-run/--no-dry-run",
    default=False,
    type=bool,
    help="Do not add dataset to a catalog on dry-run",
)
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to S3 as snapshot",
)
@click.option(
    "--run/--no-run",
    default=False,
    type=bool,
    help="Run snapshot, ETL+grapher, and indicator upgrade after generating files",
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
    run: bool = False,
) -> None:
    """Migrate existing dataset from MySQL into ETL.

    It imports datasets into ETL from MySQL by creating:
    - Ingest script that downloads data from S3 backport
    - Garden step with YAML metadata
    - Grapher step

    With `--run`, it also executes the full pipeline: snapshot, ETL+grapher,
    and indicator upgrade (match + upgrade).

    **Example:**

    ```
    ENV=.env.live etl b migrate --dataset-id 5205 --namespace covid --short-name hospital__and__icu --no-backport
    etl b migrate --dataset-id 2660 --namespace trade --run
    ```
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
        run=run,
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
    run: bool = False,
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

    if run and not dry_run:
        _run_full_pipeline(dataset_id, namespace, version, short_name, engine)
    else:
        # Print instructions for manual execution
        print("\n[bold yellow]Follow-up instructions:[/bold yellow]")
        print("[green]1.[/green] Create a PR")
        print(f"[green]2.[/green] Execute snapshot with [bold]`etls {namespace}/{version}/{short_name}`[/bold]")
        print(f"[green]3.[/green] Run dataset with [bold]`etlr {namespace}/{version}/{short_name} --grapher`[/bold]")
        print("[green]4.[/green] Run indicator upgrader in [bold]`make wizard`[/bold]")
        print("[green]5.[/green] Merge your PR, then [bold]delete[/bold] or archive the old dataset")


def _run_full_pipeline(old_dataset_id: int, namespace: str, version: str, short_name: str, engine: Engine) -> None:
    """Run the full migration pipeline: snapshot, ETL+grapher, and indicator upgrade."""
    from apps.indicator_upgrade.match import main as match_main
    from apps.indicator_upgrade.upgrade import cli_upgrade_indicators
    from etl.grapher.model import Dataset as GrapherDataset

    dataset_path = f"{namespace}/{version}/{short_name}"
    etls = str(Path(sys.executable).parent / "etls")
    etlr = str(Path(sys.executable).parent / "etlr")

    # 1. Run snapshot
    log.info("migrate.run_snapshot", dataset_path=dataset_path)
    subprocess.run([etls, dataset_path], check=True)

    # 2. Run ETL + grapher
    log.info("migrate.run_etl_grapher", dataset_path=dataset_path)
    subprocess.run([etlr, dataset_path, "--grapher", "--private"], check=True)

    # 3. Look up the new dataset ID
    catalog_path = f"{namespace}/{version}/{short_name}"
    with Session(engine) as session:
        ds = session.query(GrapherDataset).filter(GrapherDataset.catalogPath == catalog_path).one()
        new_dataset_id = ds.id

    log.info("migrate.new_dataset_created", old_id=old_dataset_id, new_id=new_dataset_id)

    # 4. Match indicators (non-interactive, perfect matches only)
    log.info("migrate.match_indicators", old_id=old_dataset_id, new_id=new_dataset_id)
    match_main(
        old_dataset_id=old_dataset_id,
        new_dataset_id=new_dataset_id,
        dry_run=False,
        match_identical=True,
        no_interactive=True,
        auto_threshold=100.0,
        quiet=True,
    )

    # 5. Upgrade indicators
    log.info("migrate.upgrade_indicators")
    cli_upgrade_indicators(dry_run=False)

    print(f"\n[bold green]Migration complete![/bold green] Dataset {old_dataset_id} → {new_dataset_id}")
    print("[green]Charts have been updated to use the new variables.[/green]")


def _add_to_migrated_dag(namespace: str, version: str, short_name: str):
    add_to_dag(
        {f"data://grapher/{namespace}/{version}/{short_name}": {f"data://garden/{namespace}/{version}/{short_name}"}},
        dag_path=DAG_MIGRATED_PATH,
    )
    add_to_dag(
        {
            f"data://garden/{namespace}/{version}/{short_name}": {
                f"snapshot://{namespace}/{version}/{short_name}.feather"
            }
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
