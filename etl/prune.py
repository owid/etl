import shutil
from pathlib import Path
from typing import Set
from urllib.parse import urlparse

import click
import structlog
from owid.catalog import CHANNEL, LocalCatalog

from etl import config, paths
from etl.command import construct_dag

config.enable_bugsnag()

log = structlog.get_logger()


EXCLUDE_STEP_TYPES = ("grapher", "walden", "walden-private", "github")


@click.command()
@click.option(
    "--dag-path",
    type=click.Path(exists=True),
    help="Path to DAG yaml file",
    default=paths.DEFAULT_DAG_FILE,
)
@click.option(
    "--data-dir",
    type=click.Path(exists=True),
    help="Path to data directory",
    default=paths.DATA_DIR,
)
@click.option("--dry-run", is_flag=True, help="Only print files that would be deleted")
def prune_cli(
    dag_path: Path,
    data_dir: Path,
    dry_run: bool,
) -> None:
    """Prune data without steps in the DAG."""
    return prune(dag_path=dag_path, data_dir=data_dir, dry_run=dry_run)


def prune(
    dag_path: Path = paths.DEFAULT_DAG_FILE,
    data_dir: Path = paths.DATA_DIR,
    dry_run: bool = False,
) -> None:
    dag_dirs = dag_datasets_dirs(dag_path)
    data_dirs = local_datasets_dirs(data_dir)

    to_delete = set(data_dirs) - set(dag_dirs)

    for path in to_delete:
        log.info("prune.delete", path=data_dir / path)
        if not dry_run:
            shutil.rmtree(data_dir / path)

    if to_delete:
        reindex_catalog(data_dir)


def reindex_catalog(data_dir: Path) -> None:
    # the index on disk is out of date, reindex
    LocalCatalog(data_dir, channels=CHANNEL.__args__).reindex()


def dag_datasets_dirs(dag_path: Path) -> Set[str]:
    """Return a list of directories that contain datasets in the DAG."""
    # make sure we get as many datasets as possible to avoid deleting `backport`
    # if `--backport` flag is not used
    dag = construct_dag(dag_path, backport=True, private=True, grapher=False)

    dataset_dirs = []
    for step_name in dag.keys():
        parts = urlparse(step_name)
        # NOTE: it is safer to exclude step types to avoid silently deleting unwanted data
        # if we ever add a new step type
        if parts.scheme not in EXCLUDE_STEP_TYPES:
            path = parts.netloc + parts.path
            dataset_dirs.append(path)

    return set(dataset_dirs)


def local_datasets_dirs(data_dir: Path) -> Set[str]:
    lc = LocalCatalog(data_dir, channels=CHANNEL.__args__)
    return {str(Path(p).parent) for p in lc.frame.path}


if __name__ == "__main__":
    prune()
