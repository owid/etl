import os
import shutil
from pathlib import Path
from typing import List
from urllib.parse import urlparse

import click
import structlog

from etl.command import construct_dag
from etl.steps import paths

log = structlog.get_logger()


@click.command()
@click.option(
    "--dag-path",
    type=click.Path(exists=True),
    help="Path to DAG yaml file",
    default=paths.DAG_FILE,
)
@click.option(
    "--data-dir",
    type=click.Path(exists=True),
    help="Path to data directory",
    default=paths.DATA_DIR,
)
@click.option("--dry-run", is_flag=True, help="Only print files that would be deleted")
def prune(
    dag_path: Path = paths.DAG_FILE,
    data_dir: Path = paths.DATA_DIR,
    dry_run: bool = False,
) -> None:
    """Prune data without steps in the DAG."""
    dag_dirs = dag_datasets_dirs(dag_path)
    data_dirs = local_datasets_dirs(data_dir)

    to_delete = set(data_dirs) - set(dag_dirs)

    for path in to_delete:
        log.info("prune.delete", path=data_dir / path)
        if not dry_run:
            shutil.rmtree(data_dir / path)


def dag_datasets_dirs(dag_path: Path) -> List[str]:
    """Return a list of directories that contain datasets in the DAG."""
    # make sure we get as many datasets as possible to avoid deleting `backport`
    # if `--backport` flag is not used
    dag = construct_dag(dag_path, backport=True, private=True)

    dataset_dirs = []
    for step_name in dag.keys():
        parts = urlparse(step_name)
        path = parts.netloc + parts.path
        dataset_dirs.append(path)

    return dataset_dirs


def local_datasets_dirs(data_dir: Path) -> List[str]:
    """Walk /data folder and return all directories that contain datasets, i.e.
    any feather file."""
    dataset_dirs = []

    # old approach, but extremely fast
    for root, _, files in os.walk(data_dir):
        if str(data_dir) == root:
            continue

        if any(f.endswith(".feather") for f in files):
            # safeguard against deleting unwanted files
            assert all(f.split(".")[-1] in {"feather", "json", "csv"} for f in files)

            dataset_dirs.append(Path(root).relative_to(data_dir))

    return [str(d) for d in dataset_dirs]


if __name__ == "__main__":
    prune()
