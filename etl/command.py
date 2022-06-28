#!/usr/bin/env python
#
#  etl.py
#

from typing import Callable, List, Any, Optional
import os
import time
import sys
import re
from pathlib import Path

import click

from etl.steps import load_dag, compile_steps, select_dirty_steps, DAG, paths
from etl import config
from owid.walden import Catalog as WaldenCatalog, CATALOG as WALDEN_CATALOG


WALDEN_NAMESPACE = os.environ.get("WALDEN_NAMESPACE", "backport")


THREADPOOL_WORKERS = 5


@click.command()
@click.option("--dry-run", is_flag=True, help="Only print the steps that would be run")
@click.option(
    "--force", is_flag=True, help="Redo a step even if it appears done and up-to-date"
)
@click.option("--private", is_flag=True, help="Execute private steps")
@click.option(
    "--grapher", is_flag=True, help="Publish changes to grapher (OWID staff only)"
)
@click.option(
    "--backport",
    is_flag=True,
    help="Add steps for backporting OWID datasets",
)
@click.option(
    "--downstream",
    is_flag=True,
    help="Include downstream dependencies (steps that depend on the included steps)",
)
@click.option(
    "--only",
    is_flag=True,
    help="Only run the selected step (no upstream or downstream dependencies). Overrides `downstream` option",
)
@click.option("--exclude", help="Comma-separated patterns to exclude")
@click.option(
    "--dag-path",
    type=click.Path(exists=True),
    help="Path to DAG yaml file",
    default=paths.DAG_FILE,
)
@click.option(
    "--workers",
    type=int,
    help="Thread workers to parallelize which steps need rebuilding (steps execution is not parallelized)",
    default=5,
)
@click.argument("steps", nargs=-1)
def main_cli(
    steps: List[str],
    dry_run: bool = False,
    force: bool = False,
    private: bool = False,
    grapher: bool = False,
    backport: bool = False,
    downstream: bool = False,
    only: bool = False,
    exclude: Optional[str] = None,
    dag_path: Path = paths.DAG_FILE,
    workers: int = 5,
) -> None:
    return main(
        steps=steps,
        dry_run=dry_run,
        force=force,
        private=private,
        grapher=grapher,
        backport=backport,
        downstream=downstream,
        only=only,
        exclude=exclude,
        dag_path=dag_path,
        workers=workers,
    )


def main(
    steps: List[str],
    dry_run: bool = False,
    force: bool = False,
    private: bool = False,
    grapher: bool = False,
    backport: bool = False,
    downstream: bool = False,
    only: bool = False,
    exclude: Optional[str] = None,
    dag_path: Path = paths.DAG_FILE,
    workers: int = 5,
) -> None:
    """
    Execute all ETL steps listed in dag.yaml
    """
    if grapher:
        sanity_check_db_settings()

    # Load our graph of steps and the things they depend on
    dag = load_dag(dag_path)

    # Add all steps for backporting datasets (there are currently >800 of them)
    if backport:
        dag.update(_backporting_steps(private, walden_catalog=WALDEN_CATALOG))

    excludes = exclude.split(",") if exclude else []

    # Run the steps we have selected, and everything downstream of them
    run_dag(
        dag,
        steps,
        dry_run=dry_run,
        force=force,
        private=private,
        include_grapher=grapher,
        downstream=downstream,
        only=only,
        excludes=excludes,
        workers=workers,
    )


def sanity_check_db_settings() -> None:
    """
    Give a nice error if the DB has not been configured.
    """
    if config.GRAPHER_USER_ID is None:
        click.echo("ERROR: No grapher user id has been set in the environment.")
        click.echo("       Did you configure the MySQL connection in .env?")
        sys.exit(1)


def run_dag(
    dag: DAG,
    includes: Optional[List[str]] = None,
    dry_run: bool = False,
    force: bool = False,
    private: bool = False,
    include_grapher: bool = False,
    downstream: bool = False,
    only: bool = False,
    excludes: Optional[List[str]] = None,
    workers: int = 1,
) -> None:
    """
    Run the selected steps, and anything that needs updating based on them. An empty
    selection means "run all steps".

    By default, data steps do not re-run if they appear to be up-to-date already by
    looking at their checksum.
    """
    excludes = excludes or []
    if not include_grapher:
        excludes.append("grapher://")

    _validate_private_steps(dag)

    if not private:
        excludes.append("-private://")

    steps = compile_steps(dag, includes, excludes, downstream=downstream, only=only)

    if not force:
        print("Detecting which steps need rebuilding...")
        steps = select_dirty_steps(steps, workers)

    if not steps:
        print("All datasets up to date!")
        return

    print(f"Running {len(steps)} steps:")
    for i, step in enumerate(steps, 1):
        print(f"{i}. {step}...")
        if not dry_run:
            time_taken = timed_run(lambda: step.run())
            click.echo(f"{click.style('OK', fg='blue')} ({time_taken:.0f}s)")
            print()


def timed_run(f: Callable[[], Any]) -> float:
    start_time = time.time()
    f()
    return time.time() - start_time


def _validate_private_steps(dag: DAG) -> None:
    """Make sure there are no public steps that have private steps as dependency."""
    for step_name, step_dependencies in dag.items():
        if _is_private_step(step_name):
            continue
        for dependency in step_dependencies:
            if _is_private_step(dependency):
                raise ValueError(
                    f"Public step {step_name} has a dependency on private {dependency}"
                )


def _is_private_step(step_name: str) -> bool:
    return bool(re.findall(r".*?-private://", step_name))


def _backporting_steps(private: bool, walden_catalog: WaldenCatalog) -> DAG:
    """Return a DAG of steps for backporting datasets."""
    dag: DAG = {}

    # load all backported datasets from walden
    for ds in walden_catalog.find(namespace=WALDEN_NAMESPACE):

        # skip private backported steps
        if not private and not ds.is_public:
            continue

        # two files are generated for each dataset, skip one
        if ds.short_name.endswith("_values"):
            short_name = ds.short_name.removesuffix("_values")

            private_suffix = "" if ds.is_public else "-private"

            dag[f"backport{private_suffix}://backport/owid/latest/{short_name}"] = {
                f"walden{private_suffix}://{ds.namespace}/latest/{short_name}_values",
                f"walden{private_suffix}://{ds.namespace}/latest/{short_name}_config",
            }

    return dag


if __name__ == "__main__":
    main_cli()
