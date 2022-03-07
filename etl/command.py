#!/usr/bin/env python
#
#  etl.py
#

from typing import Callable, List, Any, Optional
import time
import sys
import re
from pathlib import Path

import click

from etl.steps import load_dag, compile_steps, DAG, paths
from etl import config


@click.command()
@click.option("--dry-run", is_flag=True, help="Only print the steps that would be run")
@click.option(
    "--force", is_flag=True, help="Redo a step even if it appears done and up-to-date"
)
@click.option("--private", is_flag=True, help="Execute private steps")
@click.option(
    "--grapher", is_flag=True, help="Publish changes to grapher (OWID staff only)"
)
@click.option("--exclude", help="Comma-separated patterns to exclude")
@click.option(
    "--dag-path",
    type=click.Path(exists=True),
    help="Path to DAG yaml file",
    default=paths.DAG_FILE,
)
@click.argument("steps", nargs=-1)
def main(
    steps: List[str],
    dry_run: bool = False,
    force: bool = False,
    private: bool = False,
    grapher: bool = False,
    exclude: Optional[str] = None,
    dag_path: Path = paths.DAG_FILE,
) -> None:
    """
    Execute all ETL steps listed in dag.yaml
    """
    if grapher:
        sanity_check_db_settings()

    # Load our graph of steps and the things they depend on
    dag = load_dag(dag_path)

    excludes = exclude.split(",") if exclude else []

    # Run the steps we have selected, and everything downstream of them
    run_dag(
        dag,
        steps,
        dry_run=dry_run,
        force=force,
        private=private,
        include_grapher=grapher,
        excludes=excludes,
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
    excludes: Optional[List[str]] = None,
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
        # NOTE: could this possibly go to `excludes`?
        dag = _filter_private_steps(dag)
        print("Excluding private steps...")

    steps = compile_steps(dag, includes, excludes)

    if not force:
        print("Detecting which steps need rebuilding...")
        steps = [s for s in steps if s.is_dirty()]

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


def _filter_private_steps(dag: DAG) -> DAG:
    return {k: v for k, v in dag.items() if not _is_private_step(k)}


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


if __name__ == "__main__":
    main()
