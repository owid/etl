#!/usr/bin/env python
#
#  etl.py
#

from typing import Callable, List, Any, Optional
import time
import sys

import click

from etl.steps import load_dag, compile_steps, DAG
from etl import config


@click.command()
@click.option("--dry-run", is_flag=True, help="Only print the steps that would be run")
@click.option(
    "--force", is_flag=True, help="Redo a step even if it appears done and up-to-date"
)
@click.option(
    "--grapher", is_flag=True, help="Publish changes to grapher (OWID staff only)"
)
@click.option("--exclude", help="Comma-separated patterns to exclude")
@click.argument("steps", nargs=-1)
def main(
    steps: List[str],
    dry_run: bool = False,
    force: bool = False,
    grapher: bool = False,
    exclude: Optional[str] = None,
) -> None:
    """
    Execute all ETL steps listed in dag.yaml
    """
    if grapher:
        sanity_check_db_settings()

    # Load our graph of steps and the things they depend on
    dag = load_dag()

    excludes = exclude.split(",") if exclude else []

    # Run the steps we have selected, and everything downstream of them
    run_dag(
        dag,
        steps,
        dry_run=dry_run,
        force=force,
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


if __name__ == "__main__":
    main()
