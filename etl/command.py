#!/usr/bin/env python
#
#  etl.py
#

from typing import Callable, List, Dict, Any
import time
import sys

import click

from etl.steps import load_dag, compile_steps, parse_step
from etl import config


@click.command()
@click.option("--dry-run", is_flag=True, help="Only print the steps that would be run")
@click.option(
    "--force", is_flag=True, help="Redo a step even if it appears done and up-to-date"
)
@click.option(
    "--grapher", is_flag=True, help="Publish changes to grapher (OWID staff only)"
)
@click.argument("steps", nargs=-1)
def main(
    steps: List[str], dry_run: bool = False, force: bool = False, grapher: bool = False
) -> None:
    """
    Execute all ETL steps listed in dag.yaml
    """
    if grapher:
        sanity_check_db_settings()

    # Load our graph of steps and the things they depend on
    dag = load_dag()

    # Run the steps we have selected, and everything downstream of them
    run_dag(dag, steps, dry_run=dry_run, force=force, include_grapher=grapher)


def sanity_check_db_settings():
    """
    Give a nice error if the DB has not been configured.
    """
    if config.GRAPHER_USER_ID is None:
        click.echo("ERROR: No grapher user id has been set in the environment.")
        click.echo("       Did you configure the MySQL connection in .env?")
        sys.exit(1)


def run_dag(
    dag: Dict[str, Any],
    selection: List[str],
    dry_run: bool = False,
    force: bool = False,
    include_grapher: bool = False,
) -> None:
    """
    Run the selected steps, and anything that needs updating based on them. An empty
    selection means "run all steps".

    By default, data steps do not re-run if they appear to be up-to-date already by
    looking at their checksum.
    """
    step_names = compile_steps(dag, selection, include_grapher=include_grapher)
    steps = [parse_step(name, dag) for name in step_names if name != "data://reference"]

    if not force:
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
