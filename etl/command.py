#!/usr/bin/env python
#
#  etl.py
#

from typing import Callable, List, Dict, Any
import time

import click

from etl.steps import load_dag, select_steps, parse_step
from etl import paths


@click.command()
@click.option("--dry-run", is_flag=True, help="Only print the steps that would be run")
@click.option(
    "--force", is_flag=True, help="Redo a step even if it appears done and up-to-date"
)
@click.argument("steps", nargs=-1)
def main(steps: List[str], dry_run: bool = False, force: bool = False) -> None:
    """
    Execute all ETL steps listed in dag.yaml
    """
    # Load our graph of steps and the things they depend on
    dag = load_dag(paths.DAG_FILE.as_posix())

    # Run the steps we have selected, and everything downstream of them
    run_dag(dag, steps, dry_run=dry_run, force=force)


def run_dag(
    dag: Dict[str, Any],
    selection: List[str],
    dry_run: bool = False,
    force: bool = False,
) -> None:
    """
    Run the selected steps, and anything that needs updating based on them. An empty
    selection means "run all steps".

    By default, data steps do not re-run if they appear to be up-to-date already by
    looking at their checksum.
    """
    step_names = select_steps(dag, selection)
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
