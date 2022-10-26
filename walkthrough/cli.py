from collections.abc import Iterable
from typing import Literal

import click
from pywebio import start_server
from rich import print

from . import garden, grapher, meadow, walden

PHASES = Literal["walden", "meadow", "garden", "grapher"]


@click.command()
@click.argument(
    "phase",
    type=click.Choice(PHASES.__args__),  # type: ignore
)
@click.option(
    "--run-checks/--skip-checks",
    default=True,
    type=bool,
    help="Environment checks",
)
@click.option(
    "--dummy-data",
    is_flag=True,
    help="Prefill form with dummy data, useful for development",
)
@click.option(
    "--auto-open/--no-auto-open",
    is_flag=True,
    default=True,
    help="Auto open browser on port 8082",
)
def cli(phase: Iterable[PHASES], run_checks: bool, dummy_data: bool, auto_open: bool) -> None:
    print("Walkthrough has been opened at http://localhost:8082/")
    if phase == "walden":
        phase_func = walden.app
    elif phase == "meadow":
        phase_func = meadow.app
    elif phase == "garden":
        phase_func = garden.app
    elif phase == "grapher":
        phase_func = grapher.app
    else:
        raise NotImplementedError(f"{phase} is not implemented yet.")

    start_server(
        lambda: phase_func(run_checks=run_checks, dummy_data=dummy_data),
        port=8082,
        debug=True,
        auto_open_webbrowser=auto_open,
    )


if __name__ == "__main__":
    cli()
