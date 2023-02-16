from collections.abc import Iterable
from typing import Literal

import click
from pywebio import start_server
from rich import print

from . import charts, explorers, garden, grapher, meadow, snapshot, walden

PHASES = Literal["walden", "snapshot", "meadow", "garden", "grapher", "explorers", "charts"]


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
@click.option(
    "--port",
    default=8082,
    type=int,
    help="Application port",
)
def cli(phase: Iterable[PHASES], run_checks: bool, dummy_data: bool, auto_open: bool, port: int) -> None:
    print(f"Walkthrough has been opened at http://localhost:{port}/")
    if phase == "walden":
        phase_func = walden.app
    elif phase == "snapshot":
        phase_func = snapshot.app
    elif phase == "meadow":
        phase_func = meadow.app
    elif phase == "garden":
        phase_func = garden.app
    elif phase == "grapher":
        phase_func = grapher.app
    elif phase == "explorers":
        phase_func = explorers.app
    elif phase == "charts":
        phase_func = charts.app
    else:
        raise NotImplementedError(f"{phase} is not implemented yet.")

    start_server(
        lambda: phase_func(run_checks=run_checks, dummy_data=dummy_data),
        port=port,
        debug=True,
        auto_open_webbrowser=auto_open,
    )


if __name__ == "__main__":
    cli()
