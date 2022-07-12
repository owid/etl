from collections.abc import Iterable
from typing import Literal

import click
from pywebio import start_server
from rich import print

from . import walden

PHASES = Literal["walden", "meadow", "garden", "grapher"]


@click.command()
@click.argument(
    "phase",
    type=click.Choice(PHASES.__args__),
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
    help="Prefill form with dummy data",
)
def cli(phase: Iterable[PHASES], run_checks: bool, dummy_data: bool) -> None:
    print("Walkthrough has been opened at http://localhost:8082/")
    if phase == "walden":
        start_server(
            lambda: walden.app(run_checks=run_checks, dummy_data=dummy_data),
            port=8082,
            debug=True,
            auto_open_webbrowser=True,
        )
    else:
        raise NotImplementedError(f"{phase} is not implemented yet.")


if __name__ == "__main__":
    cli()
