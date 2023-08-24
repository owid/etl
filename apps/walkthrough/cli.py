import os
from collections.abc import Iterable
from typing import Literal

import click
from pywebio import start_server
from pywebio.session import go_app
from rich import print

from . import charts, explorers, garden, grapher, meadow, snapshot, utils

PHASES = Literal["all", "snapshot", "meadow", "garden", "grapher", "explorers", "charts", "charts-old"]


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
    if phase == "charts":
        import sys

        from streamlit.web import cli as stcli

        script_path = os.path.join(os.path.dirname(__file__), "charts_v2/__main__.py")
        sys.argv = ["streamlit", "run", script_path, "--server.port", str(port)]
        sys.exit(stcli.main())
    else:
        apps = {
            "snapshot": snapshot.app,
            "meadow": meadow.app,
            "garden": garden.app,
            "grapher": grapher.app,
            "explorers": explorers.app,
            "charts-old": charts.app,
        }

        # prefill state with dummy data
        if dummy_data:
            utils.APP_STATE = utils.DUMMY_DATA

        apps_to_start = {app_name: lambda app=app: app(run_checks=run_checks) for app_name, app in apps.items()}

        def index():
            go_app(phase, new_window=False)

        # start only one app if given phase, otherwise show default index page
        if phase != "all":
            apps_to_start["index"] = index  # type: ignore

        start_server(
            apps_to_start,
            port=port,
            debug=True,
            auto_open_webbrowser=True,
        )


if __name__ == "__main__":
    cli()
