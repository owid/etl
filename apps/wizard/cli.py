"""Interface to run the app from python.

This module is implemented so that we can run the app with the `python` keyword:

python cli.py
"""
import logging
import sys
from typing import Iterable

import rich_click as click
import streamlit.web.cli as stcli
from rich_click.rich_command import RichCommand

from apps.wizard.config import WIZARD_PHASES
from apps.wizard.utils import CURRENT_DIR

# Disable streamlit cache data API logging
# ref: @kajarenc from https://github.com/streamlit/streamlit/issues/6620#issuecomment-1564735996
logging.getLogger("streamlit.runtime.caching.cache_data_api").setLevel(logging.ERROR)
# RICH-CLICK CONFIGURATION
# click.rich_click.USE_RICH_MARKUP = True
click.rich_click.USE_MARKDOWN = True
click.rich_click.SHOW_ARGUMENTS = True
# click.rich_click.STYLE_HEADER_TEXT = "bold"
# click.rich_click.GROUP_ARGUMENTS_OPTIONS = True
# Show variable types under description
click.rich_click.SHOW_METAVARS_COLUMN = False
click.rich_click.APPEND_METAVARS_HELP = True
click.rich_click.OPTION_ENVVAR_FIRST = True


# NOTE: Any new arguments here need to be in sync with the arguments defined in
# wizard.utils.APP_STATE.args property method
@click.command(cls=RichCommand)
@click.argument(
    "phase",
    type=click.Choice(WIZARD_PHASES.__args__),  # type: ignore
    default="all",
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
    "--port",
    default=8053,
    type=int,
    help="Application port",
)
def cli(phase: Iterable[WIZARD_PHASES], run_checks: bool, dummy_data: bool, port: int) -> None:
    """Run OWID's ETL admin tool, the Wizard.

    ```
    ..__    __ _                  _
    ./ / /\ \ (_)______ _ _ __ __| |
    .\ \/  \/ | |_  / _` | '__/ _` |
    ..\  /\  /| |/ | (_| | | | (_| |
    ...\/  \/ |_/___\__,_|_|  \__,_|
    ```

    Just launch it and start using it! 🪄

    **Note:** Alternatively, you can run it as `streamlit run apps/wizard/app.py`.
    """
    script_path = CURRENT_DIR / "app.py"

    # Define command with arguments
    args = [
        "streamlit",
        "run",
        str(script_path),
        "--server.port",
        str(port),
        "--",
        "--phase",
        phase,
    ]
    if run_checks:
        args.append("--run-checks")
    if dummy_data:
        args.append("--dummy-data")
    sys.argv = args

    # Call
    sys.exit(stcli.main())
