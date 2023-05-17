"""Generate chart revisions in Grapher using MAPPING_FILE JSON file.

MAPPING_FILE is a JSON file with old_variable_id -> new_variable_id pairs. E.g. {2032: 147395, 2033: 147396, ...}.

Make sure that you are connected to the database. By default, it connects to Grapher based on the environment file found in the project's root directory `path/to/etl/.env`.
"""
import traceback

import click
from rich_click.rich_command import RichCommand
from structlog import get_logger

from etl.chart_revision.v1.cli import main as main_v1

# TBD
from etl.chart_revision.v1.deprecated import ChartRevisionSuggester
from etl.chart_revision.v2.cli import main as main_v2
from etl.config import DEBUG

log = get_logger()
# Available versions
VERSIONS = ["0", "1", "2"]
VERSION_DEFAULT = max(VERSIONS)


@click.command(cls=RichCommand, help=__doc__)
@click.argument(
    "mapping-file",
    type=str,
)
@click.option("--revision-reason", default=None, help="Assign a reason for the suggested chart revision.")
@click.option(
    "-u",
    "--use-version",
    type=click.Choice(VERSIONS),
    default=VERSION_DEFAULT,
    help="Choose chart_revision backend version to use. By default uses latest version.",
)
def main_cli(mapping_file: str, revision_reason: str, use_version: int) -> None:
    """Chart revision backend client."""
    try:
        if use_version == "0":
            suggester = ChartRevisionSuggester.from_json(mapping_file, revision_reason)
            suggester.suggest()
        elif use_version == "1":
            main_v1(mapping_file, revision_reason)
        elif use_version == "2":
            main_v2(mapping_file, revision_reason)

    except Exception as e:
        log.error(e)
        if DEBUG:
            traceback.print_exc()
