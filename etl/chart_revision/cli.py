"""Generate chart revisions in Grapher using MAPPING_FILE JSON file.

MAPPING_FILE is a JSON file with old_variable_id -> new_variable_id pairs. E.g. {2032: 147395, 2033: 147396, ...}.

Make sure that you are connected to the database. By default, it connects to Grapher based on the environment file found in the project's root directory `path/to/etl/.env`.
"""
import json
import traceback
from typing import Optional

import click
from rich_click.rich_command import RichCommand
from structlog import get_logger

# TBD
from etl.chart_revision.deprecated import ChartRevisionSuggester
from etl.chart_revision.revision import create_and_submit_charts_revisions
from etl.config import DEBUG

log = get_logger()


@click.command(cls=RichCommand, help=__doc__)
@click.argument(
    "mapping-file",
    type=str,
)
@click.option("--revision-reason", default=None, help="Assign a reason for the suggested chart revision.")
@click.option("-o", "--old-version", is_flag=True, help="Use old backend version.")
def main_cli(mapping_file: str, revision_reason: str, old_version: str) -> None:
    try:
        if old_version:
            suggester = ChartRevisionSuggester.from_json(mapping_file, revision_reason)
            suggester.suggest()
        else:
            main(mapping_file, revision_reason)

    except Exception as e:
        log.error(e)
        if DEBUG:
            traceback.print_exc()


def main(mapping_file: str, revision_reason: Optional[str] = None):
    # Load mapping
    with open(mapping_file, "r") as f:
        variable_mapping = json.load(f)
        variable_mapping = {int(k): int(v) for k, v in variable_mapping.items()}
    create_and_submit_charts_revisions(variable_mapping, revision_reason)
