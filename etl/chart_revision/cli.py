import traceback

import rich_click as click
from structlog import get_logger

from etl.chart_revision.v2.cli import main as main_v2
from etl.config import DEBUG

log = get_logger()


@click.command(name="chart-upgrade")
@click.argument(
    "mapping-file",
    type=str,
)
@click.option(
    "--revision-reason",
    "-r",
    default=None,
    help="Assign a reason for the suggested chart revision.",
)
def main_cli(mapping_file: str, revision_reason: str, use_version: int) -> None:
    """Generate chart revisions in Grapher using `MAPPING_FILE` JSON file.

    `MAPPING_FILE` is a JSON file mapping "old variables" to "new" ones. Typically old variables belong to a dataset that you want to deprecate and replace with a new one, which contains the "new variables".

    **Note 1:** Make sure that you are connected to the database. By default, it connects to Grapher based on the environment file found in the project's root directory "path/to/etl/.env".

    **Example:**

    ```json
    /* file: variable-mapping.json */
    {
        2032: 147395,
        2033: 147396
    }
    ```
    """
    try:
        main_v2(mapping_file, revision_reason)

    except Exception as e:
        log.error(e)
        if DEBUG:
            traceback.print_exc()
