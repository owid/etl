import json
from typing import Optional, Tuple, get_args

import click
import structlog
from joblib import Memory
from rich_click.rich_command import RichCommand

from apps.anomalist.anomalist_api import ANOMALY_TYPE, anomaly_detection
from etl.paths import CACHE_DIR

log = structlog.get_logger()

memory = Memory(CACHE_DIR, verbose=0)


@click.command(name="anomalist", cls=RichCommand, help=anomaly_detection.__doc__)
@click.option(
    "--anomaly-types",
    type=click.Choice(list(get_args(ANOMALY_TYPE))),
    multiple=True,
    default=None,
    help="Type (or types) of anomaly detection algorithm to use.",
)
@click.option(
    "--dataset-ids",
    type=int,
    multiple=True,
    default=None,
    help="Generate anomalies for the variables of a specific dataset ID (or multiple dataset IDs).",
)
@click.option(
    "--variable-mapping",
    type=str,
    default="",
    help="Optional JSON dictionary mapping variable IDs from a previous to a new version (where at least some of the new variable IDs must belong to the datasets whose IDs were given).",
)
@click.option(
    "--variable-ids",
    type=int,
    multiple=True,
    default=None,
    help="Generate anomalies for a list of variable IDs (in addition to the ones from dataset ID, if any dataset was given).",
)
@click.option(
    "--dry-run/--no-dry-run",
    default=False,
    type=bool,
    help="Do not write to target database.",
)
@click.option(
    "--reset-db/--no-reset-db",
    default=False,
    type=bool,
    help="Drop anomalies table and recreate it. This is useful for development when the schema changes.",
)
def cli(
    anomaly_types: Optional[Tuple[str, ...]],
    dataset_ids: Optional[list[int]],
    variable_mapping: str,
    variable_ids: Optional[list[int]],
    dry_run: bool,
    reset_db: bool,
) -> None:
    """TBD

    TBD

    **Example 1:** Create random anomaly for a dataset

    ```
    $ etl anomalist --anomaly-type sample --dataset-ids 6369
    ```

    **Example 2:** Create GP anomalies

    ```
    $ etl anomalist --anomaly-type gp --dataset-ids 6369
    ```

    **Example 3:** Create anomalies by comparing dataset to its previous version

    ```
    $ etl anomalist --anomaly-type gp --dataset-ids 6589
    ```
    """
    # Convert variable mapping from JSON to dictionary.
    if variable_mapping:
        try:
            variable_mapping_dict = {
                int(variable_old): int(variable_new)
                for variable_old, variable_new in json.loads(variable_mapping).items()
            }
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON format for variable_mapping.")
    else:
        variable_mapping_dict = {}

    anomaly_detection(
        anomaly_types=anomaly_types,
        dataset_ids=dataset_ids,
        variable_mapping=variable_mapping_dict,
        variable_ids=variable_ids,
        dry_run=dry_run,
        reset_db=reset_db,
    )


if __name__ == "__main__":
    cli()
