import json
from typing import Optional, Tuple, get_args

import click
import structlog
from joblib import Memory
from rich_click.rich_command import RichCommand
from sqlalchemy.engine import Engine

from apps.anomalist.anomalist_api import ANOMALY_TYPE, anomaly_detection
from etl.db import get_engine, production_or_master_engine, read_sql
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
    "--force",
    "-f",
    is_flag=True,
    help="TBD",
)
@click.option(
    "--reset-db/--no-reset-db",
    default=False,
    type=bool,
    help="Drop anomalies table and recreate it. This is useful for development when the schema changes.",
)
@click.option(
    "--sample-n",
    type=int,
    default=500,
    help="Sample at most N variables from a dataset",
)
def cli(
    anomaly_types: Optional[Tuple[str, ...]],
    dataset_ids: Optional[list[int]],
    variable_mapping: str,
    variable_ids: Optional[list[int]],
    dry_run: bool,
    force: bool,
    reset_db: bool,
    sample_n: Optional[int],
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

    **Example 4:** Create anomalies for new datasets

    ```
    $ etl anomalist --anomaly-type gp
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

    # If no variable IDs are given, load all variables from the given datasets.
    if not variable_ids:
        # Use new datasets
        if not dataset_ids:
            dataset_ids = load_datasets_new_ids(get_engine())

            # Still no datasets, exit
            if not dataset_ids:
                log.info("No new datasets found.")
                return

        # Load all variables from given datasets
        assert not variable_ids, "Cannot specify both dataset IDs and variable IDs."
        q = """
        select id from variables
        where datasetId in %(dataset_ids)s
        """
        variable_ids = list(read_sql(q, get_engine(), params={"dataset_ids": dataset_ids})["id"])

    else:
        assert not dataset_ids, "Cannot specify both dataset IDs and variable IDs."

    anomaly_detection(
        anomaly_types=anomaly_types,
        variable_mapping=variable_mapping_dict,
        variable_ids=list(variable_ids) if variable_ids else None,
        dry_run=dry_run,
        force=force,
        reset_db=reset_db,
        sample_n=sample_n,
    )


def load_datasets_new_ids(source_engine: Engine) -> list[int]:
    # Compare against production or staging-site-master
    target_engine = production_or_master_engine()

    # Get new datasets
    q = """SELECT
        id,
        catalogPath
    FROM datasets
    """
    source_datasets = read_sql(q, source_engine)
    target_datasets = read_sql(q, target_engine)

    return list(
        source_datasets[
            source_datasets.catalogPath.isin(set(source_datasets["catalogPath"]) - set(target_datasets["catalogPath"]))
        ]["id"]
    )


if __name__ == "__main__":
    cli()
