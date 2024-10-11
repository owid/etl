import json
from typing import Dict, Literal, Optional, Tuple, get_args

import click
import pandas as pd
import structlog
from joblib import Memory
from rich_click.rich_command import RichCommand
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from apps.anomalist.bard_anomaly import NaNAnomalyDetector
from apps.anomalist.gp_anomaly import GPAnomalyDetector, SampleAnomalyDetector
from etl import grapher_model as gm
from etl.db import get_engine, read_sql
from etl.grapher_io import variable_data_df_from_s3
from etl.paths import CACHE_DIR

log = structlog.get_logger()

memory = Memory(CACHE_DIR, verbose=0)

ANOMALY_TYPE = Literal["sample", "gp", "nan"]


@click.command(name="anomalist", cls=RichCommand, help=__doc__)
@click.option(
    "--anomaly-types",
    type=click.Choice(get_args(ANOMALY_TYPE)),
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
    default=None,
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
    variable_mapping: Optional[str],  # type: ignore
    variable_ids: Optional[list[int]],
    dry_run: bool,
    reset_db: bool,
) -> None:
    """TBD

    TBD

    **Example 1:** Create random anomaly for a dataset

    ```
    $ etl anomalist --type sample --dataset-id 6369
    ```

    **Example 2:** Create GP anomalies

    ```
    $ etl anomalist --type gp --dataset-id 6369
    ```

    **Example 3:** Create anomalies by comparing dataset to its previous version

    ```
    $ etl anomalist --type gp --previous-dataset-id 6322 --dataset-id 6589
    ```
    """
    engine = get_engine()

    if reset_db:
        # Drop the 'anomalies' table if it exists
        gm.Anomaly.__table__.drop(engine, checkfirst=True)  # type: ignore

        # Create the 'anomalies' table
        gm.Anomaly.__table__.create(engine)  # type: ignore
        return

    # If no anomaly types are provided, default to all available types
    if not anomaly_types:
        anomaly_types = get_args(ANOMALY_TYPE)

    # Parse the variable_mapping if any provided.
    if variable_mapping:
        try:
            variable_mapping: Dict[int, int] = json.loads(variable_mapping)
            if not isinstance(variable_mapping, dict):
                raise ValueError("variable_mapping must be a dictionary.")
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON format for variable_mapping.")
    else:
        variable_mapping = dict()

    # Load metadata for all variables in dataset_ids (if any given) and variable_ids, and new variables in variable_mapping.
    variable_ids_all = (
        list(variable_mapping.values()) if variable_mapping else [] + list(variable_ids) if variable_ids else []
    )
    if dataset_ids is None:
        dataset_ids = []
    variables = _load_variables_meta(engine, dataset_ids, variable_ids_all)

    # Create a dictionary of all variable_ids for each dataset_id.
    dataset_variable_ids = {}
    for variable in variables:
        if variable.datasetId not in dataset_variable_ids:
            dataset_variable_ids[variable.datasetId] = []
        dataset_variable_ids[variable.datasetId].append(variable)

    log.info("Detecting anomalies")
    anomalies = []

    for dataset_id, variables_in_dataset in dataset_variable_ids.items():
        for anomaly_type in anomaly_types:
            if anomaly_type == "gp":
                detector = GPAnomalyDetector()
            elif anomaly_type == "sample":
                detector = SampleAnomalyDetector()
            elif anomaly_type == "nan":
                detector = NaNAnomalyDetector()
            else:
                raise ValueError(f"Unsupported anomaly type: {anomaly_type}")

            # dataframe with (entityName, year) as index and variableId as columns
            log.info("Loading data from S3")
            df = load_data_for_variables(engine, variables_in_dataset)

            # TODO: If any of the variables are in variable_mapping, load df_old as well.

            # detect anomalies
            log.info("Detecting anomalies")
            # the output has the same shape as the input dataframe, but we should make
            # it possible to return anomalies in a long format (for detectors that return
            # just a few anomalies)
            df_score = detector.get_score_df(df, variables_in_dataset)

            # TODO: validate format of the output dataframe

            anomaly = gm.Anomaly(
                datasetId=dataset_id,
                anomalyType=detector.anomaly_type,
            )
            anomaly.dfScore = df_score

            if dry_run:
                log.info(anomaly)
            else:
                with Session(engine) as session:
                    # TODO: Is this right? I suppose it should also delete if already existing.
                    log.info("Deleting existing anomalies")
                    session.query(gm.Anomaly).filter(
                        gm.Anomaly.datasetId == dataset_id,
                        gm.Anomaly.anomalyType.in_([a.anomalyType for a in anomalies]),
                    ).delete(synchronize_session=False)
                    session.commit()

                    # Insert new anomalies
                    log.info("Writing anomalies to database")
                    session.add_all(anomalies)
                    session.commit()


# @memory.cache
def load_data_for_variables(engine: Engine, variables: list[gm.Variable]) -> pd.DataFrame:
    # TODO: cache this on disk & re-validate with etags
    df_long = variable_data_df_from_s3(engine, [v.id for v in variables])

    # pivot dataframe
    df = df_long.pivot(index=["entityName", "year"], columns="variableId", values="value")

    # reorder in the same order as variables
    df = df[[v.id for v in variables]]

    # try converting to numeric
    df = df.astype(float)

    # TODO:
    # remove countries with all nulls or all zeros or constant values
    # df = df.loc[:, df.fillna(0).std(axis=0) != 0]

    return df


@memory.cache
def _load_variables_meta(
    engine: Engine, dataset_ids: Optional[list[int]], variable_ids: Optional[list[int]]
) -> list[gm.Variable]:
    if dataset_ids:
        q = """
        select id from variables
        where datasetId in %(dataset_ids)s
        """
        df_from_dataset_ids = read_sql(q, engine, params={"dataset_ids": dataset_ids})
    else:
        df_from_dataset_ids = pd.DataFrame()

    if variable_ids:
        q = """
        select id from variables
        where id in %(variable_ids)s
        """
        df_from_variable_ids = read_sql(q, engine, params={"variable_ids": variable_ids})
    else:
        df_from_variable_ids = pd.DataFrame()

    # Combine both dataframes to get all possible variables required.
    df = pd.concat([df_from_dataset_ids, df_from_variable_ids]).drop_duplicates()

    # load all variables from a random dataset
    if df.empty:
        q = """
        with t as (
            select id from datasets order by rand() limit 1
        )
        select id from variables
        where datasetId in (select id from t)
        """
        df = read_sql(q, engine)

    # select all variables using SQLAlchemy
    with Session(engine) as session:
        return gm.Variable.load_variables(session, list(df["id"]))


if __name__ == "__main__":
    cli()
