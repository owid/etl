from typing import Literal, Optional, get_args

import click
import pandas as pd
import structlog
from joblib import Memory
from rich_click.rich_command import RichCommand
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from apps.backport.datasync.data_metadata import variable_data_df_from_s3
from etl import grapher_model as gm
from etl.db import get_engine, read_sql
from etl.paths import CACHE_DIR

from .gp_anomaly import GPAnomalyDetector, SampleAnomalyDetector

log = structlog.get_logger()

memory = Memory(CACHE_DIR, verbose=0)

ANOMALY_TYPE = Literal["sample", "gp"]


@click.command(name="anomalist", cls=RichCommand, help=__doc__)
@click.option(
    "--type",
    type=click.Choice(get_args(ANOMALY_TYPE)),
    help="Type of anomaly detection algorithm to use.",
)
@click.option(
    "--dataset-id",
    type=int,
    help="Generate anomalies for a specific dataset ID.",
)
@click.option(
    "--previous-dataset-id",
    type=int,
    help="Dataset ID of the previous version.",
)
@click.option(
    "--variable-id",
    type=int,
    multiple=True,
    help="Generate anomalies for a list of variable IDs.",
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
    type: Optional[ANOMALY_TYPE],
    dataset_id: Optional[int],
    previous_dataset_id: Optional[int],
    variable_id: Optional[int],
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

    assert type, "Anomaly type must be specified."

    # load metadata
    variables = _load_variables_meta(engine, dataset_id, variable_id)

    assert len(variables) < 10, "Too many indicators to process."

    log.info("Detecting anomalies")
    anomalies = []

    if type == "gp":
        detector = GPAnomalyDetector()
    elif type == "sample":
        detector = SampleAnomalyDetector()
    else:
        raise ValueError(f"Unsupported anomaly type: {type}")

    for variable in variables:
        assert variable.catalogPath

        # load dataframe
        log.info("Loading data from S3", variable_id=variable.id)
        df = load_data_for_variable(engine, variable)

        # detect anomalies
        log.info("Detecting anomalies", variable_id=variable.id)
        for df_score in detector.get_score_df(df, variable):
            anomaly = gm.Anomaly(
                datasetId=variable.datasetId,
                anomalyType=detector.anomaly_type,
                catalogPath=variable.catalogPath,
            )
            __import__("ipdb").set_trace()
            anomaly.dfScore = df_score
            anomalies.append(anomaly)

    if dry_run:
        for anomaly in anomalies:
            log.info(anomaly)
    else:
        with Session(engine) as session:
            log.info("Deleting existing anomalies")
            session.query(gm.Anomaly).filter(
                gm.Anomaly.anomalyType == detector.anomaly_type,
                gm.Anomaly.catalogPath.in_([v.catalogPath for v in variables]),
            ).delete(synchronize_session=False)
            session.commit()

            # Insert new anomalies
            log.info("Writing anomalies to database")
            session.add_all(anomalies)
            session.commit()


# @memory.cache
def load_data_for_variable(engine: Engine, variable: gm.Variable) -> pd.DataFrame:
    # TODO: cache this on disk & re-validate with etags
    df_long = variable_data_df_from_s3(engine, [variable.id])

    # pivot dataframe
    df = df_long.pivot(index=["variableId", "year"], columns="entityName", values="value")

    # extract data for a single variable
    df = df_long[df_long.variableId == variable.id].pivot(index="year", columns="entityName", values="value")

    # try converting to numeric
    df = df.astype(float)

    # remove countries with all nulls or all zeros or constant values
    df = df.loc[:, df.fillna(0).std(axis=0) != 0]

    return df


@memory.cache
def _load_variables_meta(engine: Engine, dataset_id: Optional[int], variable_ids: Optional[int]) -> list[gm.Variable]:
    if dataset_id and variable_ids:
        raise ValueError("Cannot specify both dataset ID and variable IDs.")

    if variable_ids:
        q = """
        select id from variables
        where id in %(variable_ids)s
        """
    elif dataset_id:
        q = """
        select id from variables
        where datasetId = %(dataset_id)s
        """
    # load all variables from a random dataset
    else:
        q = """
        with t as (
            select id from datasets order by rand() limit 1
        )
        select id from variables
        where datasetId in (select id from t)
        """

    df = read_sql(q, engine, params={"variable_ids": variable_ids, "dataset_id": dataset_id})

    # select all variables using SQLAlchemy
    with Session(engine) as session:
        return gm.Variable.load_variables(session, list(df["id"]))


if __name__ == "__main__":
    cli()
