import tempfile
from pathlib import Path
from typing import Literal, Optional, get_args

import click
import pandas as pd
import structlog
from joblib import Memory
from rich_click.rich_command import RichCommand
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from apps.wizard.utils.paths import WIZARD_ANOMALIES_RELATIVE
from etl import grapher_model as gm
from etl.config import OWID_ENV
from etl.db import get_engine, read_sql
from etl.files import create_folder, upload_file_to_server
from etl.grapher_io import variable_data_df_from_s3
from etl.paths import CACHE_DIR

from .bard_anomaly import NaNAnomalyDetector
from .gp_anomaly import GPAnomalyDetector, SampleAnomalyDetector

log = structlog.get_logger()

memory = Memory(CACHE_DIR, verbose=0)

ANOMALY_TYPE = Literal["sample", "gp", "nan"]


@click.command(name="anomalist", cls=RichCommand, help=__doc__)
@click.option(
    "--type",
    type=click.Choice(get_args(ANOMALY_TYPE)),
    multiple=True,
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
    variable_id: Optional[list[int]],
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
        gm.Anomaly.create_table(engine)
        return

    assert type, "Anomaly type must be specified."

    # load metadata
    variables = _load_variables_meta(engine, dataset_id, variable_id)

    # set dataset_id if we're using variables
    if not dataset_id:
        assert set(v.datasetId for v in variables) == {variables[0].datasetId}
        dataset_id = variables[0].datasetId

    log.info("Detecting anomalies")
    anomalies = []

    for typ in type:
        if typ == "gp":
            detector = GPAnomalyDetector()
        elif typ == "sample":
            detector = SampleAnomalyDetector()
        elif typ == "nan":
            detector = NaNAnomalyDetector()
        else:
            raise ValueError(f"Unsupported anomaly type: {typ}")

        # dataframe with (entityName, year) as index and variableId as columns
        log.info("Loading data from S3")
        df = load_data_for_variables(engine, variables)

        # detect anomalies
        log.info("Detecting anomalies")
        # the output has the same shape as the input dataframe, but we should make
        # it possible to return anomalies in a long format (for detectors that return
        # just a few anomalies)
        df_score = detector.get_score_df(df, variables)

        # validate format of the output dataframe
        # TODO

        anomaly = gm.Anomaly(
            datasetId=dataset_id,
            anomalyType=detector.anomaly_type,
        )
        anomaly.dfScore = None

        # Export anomaly file
        anomaly.path_file = export_anomalies_file(df_score, dataset_id, detector.anomaly_type)

        anomalies.append(anomaly)

    if dry_run:
        for anomaly in anomalies:
            log.info(anomaly)
    else:
        with Session(engine) as session:
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


def export_anomalies_file(df: pd.DataFrame, dataset_id: int, anomaly_type: str) -> str:
    """Export anomaly df to local file (and upload to staging server if applicable)."""
    filename = f"{dataset_id}_{anomaly_type}.feather"
    path = Path(f".anomalies/{filename}")
    path_str = str(path)
    if OWID_ENV.env_local == "staging":
        create_folder(path.parent)
        df.to_feather(path_str)
    elif OWID_ENV.env_local == "dev":
        # tmp_filename = Path("tmp.feather")
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_file_path = Path(tmp_dir) / filename
            df.to_feather(tmp_file_path)
            upload_file_to_server(tmp_file_path, f"owid@{OWID_ENV.name}:/home/owid/etl/{WIZARD_ANOMALIES_RELATIVE}")
    else:
        raise ValueError(
            f"Unsupported environment: {OWID_ENV.env_local}. Did you try production? That's not supported!"
        )
    return path_str


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
    engine: Engine, dataset_id: Optional[int], variable_ids: Optional[list[int]]
) -> list[gm.Variable]:
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
