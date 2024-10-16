import tempfile
from pathlib import Path
from typing import Dict, List, Literal, Optional, Tuple, get_args

import numpy as np
import pandas as pd
import structlog
from owid.catalog import find
from owid.datautils.dataframes import map_series
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from apps.anomalist.detectors import (
    AnomalyTimeChange,
    AnomalyUpgradeChange,
    AnomalyUpgradeMissing,
    get_long_format_score_df,
)
from apps.anomalist.gp_detector import AnomalyGaussianProcessOutlier
from apps.wizard.utils.paths import WIZARD_ANOMALIES_RELATIVE
from etl import grapher_model as gm
from etl.config import OWID_ENV
from etl.db import get_engine, read_sql
from etl.files import create_folder, upload_file_to_server
from etl.grapher_io import variable_data_df_from_s3

log = structlog.get_logger()

# Name of index columns for dataframe.
INDEX_COLUMNS = ["entity_name", "year"]

# TODO: this is repeated in detector classes, is there a way to DRY this?
ANOMALY_TYPE = Literal["time_change", "upgrade_change", "upgrade_missing", "gp_outlier"]

# Define mapping of available anomaly types to anomaly detectors.
ANOMALY_DETECTORS = {
    detector.anomaly_type: detector
    for detector in [
        AnomalyTimeChange,
        AnomalyUpgradeChange,
        AnomalyUpgradeMissing,
        AnomalyGaussianProcessOutlier,
    ]
}


########################################################################################################################

# AGGREGATE ANOMALIES:


def load_latest_population():
    # NOTE: The "channels" parameter of the find function is not working well.
    candidates = find("population", channels=("grapher",), dataset="population", namespace="demography").sort_values(
        "version", ascending=False
    )
    population = (
        candidates[(candidates["table"] == "population") & (candidates["channel"] == "grapher")]
        .iloc[0]
        .load()
        .reset_index()[["country", "year", "population"]]
    ).rename(columns={"country": "entity_name"}, errors="raise")

    return population


def get_variables_views_in_charts(
    variable_ids: List[int],
) -> pd.DataFrame:
    # Assumed base url for all charts.
    base_url = "https://ourworldindata.org/grapher/"

    # SQL query to join variables, charts, and analytics pageviews data
    query = f"""\
    SELECT
        v.id AS variable_id,
        c.id AS chart_id,
        cc.slug AS chart_slug,
        ap.views_7d,
        ap.views_14d,
        ap.views_365d
    FROM
        charts c
    JOIN
        chart_dimensions cd ON c.id = cd.chartId
    JOIN
        variables v ON cd.variableId = v.id
    JOIN
        chart_configs cc ON c.configId = cc.id
    LEFT JOIN
        analytics_pageviews ap ON ap.url = CONCAT('{base_url}', cc.slug)
    WHERE
        v.id IN ({', '.join([str(v_id) for v_id in variable_ids])})
    ORDER BY
        v.id ASC;
    """
    df = read_sql(query)
    # Handle potential duplicated rows
    df = df.drop_duplicates().reset_index(drop=True)

    return df


def aggregate_anomalies(
    df_scores: pd.DataFrame,
    df_population: pd.DataFrame,
    df_views: pd.DataFrame,
    metadata: Dict[int, gm.Variable],
) -> pd.DataFrame:
    # Create a dataframe of zeros.
    df_aggregated = df_scores.copy()

    df_aggregated["variable"] = map_series(
        df_aggregated["variable_id"],
        {variable_id: metadata[variable_id]["shortName"] for variable_id in metadata},  # type: ignore
        warn_on_missing_mappings=True,
        warn_on_unused_mappings=False,
    )

    # Add population score to the aggregated scores dataframe.
    df_aggregated = df_aggregated.merge(
        get_population_score(df_aggregated, df_population), on=["entity_name", "year"], how="left"
    )

    # Add analytics score to the main scores dataframe.
    # TODO: Improve this, currently df_aggregated is not used.
    df_score_analytics = get_analytics_score(df_aggregated, df_views)
    df_aggregated = df_aggregated.merge(df_score_analytics, on=["variable_id"], how="left")

    # NOTE: Variables that have do not have charts will have an analytics score nan.
    #  Fill them with zeros.
    df_aggregated["analytics_score"] = df_score_analytics["analytics_score"].fillna(0)

    return df_aggregated


def get_population_score(df_aggregated: pd.DataFrame, df_population: pd.DataFrame) -> pd.DataFrame:
    # NOTE: This is a special type of score that is added afterwards to help rank anomalies.
    #  It would not make sense to calculate a population score at the beginning and select the largest anomalies based on it (which would trivially pick the most populated countries).

    # First, get the unique combinations of country-years in the scores dataframe, and add population to it.
    df_score_population = (
        df_aggregated[["entity_name", "year"]]  # type: ignore
        .drop_duplicates()
        .merge(df_population, on=["entity_name", "year"], how="left")
    )
    # To normalize the population score to the range 0, 1, divide by an absolute maximum population of 10 billion.
    # To have more convenient numbers, take the natural logarithm of the population.
    df_score_population["population_score"] = np.log(df_score_population["population"]) / np.log(10e9)
    # It's unclear what to do with entities that do not have a population (e.g. "Middle East").
    # For now, add a score of 0.5 to them.
    df_score_population["population_score"] = df_score_population["population_score"].fillna(0.5)

    df_score_population = df_score_population[["entity_name", "year", "population_score"]]

    return df_score_population


def get_analytics_score(df_aggregated: pd.DataFrame, df_views: pd.DataFrame) -> pd.DataFrame:
    # Focus on the following specific analytics column.
    analytics_column = "views_14d"

    # Get the sum of the number of views in charts for each variable id in the last 14 days.
    # So, if an indicator is used in multiple charts, their views are summed.
    # This rewards indicators that are used multiple times, and on popular charts.
    # NOTE: The analytics table often contains nans. Not sure why this happens, e.g. to coal-electricity-per-capita: https://admin.owid.io/admin/charts/4451/edit
    #  For now, for convenience, fill them with 1.1 views (to avoid zeros when calculating the log).
    df_score_analytics = (
        df_views.fillna(1.1)
        .groupby("variable_id")
        .agg({analytics_column: "sum"})
        .reset_index()
        .rename(columns={analytics_column: "views"})
    )
    # To normalize the analytics score to the range 0, 1, divide by an absolute maximum number of views.
    absolute_maximum_views = 1e6
    error = f"Expected a maximum number of views below {absolute_maximum_views}. Change this limit."
    assert df_views[analytics_column].max() < absolute_maximum_views, error
    # To have more convenient numbers, take the natural logarithm of the views.
    df_score_analytics["analytics_score"] = np.log(df_score_analytics["views"]) / np.log(absolute_maximum_views)

    return df_score_analytics


########################################################################################################################


def anomaly_detection(
    anomaly_types: Optional[Tuple[str, ...]] = None,
    variable_mapping: Optional[dict[int, int]] = None,
    variable_ids: Optional[list[int]] = None,
    dry_run: bool = False,
    reset_db: bool = False,
) -> None:
    """Detect anomalies."""
    engine = get_engine()

    # Ensure the 'anomalies' table exists. Optionally reset it if reset_db is True.
    gm.Anomaly.create_table(engine, reset=reset_db)

    # If no anomaly types are provided, default to all available types
    if not anomaly_types:
        anomaly_types = get_args(ANOMALY_TYPE)

    # Parse the variable_mapping if any provided.
    if not variable_mapping:
        variable_mapping = dict()

    if variable_ids is None:
        variable_ids = []

    # Load metadata for:
    # * All variables in dataset_ids (if any dataset_id is given).
    # * All variables in variable_ids.
    # * All variables in variable_mapping (both old and new).
    variable_ids_mapping = (
        (set(variable_mapping.keys()) | set(variable_mapping.values())) if variable_mapping else set()
    )
    variable_ids_all = list(variable_ids_mapping | set(variable_ids or []))
    # Dictionary variable_id: Variable object, for all variables (old and new).
    variables = {
        variable.id: variable for variable in _load_variables_meta(engine=engine, variable_ids=variable_ids_all)
    }

    # Create a dictionary of all variable_ids for each dataset_id (only for new variables).
    dataset_variable_ids = {}
    # TODO: Ensure variable_ids always corresponds to new variables.
    #  Note that currently, if dataset_id is passed and variable_ids is not, this will not load anything.
    for variable_id in variable_ids:
        variable = variables[variable_id]
        if variable.datasetId not in dataset_variable_ids:
            dataset_variable_ids[variable.datasetId] = []
        dataset_variable_ids[variable.datasetId].append(variable)

    for dataset_id, variables_in_dataset in dataset_variable_ids.items():
        log.info("Loading data from S3")
        variables_old = [
            variables[variable_id_old]
            for variable_id_old in variable_mapping.keys()
            if variable_mapping[variable_id_old] in [variable.id for variable in variables_in_dataset]
        ]
        variables_old_and_new = variables_in_dataset + variables_old
        # TODO: It would be more convenient if df had a dummy index, instead of resetting here.
        df = (
            load_data_for_variables(engine=engine, variables=variables_old_and_new)
            .reset_index()
            .rename(columns={"entityName": "entity_name"}, errors="raise")
            .astype({"entity_name": str})
        )

        for anomaly_type in anomaly_types:
            # Instantiate the anomaly detector.
            if anomaly_type not in ANOMALY_DETECTORS:
                raise ValueError(f"Unsupported anomaly type: {anomaly_type}")

            # Instantiate the anomaly detector.
            detector = ANOMALY_DETECTORS[anomaly_type]()

            # detect anomalies
            log.info(f"Detecting anomaly type {anomaly_type} for dataset {dataset_id}")
            # the output has the same shape as the input dataframe, but we should make
            # it possible to return anomalies in a long format (for detectors that return
            # just a few anomalies)
            df_score = detector.get_score_df(df=df, variable_ids=variable_ids, variable_mapping=variable_mapping)

            # Create a long format score dataframe.
            df_score_long = get_long_format_score_df(df_score)

            # TODO: validate format of the output dataframe
            anomaly = gm.Anomaly(
                datasetId=dataset_id,
                anomalyType=anomaly_type,
            )
            anomaly.dfScore = df_score_long

            # TODO: Use this as an alternative to storing binary files in the DB
            # anomaly = gm.Anomaly(
            #     datasetId=dataset_id,
            #     anomalyType=detector.anomaly_type,
            # )
            # anomaly.dfScore = None

            # # Export anomaly file
            # anomaly.path_file = export_anomalies_file(df_score, dataset_id, detector.anomaly_type)

            if not dry_run:
                with Session(engine) as session:
                    # log.info("Deleting existing anomalies")
                    session.query(gm.Anomaly).filter(
                        gm.Anomaly.datasetId == dataset_id,
                        gm.Anomaly.anomalyType == anomaly_type,
                    ).delete(synchronize_session=False)
                    session.commit()

                    # Don't save anomalies if there are none
                    if df_score_long.empty:
                        log.info(f"No anomalies found for anomaly type {anomaly_type} in dataset {dataset_id}")
                    else:
                        # Insert new anomalies
                        log.info("Writing anomaly to database")
                        session.add(anomaly)
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
    df_long = variable_data_df_from_s3(engine, [v.id for v in variables], workers=None)

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


# @memory.cache
def _load_variables_meta(engine: Engine, variable_ids: list[int]) -> list[gm.Variable]:
    q = """
    select id from variables
    where id in %(variable_ids)s
    """
    df = read_sql(q, engine, params={"variable_ids": variable_ids})

    # select all variables using SQLAlchemy
    with Session(engine) as session:
        return gm.Variable.load_variables(session, list(df["id"]))
