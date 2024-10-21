import tempfile
import time
from pathlib import Path
from typing import List, Literal, Optional, Tuple, cast, get_args

import numpy as np
import pandas as pd
import structlog
from owid.catalog import find
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Session

from apps.anomalist.detectors import (
    AnomalyDetector,
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


def load_detector(anomaly_type: ANOMALY_TYPE) -> AnomalyDetector:
    """Load detector."""
    return ANOMALY_DETECTORS[anomaly_type]


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

    if len(df) == 0:
        df = pd.DataFrame(columns=["variable_id", "chart_id", "chart_slug", "views_7d", "views_14d", "views_365d"])

    return df


def add_population_score(df_reduced: pd.DataFrame) -> pd.DataFrame:
    # To normalize the analytics score to the range 0, 1, divide by an absolute maximum number of people.
    # NOTE: This should a safe assumption before ~2060.
    absolute_maximum_population = 1e10
    # Value to use to fill missing values in the population score (e.g. for regions like "Middle East" that are not included in our population dataset).
    fillna_value = 0.5

    # Load the latest population data.
    df_population = load_latest_population()
    error = f"Expected a maximum population below {absolute_maximum_population}."
    assert df_population[df_population["year"] < 2040]["population"].max() < absolute_maximum_population, error

    # First, get the unique combinations of country-years in the scores dataframe, and add population to it.
    df_score_population = (
        df_reduced[["entity_name", "year"]]  # type: ignore
        .drop_duplicates()
        .merge(df_population, on=["entity_name", "year"], how="left")
    )

    # To normalize the population score to the range 0, 1, divide by an absolute maximum population.
    # To have more convenient numbers, take the natural logarithm of the population.
    df_score_population["score_population"] = np.log(df_score_population["population"]) / np.log(
        absolute_maximum_population
    )

    # Add the population score to the scores dataframe.
    df_reduced = df_reduced.merge(df_score_population, on=["entity_name", "year"], how="left").drop(
        columns="population", errors="raise"
    )

    # Variables that do not have population data will have a population score nan. Fill them with a low value.
    df_reduced["score_population"] = df_reduced["score_population"].fillna(fillna_value)

    return df_reduced


def add_analytics_score(df_reduced: pd.DataFrame) -> pd.DataFrame:
    # Focus on the following specific analytics column.
    analytics_column = "views_14d"
    # To normalize the analytics score to the range 0, 1, divide by an absolute maximum number of views.
    absolute_maximum_views = 1e6
    # Value to use to fill missing values in the analytics score (e.g. for variables that are not used in charts).
    fillna_value = 0.1

    # Get number of views in charts for each variable id.
    df_views = get_variables_views_in_charts(list(df_reduced["variable_id"].unique()))
    # Sanity check.
    if not df_views.empty:
        error = f"Expected a maximum number of views below {absolute_maximum_views}. Change this limit."
        assert df_views[analytics_column].max() < absolute_maximum_views, error

    # Get the sum of the number of views in charts for each variable id in the last 14 days.
    # So, if an indicator is used in multiple charts, their views are summed.
    # This rewards indicators that are used multiple times, and on popular charts.
    # NOTE: The analytics table often contains nans. For now, for convenience, fill them with 1.1 views (to avoid zeros when calculating the log).
    df_score_analytics = (
        df_views.groupby("variable_id")
        .agg({analytics_column: "sum"})
        .reset_index()
        .rename(columns={analytics_column: "views"})
    )
    # To have more convenient numbers, take the natural logarithm of the views.
    df_score_analytics["score_analytics"] = np.log(df_score_analytics["views"]) / np.log(absolute_maximum_views)

    # Add the analytics score to the scores dataframe.
    df_reduced = df_reduced.merge(df_score_analytics, on=["variable_id"], how="left")

    # Variables that do not have charts will have an analytics score nan.
    # Fill them with a low value (e.g. 0.1) to avoid zeros when calculating the final score.
    df_reduced["score_analytics"] = df_reduced["score_analytics"].fillna(fillna_value)

    return df_reduced


def add_weighted_score(df: pd.DataFrame) -> pd.DataFrame:
    """Add a weighted combined score."""
    w_score = 1
    w_pop = 1
    w_views = 1
    df["score_weighted"] = (
        w_score * df["score"] + w_pop * df["score_population"] + w_views * df["score_analytics"]
    ) / (w_score + w_pop + w_views)

    return df


def add_auxiliary_scores(df: pd.DataFrame) -> pd.DataFrame:
    # Add a population score.
    df = add_population_score(df_reduced=df)

    # Add an analytics score.
    df = add_analytics_score(df_reduced=df)

    # Rename columns for convenience.
    df = df.rename(columns={"variable_id": "indicator_id", "anomaly_score": "score"}, errors="raise")

    # Create a weighted combined score.
    df = add_weighted_score(df)

    return df


def anomaly_detection(
    anomaly_types: Optional[Tuple[str, ...]] = None,
    variable_mapping: Optional[dict[int, int]] = None,
    variable_ids: Optional[list[int]] = None,
    dry_run: bool = False,
    force: bool = False,
    reset_db: bool = False,
) -> None:
    """Detect anomalies."""
    engine = get_engine()

    # Ensure the 'anomalies' table exists. Optionally reset it if reset_db is True.
    gm.Anomaly.create_table(engine, if_exists="replace" if reset_db else "skip")

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
        # Get dataset's checksum
        with Session(engine) as session:
            dataset = gm.Dataset.load_dataset(session, dataset_id)

        log.info("loading_data_from_s3.start")
        variables_old = [
            variables[variable_id_old]
            for variable_id_old in variable_mapping.keys()
            if variable_mapping[variable_id_old] in [variable.id for variable in variables_in_dataset]
        ]
        variables_old_and_new = variables_in_dataset + variables_old
        t = time.time()
        df = load_data_for_variables(engine=engine, variables=variables_old_and_new)
        log.info("loading_data_from_s3.end", t=time.time() - t)

        for anomaly_type in anomaly_types:
            # Instantiate the anomaly detector.
            if anomaly_type not in ANOMALY_DETECTORS:
                raise ValueError(f"Unsupported anomaly type: {anomaly_type}")

            if not force:
                if not needs_update(engine, dataset, anomaly_type):
                    log.info(f"Anomaly type {anomaly_type} for dataset {dataset_id} already exists in the database.")
                    continue

            log.info(f"Detecting anomaly type {anomaly_type} for dataset {dataset_id}")

            # Instantiate the anomaly detector.
            detector = ANOMALY_DETECTORS[anomaly_type]()

            # Select the variable ids that are included in the current dataset.
            variable_ids_for_current_dataset = [variable.id for variable in variables_in_dataset]
            # Select the subset of the mapping that is relevant for the current dataset.
            variable_mapping_for_current_dataset = {
                variable_old: variable_new
                for variable_old, variable_new in variable_mapping.items()
                if variable_new in variable_ids_for_current_dataset
            }

            # Get the anomaly score dataframe for the current dataset and anomaly type.
            df_score = detector.get_score_df(
                df=df,
                variable_ids=variable_ids_for_current_dataset,
                variable_mapping=variable_mapping_for_current_dataset,
            )

            if df_score.empty:
                log.info("No anomalies detected.`")
                continue

            # Create a long format score dataframe.
            df_score_long = get_long_format_score_df(df_score)

            # TODO: validate format of the output dataframe
            anomaly = gm.Anomaly(
                datasetId=dataset_id,
                datasetSourceChecksum=dataset.sourceChecksum,
                anomalyType=anomaly_type,
            )
            # We could store the full dataframe in the database, but it ends up making the load quite slow.
            # Since we are not using it for now, we will store only the reduced dataframe.
            # anomaly.dfScore = df_score_long

            # Reduce dataframe
            df_score_long_reduced = (
                df_score_long.sort_values("anomaly_score", ascending=False)
                .drop_duplicates(subset=["entity_name", "variable_id"], keep="first")
                .reset_index(drop=True)
            )
            anomaly.dfReduced = df_score_long_reduced

            ##################################################################
            # TODO: Use this as an alternative to storing binary files in the DB
            # anomaly = gm.Anomaly(
            #     datasetId=dataset_id,
            #     anomalyType=detector.anomaly_type,
            # )
            # anomaly.dfScore = None

            # # Export anomaly file
            # anomaly.path_file = export_anomalies_file(df_score, dataset_id, detector.anomaly_type)
            ##################################################################

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


def needs_update(engine: Engine, dataset: gm.Dataset, anomaly_type: str) -> bool:
    """If there's an anomaly with the dataset checksum in DB, it doesn't need
    to be updated."""
    with Session(engine) as session:
        try:
            anomaly = gm.Anomaly.load(
                session,
                dataset_id=dataset.id,
                anomaly_type=anomaly_type,
            )
        except NoResultFound:
            return True

        return anomaly.datasetSourceChecksum != dataset.sourceChecksum


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

    df_long = df_long.rename(columns={"variableId": "variable_id", "entityName": "entity_name"})

    # pivot dataframe
    df = df_long.pivot(index=["entity_name", "year"], columns="variable_id", values="value")

    # reorder in the same order as variables
    df = df[[v.id for v in variables]]

    # set non-numeric values to NaN
    df = df.apply(pd.to_numeric, errors="coerce")

    # remove variables with all nulls or all zeros or constant values
    df = df.loc[:, df.fillna(0).std(axis=0) != 0]

    df = df.reset_index().astype({"entity_name": str})

    return df  # type: ignore


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


def combine_and_reduce_scores_df(anomalies: List[gm.Anomaly]) -> pd.DataFrame:
    """Get the combined dataframe with scores for all anomalies, and reduce it to include only the largest anomaly for each contry-indicator."""
    # Combine the reduced dataframes for all anomalies into a single dataframe.
    dfs = []
    for anomaly in anomalies:
        df = anomaly.dfReduced
        if df is None:
            log.warning(f"Anomaly {anomaly} has no reduced dataframe.")
            continue
        df["type"] = anomaly.anomalyType
        dfs.append(df)

    df_reduced = cast(pd.DataFrame, pd.concat(dfs, ignore_index=True))
    # Dtypes
    # df = df.astype({"year": int})

    return df_reduced
