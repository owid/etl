import tempfile
from pathlib import Path
from typing import Dict, List, Literal, Optional, Tuple, get_args

import numpy as np
import pandas as pd
import plotly.express as px
import structlog
from owid.catalog import find
from owid.datautils.dataframes import map_series
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

import etl.grapher_io as io
from apps.wizard.utils.paths import WIZARD_ANOMALIES_RELATIVE
from etl import grapher_model as gm
from etl.config import OWID_ENV
from etl.data_helpers.misc import bard
from etl.db import get_engine, read_sql
from etl.files import create_folder, upload_file_to_server
from etl.grapher_io import variable_data_df_from_s3

log = structlog.get_logger()

# Name of index columns for dataframe.
INDEX_COLUMNS = ["entity_id", "year"]

# Define anomaly types.
ANOMALY_TYPE = Literal["version_change", "time_change", "lost"]


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
    )

    return population


def estimate_bard_epsilon(series: pd.Series) -> float:
    # Make all values positive, and ignore zeros.
    positive_values = abs(series.dropna())
    # Ignore zeros, since they can lead to epsilon being zero, hence allowing division by zero in BARD.
    positive_values = positive_values.loc[positive_values > 0]
    # Estimate epsilon as the absolute range of values divided by 10.
    # eps = (positive_values.max() - positive_values.min()) / 10
    # Instead of just taking maximum and minimum, take 95th percentile and 5th percentile.
    eps = (positive_values.quantile(0.95) - positive_values.quantile(0.05)) / 10

    return eps


def get_long_format_score_df(df_score: pd.DataFrame) -> pd.DataFrame:
    # Create a reduced score dataframe.
    df_score_long = df_score.melt(
        id_vars=["entity_id", "year"], var_name="variable_id", value_name="anomaly_score"
    ).fillna(0)
    # For now, keep only the latest year affected for each country-indicator.
    df_score_long = (
        df_score_long.sort_values("anomaly_score", ascending=False)
        .drop_duplicates(subset=["variable_id", "entity_id"], keep="first")
        .reset_index(drop=True)
    )

    return df_score_long


class AnomalyDetector:
    anomaly_type: str

    def __init__(
        self,
        variable_ids: List[int],
        df: pd.DataFrame,
        metadata: Dict[int, gm.Variable],
        variable_mapping: Dict[int, int],
        entity_id_to_name: Dict[int, str],
    ) -> None:
        self.variable_ids = variable_ids
        self.variable_mapping = variable_mapping

        # Initialize data objects.
        self.df = df
        self.metadata = metadata
        self.entity_id_to_name = entity_id_to_name

        # Create a dataframe of zeros, that will be used for each data anomaly type.
        self.df_zeros = pd.DataFrame(np.zeros_like(self.df), columns=self.df.columns)[INDEX_COLUMNS + self.variable_ids]
        self.df_zeros[INDEX_COLUMNS] = self.df[INDEX_COLUMNS].copy()

    def get_score_df(self) -> pd.DataFrame:
        raise NotImplementedError()

    # Visually inspect the most significant anomalies on a certain scores dataframe.
    def inspect_anomalies(self, anomalies: Optional[pd.DataFrame] = None, n_anomalies: int = 10) -> None:
        # Select the most significant anomalies.
        anomalies = anomalies.sort_values("anomaly_score", ascending=False).head(n_anomalies)  # type: ignore
        # Reverse variable mapping.
        variable_id_new_to_old = {v: k for k, v in self.variable_mapping.items()}
        anomalies["variable_id_old"] = map_series(  # type: ignore
            anomalies["variable_id"],  # type: ignore
            variable_id_new_to_old,
            warn_on_missing_mappings=False,  # type: ignore
        )
        for _, row in anomalies.iterrows():  # type: ignore
            variable_id = row["variable_id"]
            variable_name = self.metadata[variable_id].shortName  # type: ignore
            country = row["country"]
            score_name = row["anomaly_type"]
            title = f'{country} ({row["year"]} - {score_name} {row["anomaly_score"]:.0%}) {variable_name}'
            new = self.df[self.df["entity_id"] == row["entity_id"]][["entity_id", "year", variable_id]]
            new["country"] = map_series(new["entity_id"], self.entity_id_to_name)
            new = new.drop(columns=["entity_id"]).rename(columns={row["variable_id"]: variable_name}, errors="raise")
            if score_name == "version_change":
                variable_id_old = row["variable_id_old"]
                old = self.df[self.df["entity_id"] == row["entity_id"]][["entity_id", "year", variable_id_old]]
                old["country"] = map_series(old["entity_id"], self.entity_id_to_name)
                old = old.drop(columns=["entity_id"]).rename(
                    columns={row["variable_id_old"]: variable_name}, errors="raise"
                )
                compare = pd.concat(
                    [old.assign(**{"source": "old"}), new.assign(**{"source": "new"})], ignore_index=True
                )
                px.line(
                    compare,
                    x="year",
                    y=variable_name,
                    color="source",
                    title=title,
                    markers=True,
                    color_discrete_map={"old": "rgba(256,0,0,0.5)", "new": "rgba(0,256,0,0.5)"},
                ).show()
            else:
                px.line(
                    new,
                    x="year",
                    y=variable_name,
                    title=title,
                    markers=True,
                    color_discrete_map={"new": "rgba(0,256,0,0.5)"},
                ).show()


class AnomalyLostData(AnomalyDetector):
    """New data misses entity-years that used to exist in old version."""

    anomaly_type = "lost"

    def get_score_df(self) -> pd.DataFrame:
        # Create a dataframe of zeros.
        df_lost = self.df_zeros.copy()
        # Make 1 all cells that used to have data in the old version, but are missing in the new version.
        for variable_id_old, variable_id_new in self.variable_mapping.items():
            affected_rows = self.df[(self.df[variable_id_old].notnull()) & (self.df[variable_id_new].isnull())].index
            df_lost.loc[affected_rows, variable_id_new] = 1

        # Get long format of the score dataframe.
        df_score = get_long_format_score_df(df_lost)

        return df_score


class AnomalyVersionChange(AnomalyDetector):
    """New dataframe has changed abruptly with respect to the old version."""

    anomaly_type = "version_change"

    def get_score_df(self) -> pd.DataFrame:
        # Create a dataframe of zeros.
        df_version_change = self.df_zeros.copy()
        for variable_id_old, variable_id_new in self.variable_mapping.items():
            # Calculate the BARD epsilon for each variable.
            eps = estimate_bard_epsilon(series=self.df[variable_id_new])
            # Calculate the BARD for each variable.
            variable_bard = bard(a=self.df[variable_id_old], b=self.df[variable_id_new], eps=eps)
            # Add bard to the dataframe.
            df_version_change[variable_id_new] = variable_bard

        # Get long format of the score dataframe.
        df_score = get_long_format_score_df(df_version_change)

        return df_score


class AnomalyTimeChange(AnomalyDetector):
    """New dataframe has abrupt changes in time series."""

    anomaly_type = "time_change"

    def get_score_df(self) -> pd.DataFrame:
        # Create a dataframe of zeros.
        df_time_change = self.df_zeros.copy()
        # Sanity check.
        error = "The function that detects abrupt time changes assumes the data is sorted by entity_id and year. But this is not the case. Either ensure the data is sorted, or fix the function."
        assert (self.df.sort_values(by=INDEX_COLUMNS).index == self.df.index).all(), error
        for variable_id in self.variable_ids:
            series = self.df[variable_id].copy()
            # Calculate the BARD epsilon for this variable.
            eps = estimate_bard_epsilon(series=series)
            # Calculate the BARD for this variable.
            _bard = bard(series, series.shift(), eps).fillna(0)

            # Add bard to the dataframe.
            df_time_change[variable_id] = _bard
        # The previous procedure includes the calculation of the deviation between the last point of an entity and the first point of the next, which is meaningless, and can lead to a high BARD.
        # Therefore, make zero the first point of each entity_id for all columns.
        df_time_change.loc[df_time_change["entity_id"].diff().fillna(1) > 0, self.variable_ids] = 0

        # Get long format of the score dataframe.
        df_score = get_long_format_score_df(df_time_change)

        return df_score


# Define mapping of available anomaly types to anomaly detectors.
ANOMALY_DETECTORS = {
    "time_change": AnomalyTimeChange,
    "version_change": AnomalyVersionChange,
    "lost": AnomalyLostData,
}

########################################################################################################################

# AGGREGATE ANOMALIES:


def aggregate_anomalies(
    df_scores: pd.DataFrame,
    df_population: pd.DataFrame,
    df_views: pd.DataFrame,
    metadata: Dict[int, gm.Variable],
    entity_id_to_name: Dict[int, str],
) -> pd.DataFrame:
    # Create a dataframe of zeros.
    df_aggregated = df_scores.copy()

    # Add country and indicator names.
    df_aggregated["country"] = map_series(
        df_aggregated["entity_id"], entity_id_to_name, warn_on_missing_mappings=True, warn_on_unused_mappings=True
    )
    df_aggregated["variable"] = map_series(
        df_aggregated["variable_id"],
        {variable_id: metadata[variable_id]["shortName"] for variable_id in metadata},  # type: ignore
        warn_on_missing_mappings=True,
        warn_on_unused_mappings=False,
    )

    # Add population score to the aggregated scores dataframe.
    df_aggregated = df_aggregated.merge(
        get_population_score(df_aggregated, df_population), on=["country", "year"], how="left"
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
        df_aggregated[["country", "year"]]  # type: ignore
        .drop_duplicates()
        .merge(df_population, on=["country", "year"], how="left")
    )
    # To normalize the population score to the range 0, 1, divide by an absolute maximum population of 10 billion.
    # To have more convenient numbers, take the natural logarithm of the population.
    df_score_population["population_score"] = np.log(df_score_population["population"]) / np.log(10e9)
    # It's unclear what to do with entities that do not have a population (e.g. "Middle East").
    # For now, add a score of 0.5 to them.
    df_score_population["population_score"] = df_score_population["population_score"].fillna(0.5)

    df_score_population = df_score_population[["country", "year", "population_score"]]

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
    dataset_ids: Optional[list[int]] = None,
    variable_mapping: Optional[dict[int, int]] = None,
    variable_ids: Optional[list[int]] = None,
    dry_run: bool = False,
    reset_db: bool = False,
) -> None:
    """TBD

    TBD

    **Example 1:** Create random anomaly for a dataset

    ```
    $ etl anomalist --anomaly-type sample --dataset-ids 6369
    ```

    **Example 2:** Create GP anomalies

    ```
    $ etl anomalist --anomal-_type gp --dataset-ids 6369
    ```

    **Example 3:** Create anomalies by comparing dataset to its previous version

    ```
    $ etl anomalist --anomaly-type gp --dataset-ids 6589
    ```
    """
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
    if dataset_ids is None:
        dataset_ids = []
    # Dictionary variable_id: Variable object, for all variables (old and new).
    variables = {
        variable.id: variable
        for variable in _load_variables_meta(engine=engine, dataset_ids=dataset_ids, variable_ids=variable_ids_all)
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
            .rename(columns={"entityId": "entity_id"}, errors="raise")
        )

        # Load mapping of entity ids to entity names.
        # NOTE: Ideally, entities should be loaded earlier.
        entity_id_to_name = io.load_entity_mapping(entity_ids=list(set(df["entity_id"])))
        for anomaly_type in anomaly_types:
            # Instantiate the anomaly detector.
            if anomaly_type not in ANOMALY_DETECTORS:
                raise ValueError(f"Unsupported anomaly type: {anomaly_type}")

            # Instantiate the anomaly detector.
            detector = ANOMALY_DETECTORS[anomaly_type](
                variable_ids=variable_ids,
                df=df,
                metadata=variables,
                variable_mapping=variable_mapping,
                entity_id_to_name=entity_id_to_name,
            )

            # detect anomalies
            log.info(f"Detecting anomaly type {anomaly_type} for dataset {dataset_id}")
            # the output has the same shape as the input dataframe, but we should make
            # it possible to return anomalies in a long format (for detectors that return
            # just a few anomalies)
            df_score = detector.get_score_df()

            # TODO: validate format of the output dataframe
            anomaly = gm.Anomaly(
                datasetId=dataset_id,
                anomalyType=detector.anomaly_type,
            )
            anomaly.dfScore = df_score

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
    df = df_long.pivot(index=["entityId", "entityName", "year"], columns="variableId", values="value")

    # reorder in the same order as variables
    df = df[[v.id for v in variables]]

    # try converting to numeric
    df = df.astype(float)

    # TODO:
    # remove countries with all nulls or all zeros or constant values
    # df = df.loc[:, df.fillna(0).std(axis=0) != 0]

    return df


# @memory.cache
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
