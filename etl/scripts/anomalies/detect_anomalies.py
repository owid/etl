"""Load two consecutive versions of an ETL grapher dataset, and identify the most significant changes.

"""

import concurrent.futures
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import plotly.express as px
from owid.catalog import find
from owid.datautils.dataframes import map_series, multi_merge
from sqlalchemy.orm import Session
from tqdm.auto import tqdm

import etl.grapher_io as io
from etl.config import OWID_ENV
from etl.data_helpers.misc import bard
from etl.db import read_sql
from etl.grapher_model import Dataset

# Name of index columns for dataframe.
INDEX_COLUMNS = ["entity_id", "year"]

# TODO: Move to etl.db or elsewhere after refactoring.


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


def _load_variable_data_and_metadata(variable_id: int) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    # Load variable data.
    variable_df = io.load_variable_data(variable_id=variable_id, set_entity_names=False)

    # Rename columns appropriately.
    variable_df = variable_df.rename(
        columns={"entities": "entity_id", "years": "year", "values": variable_id}, errors="raise"
    )
    variable_df = variable_df.astype({"entity_id": int})

    # Load variable metadata.
    variable_metadata = io.load_variable_metadata(variable_id=variable_id)

    return variable_df, variable_metadata


def load_variables_data_and_metadata(variable_ids: List[int]) -> Tuple[pd.DataFrame, Dict[int, str]]:
    # Initialize list of all variables data and dictionary of all variables metadata.
    dfs = []
    metadata = {}

    # Use ThreadPoolExecutor to parallelize loading of variables.
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit tasks for each variable_id.
        futures = {
            executor.submit(_load_variable_data_and_metadata, variable_id): variable_id for variable_id in variable_ids
        }

        # Iterate over the futures as they complete.
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
            variable_id = futures[future]
            try:
                variable_df, variable_metadata = future.result()
                # Append dataframe and metadata.
                dfs.append(variable_df)
                metadata[variable_id] = variable_metadata
            except Exception as exc:
                print(f"Variable {variable_id} generated an exception: {exc}")

    # Merge the dataframes.
    df = multi_merge(dfs, how="outer", on=INDEX_COLUMNS)

    # Sort columns and rows conveniently.
    df = df[INDEX_COLUMNS + sorted(metadata.keys())].sort_values(by=INDEX_COLUMNS).reset_index(drop=True)

    return df, metadata


########################################################################################################################


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


class AnomalyDetector:
    def __init__(self, variable_ids: List[int], variable_mapping: Dict[int, int]) -> None:
        self.variable_ids = variable_ids
        self.variable_mapping = variable_mapping

        # Initialize data objects.
        self.df = pd.DataFrame()
        self.metadata = dict()
        self.entity_id_to_name = dict()
        self.df_zeros = pd.DataFrame()

        # Dictionary with dataframes of the complete anomalies data.
        self.anomaly_dfs = dict()

        # Anomaly methods to use.
        self.anomaly_methods = {
            # "nan": self.score_nan,
            "lost": self.score_lost,
            "version_change": self.score_version_change,
            "time_change": self.score_time_change,
        }

        # Dataframe with a compilation of the most important anomalies.
        self.df_scores = pd.DataFrame()

    # Load all necessary data (this would be cached in streamlit app).
    def load_data(self) -> None:
        # Gather old and new variable ids.
        variable_ids_all = list(set(self.variable_ids + list(self.variable_mapping.keys())))
        # Load all variables data and metadata.
        self.df, self.metadata = load_variables_data_and_metadata(variable_ids=variable_ids_all)

        # Load mapping of entity ids to entity names.
        self.entity_id_to_name = io.load_entity_mapping(entity_ids=list(set(self.df["entity_id"])))

        # Create a dataframe of zeros, that will be used for each data anomaly type.
        self.df_zeros = pd.DataFrame(np.zeros_like(self.df), columns=self.df.columns)[INDEX_COLUMNS + self.variable_ids]
        self.df_zeros[INDEX_COLUMNS] = self.df[INDEX_COLUMNS].copy()

        # Load the latest population data from the catalog.
        self.df_population = load_latest_population()

        # Get variable views in charts.
        # TODO: How should the connection be handled here?
        self.df_views = get_variables_views_in_charts(variable_ids=self.variable_ids)

    ########################################################################################################################

    # ANOMALY TYPE "nan":
    # New data is nan (regardless of any possible old data).

    def score_nan(self) -> pd.DataFrame:
        # TODO: Currently, when creating self.df, we merge many variables on the same entity-year.
        #  Therefore, there are certainly going to be nans in the dataframe that were not necessarily in the original indicator data. This function should only show original nans, but this complicates things a bit at this stage (and this type of anomaly doesn't seem very useful).
        # Create a dataframe of zeros.
        df_nan = self.df[INDEX_COLUMNS + self.variable_ids].isnull().astype(float)
        df_nan[INDEX_COLUMNS] = self.df[INDEX_COLUMNS].copy()

        return df_nan

    ########################################################################################################################

    # ANOMALY TYPE "lost":
    # New data misses entity-years that used to exist in old version.

    def score_lost(self) -> pd.DataFrame:
        # Create a dataframe of zeros.
        df_lost = self.df_zeros.copy()
        # Make 1 all cells that used to have data in the old version, but are missing in the new version.
        for variable_id_old, variable_id_new in self.variable_mapping.items():
            affected_rows = self.df[(self.df[variable_id_old].notnull()) & (self.df[variable_id_new].isnull())].index
            df_lost.loc[affected_rows, variable_id_new] = 1

        return df_lost

    ########################################################################################################################

    # ANOMALY TYPE "version_change":
    # New dataframe has changed abruptly with respect to the old version.

    def score_version_change(self) -> pd.DataFrame:
        # Create a dataframe of zeros.
        df_version_change = self.df_zeros.copy()
        for variable_id_old, variable_id_new in self.variable_mapping.items():
            # Calculate the BARD epsilon for each variable.
            eps = estimate_bard_epsilon(series=self.df[variable_id_new])
            # Calculate the BARD for each variable.
            variable_bard = bard(a=self.df[variable_id_old], b=self.df[variable_id_new], eps=eps)
            # Add bard to the dataframe.
            df_version_change[variable_id_new] = variable_bard

        return df_version_change

    ########################################################################################################################

    # ANOMALY TYPE: "time_change":
    # New dataframe has abrupt changes in time series.

    def score_time_change(self) -> pd.DataFrame:
        # Create a dataframe of zeros.
        df_time_change = self.df_zeros.copy()
        # Sanity check.
        error = "The function that detects abrupt time changes assumes the data is sorted by entity_id and year. But this is not the case. Either ensure the data is sorted, or fix the function."
        assert (self.df.sort_values(by=INDEX_COLUMNS).index == self.df.index).all(), error
        for variable_id in variable_ids:
            series = self.df[variable_id].copy()
            # Calculate the BARD epsilon for this variable.
            eps = estimate_bard_epsilon(series=series)
            # Calculate the BARD for this variable.
            _bard = bard(series, series.shift(), eps).fillna(0)

            # Add bard to the dataframe.
            df_time_change[variable_id] = _bard
        # The previous procedure includes the calculation of the deviation between the last point of an entity and the first point of the next, which is meaningless, and can lead to a high BARD.
        # Therefore, make zero the first point of each entity_id for all columns.
        df_time_change.loc[df_time_change["entity_id"].diff().fillna(1) > 0, variable_ids] = 0

        return df_time_change

    ########################################################################################################################

    # DETECT ALL ANOMALIES:

    def detect_anomalies(self):
        if self.df.empty:
            # Load data if it hasn't been loaded yet.
            self.load_data()

        # Fill the anomaly_dfs dictionary with the dataframes of the complete anomalies data.
        for anomaly_method in self.anomaly_methods:
            df_score = self.anomaly_methods[anomaly_method]()
            self.anomaly_dfs[anomaly_method] = df_score

    ########################################################################################################################

    # AGGREGATE ANOMALIES:

    def aggregate_anomalies(self):
        if self.anomaly_dfs == dict():
            # Detect anomalies if they haven't been detected yet.
            self.detect_anomalies()

        # Prepare scores.
        df_scores = []
        for score_name, anomaly_df in self.anomaly_dfs.items():
            # Create a score dataframe.
            _df_score = (
                anomaly_df.melt(id_vars=["entity_id", "year"], var_name="variable_id", value_name="anomaly_score")
                .fillna(0)
                .assign(**{"anomaly_type": score_name})
            )
            # For now, keep only the latest year affected for each country-indicator.
            _df_score = (
                _df_score.sort_values("anomaly_score", ascending=False)
                .drop_duplicates(subset=["variable_id", "entity_id"], keep="first")
                .reset_index(drop=True)
            )
            # # Sanity checks.
            assert (_df_score.isnull().sum() == 0).all()
            assert len(_df_score) == len(self.df["entity_id"].unique()) * len(self.variable_ids)
            df_scores.append(_df_score)

        # Aggregate anomalies.
        df_scores = pd.concat(df_scores, ignore_index=True)

        # Add country and indicator names.
        df_scores["country"] = map_series(
            df_scores["entity_id"], self.entity_id_to_name, warn_on_missing_mappings=True, warn_on_unused_mappings=True
        )
        df_scores["variable"] = map_series(
            df_scores["variable_id"],
            {variable_id: self.metadata[variable_id]["shortName"] for variable_id in self.metadata},  # type: ignore
            warn_on_missing_mappings=True,
            warn_on_unused_mappings=False,
        )

        # Update the scores dataframe.
        self.df_scores = df_scores

        # Add population score.
        self.add_population_score()

        # Add analytics score.
        self.add_analytics_score()

    def add_population_score(self) -> None:
        # NOTE: This is a special type of score that is added afterwards to help rank anomalies.
        #  It would not make sense to calculate a population score at the beginning and select the largest anomalies based on it (which would trivially pick the most populated countries).

        # First, get the unique combinations of country-years in the scores dataframe, and add population to it.
        df_score_population = (
            self.df_scores[["country", "year"]]  # type: ignore
            .drop_duplicates()
            .merge(self.df_population, on=["country", "year"], how="left")
        )
        # To normalize the population score to the range 0, 1, divide by an absolute maximum population of 10 billion.
        # To have more convenient numbers, take the natural logarithm of the population.
        df_score_population["population_score"] = np.log(df_score_population["population"]) / np.log(10e9)
        # It's unclear what to do with entities that do not have a population (e.g. "Middle East").
        # For now, add a score of 0.5 to them.
        df_score_population["population_score"] = df_score_population["population_score"].fillna(0.5)

        # Add population score to the main scores dataframe.
        self.df_scores = self.df_scores.merge(
            df_score_population[["country", "year", "population_score"]], on=["country", "year"], how="left"
        )

    def add_analytics_score(self) -> None:
        # Focus on the following specific analytics column.
        analytics_column = "views_14d"

        # Get the sum of the number of views in charts for each variable id in the last 14 days.
        # So, if an indicator is used in multiple charts, their views are summed.
        # This rewards indicators that are used multiple times, and on popular charts.
        # NOTE: The analytics table often contains nans. Not sure why this happens, e.g. to coal-electricity-per-capita: https://admin.owid.io/admin/charts/4451/edit
        #  For now, for convenience, fill them with 1.1 views (to avoid zeros when calculating the log).
        df_score_analytics = (
            self.df_views.fillna(1.1)
            .groupby("variable_id")
            .agg({analytics_column: "sum"})
            .reset_index()
            .rename(columns={analytics_column: "views"})
        )
        # To normalize the analytics score to the range 0, 1, divide by an absolute maximum number of views.
        absolute_maximum_views = 1e6
        error = f"Expected a maximum number of views below {absolute_maximum_views}. Change this limit."
        assert self.df_views[analytics_column].max() < absolute_maximum_views, error
        # To have more convenient numbers, take the natural logarithm of the views.
        df_score_analytics["analytics_score"] = np.log(df_score_analytics["views"]) / np.log(absolute_maximum_views)

        # Add analytics score to the main scores dataframe.
        self.df_scores = self.df_scores.merge(
            df_score_analytics[["variable_id", "analytics_score"]], on=["variable_id"], how="left"
        )

        # NOTE: Variables that have do not have charts will have an analytics score nan.
        #  Fill them with zeros.
        self.df_scores["analytics_score"] = self.df_scores["analytics_score"].fillna(0)

    ########################################################################################################################

    # Visually inspect the most significant anomalies on a certain scores dataframe.
    def inspect_anomalies(self, anomalies: Optional[pd.DataFrame] = None, n_anomalies: int = 10) -> None:
        if anomalies is None:
            anomalies = self.df_scores.copy()  # type: ignore
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
            variable_name = self.metadata[variable_id]["shortName"]  # type: ignore
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


if __name__ == "__main__":
    # Emulate the circumstances that will be given in the wizard Anomalist app.

    # Dataset ids of the latest and previous versions of the electricity_mix dataset.
    # In this example, all variables from the old dataset are mapped to variables in the new one, except one.
    # One variable is unique to the new dataset, namely 985610, "Per capita electricity demand - kWh".
    DATASET_ID_NEW = 6589
    DATASET_ID_OLD = 6322

    # Load variables of the latest electricity_mix dataset.
    with Session(OWID_ENV.engine) as session:
        variables_new = Dataset.load_variables_for_dataset(session, dataset_id=DATASET_ID_NEW)
        variables_old = Dataset.load_variables_for_dataset(session, dataset_id=DATASET_ID_OLD)

    # List of new variables.
    variable_ids = []
    variable_mapping = {}
    for variable in variables_new:
        variable_name = variable.shortName
        variable_id = variable.id
        variable_ids.append(variable_id)

        candidates = [variable.id for variable in variables_old if variable.shortName == variable_name]
        if len(candidates) == 1:
            variable_id_old = candidates[0]
            variable_mapping[variable_id_old] = variable_id

    # Initialize the AnomalyDetector object.
    detector = AnomalyDetector(variable_ids=variable_ids, variable_mapping=variable_mapping)
    # Load data.
    detector.load_data()

    # Detect anomalies.
    detector.detect_anomalies()

    # Aggregate anomalies.
    detector.aggregate_anomalies()

    # Apply filters to select the most significant anomalies.
    anomalies = detector.df_scores.copy()
    # Anomalies of type "lost" and "nan" should not be shown, but rather appear in a small warning.
    # For now, remove them from the list of anomalies to inspect.
    anomalies = anomalies.loc[~anomalies["anomaly_type"].isin(["lost", "nan"])].reset_index(drop=True)
    # Renormalize scores based on the average population and analytics scores.
    anomalies["anomaly_score"] *= (anomalies["population_score"] + anomalies["analytics_score"]) * 0.5
    # anomalies = anomalies.sort_values("anomaly_score", ascending=False).reset_index(drop=True)

    # Inspect anomalies.
    detector.inspect_anomalies(anomalies=anomalies, n_anomalies=10)
