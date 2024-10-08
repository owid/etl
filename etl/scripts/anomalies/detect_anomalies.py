"""Load two consecutive versions of an ETL grapher dataset, and identify the most significant changes.

"""

import concurrent.futures
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import plotly.express as px
import requests
from owid.datautils.dataframes import map_series, multi_merge
from sqlalchemy.orm import Session
from tqdm.auto import tqdm

from etl.config import OWID_ENV
from etl.data_helpers.misc import bard
from etl.grapher_model import Dataset, Entity, Variable

# Name of index columns for dataframe.
INDEX_COLUMNS = ["entity_id", "year"]

########################################################################################################################


def load_variable_from_id(variable_id: int):
    with Session(OWID_ENV.engine) as session:
        variable = Variable.load_variable(session=session, variable_id=variable_id)

    return variable


def load_variable_metadata(variable: Variable) -> Dict[str, Any]:
    metadata = requests.get(variable.s3_metadata_path(typ="http")).json()

    return metadata


def load_variable_data(variable: Variable) -> pd.DataFrame:
    data = requests.get(variable.s3_data_path(typ="http")).json()
    df = pd.DataFrame(data)

    return df


def load_entity_mapping(entity_ids: List[int]) -> Dict[int, str]:
    # Fetch the mapping of entity ids to names.
    with Session(OWID_ENV.engine) as session:
        entity_id_to_name = Entity.load_entity_mapping(session=session, entity_ids=entity_ids)

    return entity_id_to_name


########################################################################################################################


def _load_variable_data_and_metadata(variable_id: int) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    # Load variable for the current variable id.
    variable = load_variable_from_id(variable_id=variable_id)

    # Load variable data.
    variable_df = load_variable_data(variable=variable)
    # Rename columns appropriately.
    variable_df = variable_df.rename(
        columns={"entities": "entity_id", "years": "year", "values": variable.id}, errors="raise"
    )

    # Load variable metadata.
    variable_metadata = load_variable_metadata(variable=variable)

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
            "nan": self.score_nan,
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
        self.entity_id_to_name = load_entity_mapping(entity_ids=list(set(self.df["entity_id"])))

        # Create a dataframe of zeros, that will be used for each data anomaly type.
        self.df_zeros = pd.DataFrame(np.zeros_like(self.df), columns=self.df.columns)[INDEX_COLUMNS + variable_ids]
        self.df_zeros[INDEX_COLUMNS] = self.df[INDEX_COLUMNS].copy()

    ########################################################################################################################

    # ANOMALY TYPE "nan":
    # New data is nan (regardless of any possible old data).

    def score_nan(self) -> pd.DataFrame:
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
            positive_values = abs(self.df[variable_id_new].dropna())
            positive_values = positive_values.loc[positive_values > 0]
            # One way to calculate it would be to assume that the epsilon is the 10th percentile of each new indicator.
            # eps = np.percentile(positive_values, q=0.1)
            # Another way is to simply take the range of the positive values and divide it by 10.
            eps = (positive_values.max() - positive_values.min()) / 10
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
        for col in variable_ids:
            # TODO: This is not very meaningful, but for now it's a placeholder.
            # Compute the Z-score for each value in the column.
            z_score = abs(self.df[col] - self.df[col].mean()) / self.df[col].std()
            # Rescale the Z-scores to be between 0 and 1 using a min-max scaling.
            df_time_change[col] = (z_score - z_score.min()) / (z_score.max() - z_score.min())

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
                .assign(**{"score_name": score_name})
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
        df_score = pd.concat(df_scores, ignore_index=True)

        # For convenience, add country and indicator names.
        df_score["country"] = map_series(
            df_score["entity_id"], self.entity_id_to_name, warn_on_missing_mappings=True, warn_on_unused_mappings=True
        )
        df_score["variable"] = map_series(
            df_score["variable_id"],
            {variable_id: self.metadata[variable_id]["shortName"] for variable_id in self.metadata},  # type: ignore
            warn_on_missing_mappings=True,
            warn_on_unused_mappings=False,
        )

        # NOTE: Here, we could include population data, or analytics (e.g. number of views for charts of each indicator) and create a score based on those.
        self.df_scores = df_score

    ########################################################################################################################

    # Visually inspect the most significant anomalies on a certain scores dataframe.
    def inspect_anomalies(self, anomalies: Optional[pd.DataFrame] = None, n_anomalies: int = 10) -> None:
        if anomalies is None:
            anomalies = self.df_scores.copy()
        # Select the most significant anomalies.
        anomalies = anomalies.sort_values("anomaly_score", ascending=False).head(n_anomalies)
        # Reverse variable mapping.
        variable_id_new_to_old = {v: k for k, v in self.variable_mapping.items()}
        anomalies["variable_id_old"] = map_series(
            anomalies["variable_id"], variable_id_new_to_old, warn_on_missing_mappings=False
        )
        for _, row in anomalies.iterrows():
            variable_id = row["variable_id"]
            variable_name = self.metadata[variable_id]["shortName"]  # type: ignore
            country = row["country"]
            score_name = row["score_name"]
            anomaly_year = row["year"]
            anomaly_score = row["anomaly_score"]
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
                    title=f"{variable_name} - {country} ({anomaly_year} - {anomaly_score:.0%})",
                    markers=True,
                    color_discrete_map={"old": "rgba(256,0,0,0.5)", "new": "rgba(0,256,0,0.5)"},
                ).show()
            else:
                px.line(
                    new,
                    x="year",
                    y=variable_name,
                    title=f"{variable_name} - {country} ({anomaly_year} - {anomaly_score:.0%})",
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
    # TODO: Rename score_name -> anomaly_type
    anomalies = detector.df_scores.copy()
    # anomalies = anomalies.loc[anomalies["score_name"] == "version_change"].reset_index(drop=True)
    anomalies = anomalies.loc[anomalies["score_name"] == "time_change"].reset_index(drop=True)

    # Inspect anomalies.
    detector.inspect_anomalies(anomalies=anomalies, n_anomalies=10)
