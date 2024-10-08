"""Load two consecutive versions of an ETL grapher dataset, and identify the most significant changes.

"""

import concurrent.futures
from typing import Any, Dict, List, Tuple

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

# def load_variables_data_metadata_and_entities(variable_ids: List[int]) -> Tuple[pd.DataFrame, Dict[int, str], Dict[int, str]]:
#     dfs = []
#     metadata = {}
#     for variable_id in tqdm(variable_ids):
#         # Load variable for the current variable id.
#         variable = load_variable_from_id(variable_id=variable_id)

#         # Load variable data.
#         variable_df = load_variable_data(variable=variable)
#         # Rename columns appropriately.
#         variable_df = variable_df.rename(columns={"entities": "entity_id", "years": "year", "values": variable.id}, errors="raise")
#         # Add to list of variables data.
#         dfs.append(variable_df)

#         # Load variable metadata.
#         variable_metadata = load_variable_metadata(variable=variable)
#         # Add to dictionary of variables metadata.
#         metadata[variable_id] = variable_metadata

#     # Merge the dataframes.
#     df = multi_merge(dfs, how="outer", on=["entity_id", "year"])

#     # Map entity ids to entity names.
#     entity_ids = list(set(df["entity_id"]))
#     entity_id_to_name = load_entity_mapping(entity_ids=entity_ids)

#     return df, metadata, entity_id_to_name


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

# Load all necessary data (this would be cached in streamlit app).

# Gather old and new variable ids.
variable_ids_all = list(set(variable_ids + list(variable_mapping.keys())))
# Load all variables data and metadata.
df, metadata = load_variables_data_and_metadata(variable_ids=variable_ids_all)
# Load mapping of entity ids to entity names.
entity_id_to_name = load_entity_mapping(entity_ids=list(set(df["entity_id"])))

# Create a dataframe of zeros, that will be used for each data anomaly type.
df_zeros = pd.DataFrame(np.zeros_like(df), columns=df.columns)[INDEX_COLUMNS + variable_ids]
df_zeros[INDEX_COLUMNS] = df[INDEX_COLUMNS].copy()


# TODO: It possibly makes more sense if df contains only new variables and we have a separate df_old for the old ones.


def prepare_score(df: pd.DataFrame, score_name: str) -> pd.DataFrame:
    # Create a score dataframe.
    df_score = df.melt(id_vars=["entity_id", "year"], var_name="variable_id", value_name=f"score_{score_name}").fillna(
        0
    )

    # For now, keep only the latest year affected for each country-indicator.
    df_score = (
        df_score.sort_values(f"score_{score_name}", ascending=False)
        .drop_duplicates(subset=["variable_id", "entity_id"], keep="first")
        .rename(columns={"year": f"year_{score_name}"})
        .reset_index(drop=True)
    )

    return df_score


########################################################################################################################

# ANOMALY TYPE "nan":
# New data is nan (regardless of any possible old data).

# Create a dataframe of zeros.
df_nan = df[INDEX_COLUMNS + variable_ids].isnull().astype(float)
df_nan[INDEX_COLUMNS] = df[INDEX_COLUMNS].copy()

# Create a score dataframe.
df_nan_score = prepare_score(df=df_nan, score_name="nan")

# Sanity checks.
assert (df_nan_score.isnull().sum() == 0).all()

########################################################################################################################

# ANOMALY TYPE "lost":
# New data misses entity-years that used to exist in old version.

# Create a dataframe of zeros.
df_lost = df_zeros.copy()
# Make 1 all cells that used to have data in the old version, but are missing in the new version.
for variable_id_old, variable_id_new in variable_mapping.items():
    affected_rows = df[(df[variable_id_old].notnull()) & (df[variable_id_new].isnull())].index
    df_lost.loc[affected_rows, variable_id_new] = 1

# Create a score dataframe.
df_lost_score = prepare_score(df=df_lost, score_name="lost")

# Sanity checks.
assert (df_lost_score.isnull().sum() == 0).all()
assert len(df_lost_score) == len(df_nan_score)

########################################################################################################################

# ANOMALY TYPE "version_change":
# New dataframe has changed abruptly with respect to the old version.

# Create a dataframe of zeros.
df_version_change = df_zeros.copy()
for variable_id_old, variable_id_new in variable_mapping.items():
    # Calculate the BARD epsilon for each variable.
    positive_values = abs(df[variable_id_new].dropna())
    positive_values = positive_values[positive_values > 0]
    # One way to calculate it would be to assume that the epsilon is the 10th percentile of each new indicator.
    # eps = np.percentile(positive_values, q=0.1)
    # Another way is to simply take the range of the positive values and divide it by 10.
    eps = (positive_values.max() - positive_values.min()) / 10
    # Calculate the BARD for each variable.
    variable_bard = bard(a=df[variable_id_old], b=df[variable_id_new], eps=eps)
    # Add bard to the dataframe.
    df_version_change[variable_id_new] = variable_bard

# Create a score dataframe.
df_version_change_score = prepare_score(df=df_version_change, score_name="version_change")

# Sanity checks.
assert (df_version_change_score.isnull().sum() == 0).all()
assert len(df_version_change_score) == len(df_nan_score)

########################################################################################################################

# ANOMALY TYPE: "time_change":
# New dataframe has abrupt changes in time series.

# Create a dataframe of zeros.
df_time_change = df_zeros.copy()

for col in variable_ids:
    # TODO: This is not very meaningful, but for now it's a placeholder.

    # Compute the Z-score for each value in the column.
    z_score = abs(df[col] - df[col].mean()) / df[col].std()
    # Rescale the Z-scores to be between 0 and 1 using a min-max scaling.
    df_time_change[col] = (z_score - z_score.min()) / (z_score.max() - z_score.min())

# Create a score dataframe.
df_time_change_score = prepare_score(df=df_time_change, score_name="time_change")

# Sanity checks.
assert (df_time_change_score.isnull().sum() == 0).all()
assert len(df_time_change_score) == len(df_nan_score)

########################################################################################################################

# AGGREGATE ANOMALIES:

df_scores = multi_merge(
    [df_nan_score, df_lost_score, df_version_change_score, df_time_change_score],
    how="outer",
    on=["entity_id", "variable_id"],
)
df_scores["country"] = map_series(
    df_scores["entity_id"], entity_id_to_name, warn_on_missing_mappings=True, warn_on_unused_mappings=True
)
df_scores["variable"] = map_series(
    df_scores["variable_id"],
    {variable_id: metadata[variable_id]["shortName"] for variable_id in metadata},
    warn_on_missing_mappings=True,
    warn_on_unused_mappings=False,
)


# Visually inspect the most significant anomalies.
def inspect_anomalies(df_scores: pd.DataFrame, score_name: str, n_anomalies: int = 10) -> None:
    # Select the most significant anomalies.
    anomalies = df_scores.sort_values(f"score_{score_name}", ascending=False).head(n_anomalies)
    # Reverse variable mapping.
    variable_id_new_to_old = {v: k for k, v in variable_mapping.items()}
    anomalies["variable_id_old"] = map_series(
        anomalies["variable_id"], variable_id_new_to_old, warn_on_missing_mappings=False
    )
    for _, row in anomalies.iterrows():
        variable_id = row["variable_id"]
        variable_name = metadata[variable_id]["shortName"]
        country = row["country"]
        anomaly_year = row[f"year_{score_name}"]
        anomaly_score = row[f"score_{score_name}"]
        new = df[df["entity_id"] == row["entity_id"]][["entity_id", "year", variable_id]]
        new["country"] = map_series(new["entity_id"], entity_id_to_name)
        new = new.drop(columns=["entity_id"]).rename(columns={row["variable_id"]: variable_name}, errors="raise")
        if score_name == "version_change":
            variable_id_old = row["variable_id_old"]
            old = df[df["entity_id"] == row["entity_id"]][["entity_id", "year", variable_id_old]]
            old["country"] = map_series(old["entity_id"], entity_id_to_name)
            old = old.drop(columns=["entity_id"]).rename(
                columns={row["variable_id_old"]: variable_name}, errors="raise"
            )
            compare = pd.concat([old.assign(**{"source": "old"}), new.assign(**{"source": "new"})], ignore_index=True)
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


inspect_anomalies(df_scores=df_scores, score_name="version_change")
