from typing import Dict, List

import pandas as pd
import structlog
from sklearn.ensemble import IsolationForest
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.svm import OneClassSVM
from tqdm.auto import tqdm

from etl.data_helpers.misc import bard

log = structlog.get_logger()

# Name of index columns for dataframe.
INDEX_COLUMNS = ["entity_name", "year"]


def estimate_bard_epsilon(series: pd.Series) -> float:
    # Ignore missing values.
    real_values = series.dropna()
    # Estimate epsilon as the range of values divided by 10.
    # Instead of just taking maximum and minimum, take 95th percentile and 5th percentile.
    eps = (real_values.quantile(0.95) - real_values.quantile(0.05)) / 10

    return eps  # type: ignore


def get_long_format_score_df(df_score: pd.DataFrame, df_scale: pd.DataFrame) -> pd.DataFrame:
    # Output is already in long format
    if set(df_score.columns) == {"entity_name", "year", "variable_id", "anomaly_score"}:
        df_score_long = df_score
        # For now, assume that df_score and df_scale always have the same shape.
        df_scale_long = df_scale
    else:
        # Create a long-format score dataframe.
        df_score_long = df_score.melt(
            id_vars=["entity_name", "year"], var_name="variable_id", value_name="anomaly_score"
        )
        # Create a long-format scale dataframe.
        df_scale_long = df_scale.melt(id_vars=["entity_name", "year"], var_name="variable_id", value_name="score_scale")

    # Drop NaN anomalies.
    df_score_long = df_score_long.dropna(subset=["anomaly_score"])

    # Drop zero anomalies.
    df_score_long = df_score_long[df_score_long["anomaly_score"] != 0]

    # Add a scale column.
    df_score_long = df_score_long.merge(df_scale_long, on=["entity_name", "year", "variable_id"], how="left")

    # Save memory by converting to categoricals.
    df_score_long = df_score_long.astype({"entity_name": "category", "year": "category", "variable_id": "category"})

    return df_score_long


class AnomalyDetector:
    anomaly_type: str

    @staticmethod
    def get_text(entity: str, year: int) -> str:
        return f"Anomaly happened in {entity} in {year}!"

    def get_score_df(self, df: pd.DataFrame, variable_ids: List[int], variable_mapping: Dict[int, int]) -> pd.DataFrame:
        raise NotImplementedError

    def get_scale_df(self, df: pd.DataFrame, variable_ids: List[int], variable_mapping: Dict[int, int]) -> pd.DataFrame:
        raise NotImplementedError

    def get_zeros_df(self, df: pd.DataFrame, variable_ids: List[int]) -> pd.DataFrame:
        # Create a dataframe of zeros.
        df_zeros = df[INDEX_COLUMNS + variable_ids].copy()
        df_zeros[variable_ids] = 0
        return df_zeros


class AnomalyUpgradeMissing(AnomalyDetector):
    """New data misses entity-years that used to exist in old version."""

    anomaly_type = "upgrade_missing"

    @staticmethod
    def get_text(entity: str, year: int) -> str:
        return f"There are missing values for {entity}! There might be other data points affected."

    def get_score_df(self, df: pd.DataFrame, variable_ids: List[int], variable_mapping: Dict[int, int]) -> pd.DataFrame:
        # Create a dataframe of zeros.
        df_lost = self.get_zeros_df(df, variable_ids)

        # Make 1 all cells that used to have data in the old version, but are missing in the new version.
        for variable_id_old, variable_id_new in variable_mapping.items():
            affected_rows = df[(df[variable_id_old].notnull()) & (df[variable_id_new].isnull())].index
            df_lost.loc[affected_rows, variable_id_new] = 1

        return df_lost

    def get_scale_df(self, df: pd.DataFrame, variable_ids: List[int], variable_mapping: Dict[int, int]) -> pd.DataFrame:
        # Create a dataframe of ones.
        df_scale = df.copy()
        # NOTE: Maybe, instead of 1, we could add the maximum value of each country divided by the maximum value of the variable.
        df_scale[df.columns.difference(["entity_name", "year"])] = 1

        return df_scale


class AnomalyUpgradeChange(AnomalyDetector):
    """New dataframe has changed abruptly with respect to the old version."""

    anomaly_type = "upgrade_change"

    @staticmethod
    def get_text(entity: str, year: int) -> str:
        return f"There are significant changes for {entity} in {year} with respect to the previous version. There might be other data points affected."

    def get_score_df(self, df: pd.DataFrame, variable_ids: List[int], variable_mapping: Dict[int, int]) -> pd.DataFrame:
        # Create a dataframe of zeros.
        df_version_change = self.get_zeros_df(df, variable_ids)

        for variable_id_old, variable_id_new in variable_mapping.items():
            # Calculate the BARD epsilon for each variable.
            eps = estimate_bard_epsilon(series=df[variable_id_new])
            # Calculate the BARD for each variable.
            variable_bard = bard(a=df[variable_id_old], b=df[variable_id_new], eps=eps)
            # Add bard to the dataframe.
            df_version_change[variable_id_new] = variable_bard

        return df_version_change

    def get_scale_df(self, df: pd.DataFrame, variable_ids: List[int], variable_mapping: Dict[int, int]) -> pd.DataFrame:
        # Create a dataframe of zeros.
        df_scale = self.get_zeros_df(df, variable_ids)

        for variable_id_old, variable_id_new in variable_mapping.items():
            # Define the scale as the difference between the old and new values, as a fraction of the range of values of the new variable.
            df_scale[variable_id_new] = abs(df[variable_id_new] - df[variable_id_old]) / (
                df[variable_id_new].max() - df[variable_id_new].min()
            )

        return df_scale


class AnomalyTimeChange(AnomalyDetector):
    """New dataframe has abrupt changes in time series."""

    anomaly_type = "time_change"

    @staticmethod
    def get_text(entity: str, year: int) -> str:
        return f"There are significant changes for {entity} in {year} compared to the previous point. There might be other data points affected."

    def get_score_df(self, df: pd.DataFrame, variable_ids: List[int], variable_mapping: Dict[int, int]) -> pd.DataFrame:
        # Create a dataframe of zeros.
        df_time_change = self.get_zeros_df(df, variable_ids)

        # Sanity check.
        error = "The function that detects abrupt time changes assumes the data is sorted by entity_name and year. But this is not the case. Either ensure the data is sorted, or fix the function."
        assert (df.sort_values(by=INDEX_COLUMNS).index == df.index).all(), error
        for variable_id in variable_ids:
            # Calculate the BARD score for this variable.
            series = pd.to_numeric(df[variable_id], errors="coerce")
            eps = estimate_bard_epsilon(series=series)
            score = bard(series, series.shift(), eps).fillna(0)

            # Add score to the dataframe.
            df_time_change[variable_id] = score

        # The previous procedure includes the calculation of the deviation between the last point of an entity and the first point of the next, which is meaningless, and can lead to a high BARD.
        # Therefore, make zero the first point of each entity_name for all columns.
        df_time_change.loc[df_time_change["entity_name"] != df_time_change["entity_name"].shift(), variable_ids] = 0

        return df_time_change

    def get_scale_df(self, df: pd.DataFrame, variable_ids: List[int], variable_mapping: Dict[int, int]) -> pd.DataFrame:
        # Create a dataframe of zeros.
        df_scale = self.get_zeros_df(df, variable_ids)

        for variable_id in variable_ids:
            series = pd.to_numeric(df[variable_id], errors="coerce")
            # The scale is given by the size of changes in consecutive points (for a given country), as a fraction of the maximum range of values of that variable.
            df_scale[variable_id] = abs(series.diff().fillna(0)) / (series.max() - series.min())

        # The previous procedure includes the calculation of the deviation between the last point of an entity and the first point of the next, which is meaningless.
        # Therefore, make zero the first point of each entity_name for all columns.
        df_scale.loc[df_scale["entity_name"] != df_scale["entity_name"].shift(), variable_ids] = 0

        return df_scale


class AnomalyIsolationForest(AnomalyDetector):
    """Anomaly detection using Isolation Forest, applied separately to each country-variable time series."""

    anomaly_type = "isolation_forest"

    def get_score_df(self, df: pd.DataFrame, variable_ids: List[int], variable_mapping: Dict[int, int]) -> pd.DataFrame:
        # Initialize a dataframe of zeros.
        df_anomalies = self.get_zeros_df(df, variable_ids)

        # Initialize an imputer to handle missing values.
        imputer = SimpleImputer(strategy="mean")

        for variable_id in tqdm(variable_ids):
            for country, group in df.groupby("entity_name", observed=True):
                # Get the time series for the current country and variable.
                series = group[[variable_id]].copy()

                # Skip if the series is all zeros or nans.
                if ((series == 0).all().values) or (series.dropna().shape[0] == 0):
                    continue

                # Impute missing values for this country's time series.
                series_imputed = imputer.fit_transform(series)

                # Scale the data.
                scaler = StandardScaler()
                series_scaled = scaler.fit_transform(series_imputed)

                # Initialize the Isolation Forest model.
                isolation_forest = IsolationForest(contamination=0.05, random_state=1)  # type: ignore

                # Fit the model and calculate anomaly scores.
                isolation_forest.fit(series_scaled)
                scores = isolation_forest.decision_function(series_scaled)
                df_anomalies.loc[df["entity_name"] == country, variable_id] = scores

        return df_anomalies


class AnomalyOneClassSVM(AnomalyDetector):
    """Anomaly detection using One-Class SVM, applied separately to each country-variable time series."""

    anomaly_type = "one_class_svm"

    def get_score_df(self, df: pd.DataFrame, variable_ids: List[int], variable_mapping: Dict[int, int]) -> pd.DataFrame:
        # Initialize a dataframe of zeros.
        df_anomalies = self.get_zeros_df(df, variable_ids)

        # Initialize an imputer to handle missing values.
        imputer = SimpleImputer(strategy="mean")

        for variable_id in tqdm(variable_ids):
            for country, group in df.groupby("entity_name", observed=True):
                # Get the time series for the current country and variable.
                series = group[[variable_id]].copy()

                # Skip if the series is all zeros or nans.
                if ((series == 0).all().values) or (series.dropna().shape[0] == 0):
                    continue

                # Impute missing values for this country's time series.
                series_imputed = imputer.fit_transform(series)

                # Scale the data for better performance.
                scaler = StandardScaler()
                series_scaled = scaler.fit_transform(series_imputed)

                # Initialize the One-Class SVM model for this country's time series.
                svm = OneClassSVM(kernel="rbf", gamma="scale", nu=0.05)

                # Fit the model and calculate anomaly scores.
                svm.fit(series_scaled)
                scores = svm.decision_function(series_scaled)
                df_anomalies.loc[df["entity_name"] == country, variable_id] = scores

        return df_anomalies
