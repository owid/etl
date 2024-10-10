import pandas as pd

from etl import grapher_model as gm

from .gp_anomaly import AnomalyDetector


class NaNAnomalyDetector(AnomalyDetector):
    anomaly_type = "nan"

    def get_score_df(self, df: pd.DataFrame, variables: list[gm.Variable]) -> pd.DataFrame:
        return df.isnull().astype(float)


class LostAnomalyDetector(AnomalyDetector):
    anomaly_type = "lost"


class VersionChangeAnomalyDetector(AnomalyDetector):
    anomaly_type = "version_change"


class TimeChangeAnomalyDetector(AnomalyDetector):
    anomaly_type = "time_change"
