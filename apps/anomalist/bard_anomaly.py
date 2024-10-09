from etl.scripts.anomalies.detect_anomalies import AnomalyDetector as ADetector
import random
import time
import warnings
from typing import Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import structlog
from sklearn.exceptions import ConvergenceWarning
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import RBF, WhiteKernel

from etl import grapher_model as gm

from .gp_anomaly import AnomalyDetector


class NaNAnomalyDetector(AnomalyDetector):
    anomaly_type = "nan"

    def get_anomalies(self, df: pd.DataFrame, meta: gm.Variable) -> list[gm.Anomaly]:
        ADetector(variable_ids=, variable_mapping=)

        return [
            gm.Anomaly(
                entity=random.choice(df.columns),
                year=random.choice(df.index),
                rawScore=9.99,
            )
        ]


class LostAnomalyDetector(AnomalyDetector):
    anomaly_type = "lost"


class VersionChangeAnomalyDetector(AnomalyDetector):
    anomaly_type = "version_change"


class TimeChangeAnomalyDetector(AnomalyDetector):
    anomaly_type = "time_change"
