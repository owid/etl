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

log = structlog.get_logger()


class AnomalyDetector:
    anomaly_type: str


class SampleAnomalyDetector(AnomalyDetector):
    anomaly_type = "sample"

    def get_anomalies(self, df: pd.DataFrame, meta: gm.Variable) -> list[gm.Anomaly]:
        return [
            gm.Anomaly(
                entity=random.choice(df.columns),
                year=random.choice(df.index),
                rawScore=9.99,
            )
        ]


class GPAnomalyDetector(AnomalyDetector):
    anomaly_type = "gp"

    def get_anomalies(self, df: pd.DataFrame, meta: gm.Variable) -> list[gm.Anomaly]:
        anomalies = []
        for country in df.columns:
            series = df[country].dropna()

            if len(series) <= 1:
                log.warning(f"Insufficient data for {country}")
                continue

            X, y = self.get_Xy(series)

            t = time.time()
            mean_pred, std_pred = self.fit_predict(X, y)
            log.info("Fitted GP", country=country, n_samples=len(X), elapsed=round(time.time() - t, 2))

            # Calculate Z-score for all points
            z = (y - mean_pred) / std_pred

            # Return anomalies above threshold
            for i, z_score in enumerate(z):
                if np.abs(z_score) > 3:
                    anomalies.append(gm.Anomaly(entity=country, year=series.index[i], rawScore=z_score))

        return anomalies

    def get_Xy(self, series: pd.Series) -> tuple[np.ndarray, np.ndarray]:
        X = series.index.values.reshape(-1, 1)
        y = series.values
        return X, y  # type: ignore

    def fit_predict(self, X, y):
        # normalize data... but is it necessary?
        X_mean = np.mean(X)
        y_mean = np.mean(y)
        y_std = np.std(y)
        X_normalized = X - X_mean
        y_normalized = (y - y_mean) / y_std

        x_range = X_normalized.max() - X_normalized.min()

        # TODO: we could also preprocess data by:
        # - applying power transform to un-log data
        # - removing linear trend
        # - use Nystroem kernel approximation

        # Bounds are set to prevent overfitting to the data and missing outliers, especially
        # the lower bounds for the length scale and noise level
        length_scale_bounds = (min(1e1, x_range), max(1e3, x_range))
        noise_level_bounds = (1e-1, 1e1)

        kernel = 1.0 * RBF(length_scale_bounds=length_scale_bounds) + WhiteKernel(noise_level_bounds=noise_level_bounds)

        self.gp = gp = GaussianProcessRegressor(kernel=kernel, n_restarts_optimizer=0)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", ConvergenceWarning)
            gp.fit(X_normalized, y_normalized)

        # Make predictions with confidence intervals
        mean_pred, std_pred = gp.predict(X_normalized, return_std=True)  # type: ignore

        # Denormalize
        mean_pred = mean_pred * y_std + y_mean
        std_pred = std_pred * y_std

        return mean_pred, std_pred

    def viz(self, df: pd.DataFrame, meta: gm.Variable, country: Optional[str] = None):
        if df.empty:
            log.warning("No data to visualize")
            return

        country = country or random.choice(df.columns)
        series = df[country].dropna()

        if len(series) <= 1:
            log.warning(f"Insufficient data for {country}")
            return

        X, y = self.get_Xy(series)
        log.info("Fitting Gaussian Process", country=country, n_samples=len(X))
        mean_prediction, std_prediction = self.fit_predict(X, y)

        log.info(f"Optimized Kernel: {self.gp.kernel_}")

        plt.figure(figsize=(10, 6))
        plt.scatter(X, y, label="Observations")
        plt.plot(X, y, linestyle="dotted", label="Observed Data")
        plt.plot(X, mean_prediction, label="Mean Prediction", color="orange")
        plt.fill_between(
            X.ravel(),
            mean_prediction - 1.96 * std_prediction,
            mean_prediction + 1.96 * std_prediction,
            alpha=0.3,
            color="lightblue",
            label=r"95% Confidence Interval",
        )
        plt.legend()
        plt.title(f"{meta.name}: {country}")

        z = (y - mean_prediction) / std_prediction
        print("Max Z-score: ", np.abs(z).max())

        plt.show()
