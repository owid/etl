import random
import time
import warnings
from multiprocessing import Pool
from typing import Dict, List, Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import structlog
from joblib import Memory
from sklearn.exceptions import ConvergenceWarning
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import RBF, WhiteKernel
from tqdm.auto import tqdm

from etl import grapher_model as gm
from etl.paths import CACHE_DIR

from .detectors import AnomalyDetector

log = structlog.get_logger()


memory = Memory(CACHE_DIR, verbose=0)


@memory.cache
def _load_population():
    from .anomalist_api import load_latest_population

    # Load the latest population data from the API
    pop = load_latest_population()
    # Filter the population data to only include the year 2024
    pop = pop[pop.year == 2024]
    # Set 'entity_name' as the index and return the population series
    return pop.set_index("entity_name")["population"]


def _processing_queue(items: list[tuple[str, int]]) -> List[tuple]:
    """
    Create a processing queue of (entity, variable_id) pairs, weighted by population probability.
    """
    # Load the population data (cached for efficiency)
    population = _load_population().to_dict()

    # Create a probability array for each (entity, variable_id) pair based on the entity probability
    probs = np.array([population.get(entity, np.nan) for entity, variable_id in items])

    # Fill any missing values with the mean probability
    probs = np.nan_to_num(probs, nan=np.nanmean(probs))  # type: ignore

    # Randomly shuffle the items based on their probabilities
    items_index = np.random.choice(
        len(items),
        size=len(items),
        replace=False,
        p=probs / probs.sum(),
    )

    # Return the shuffled list of items
    return np.array(items, dtype=object)[items_index]  # type: ignore


class AnomalyGaussianProcessOutlier(AnomalyDetector):
    anomaly_type = "gp_outlier"

    # TODO: max_time is hard-coded to 100, but it should be configurable in production
    def __init__(self, max_time: Optional[float] = 100, n_jobs: int = 1):
        self.max_time = max_time
        self.n_jobs = n_jobs

    @staticmethod
    def get_text(entity: str, year: int) -> str:
        return f"There are some outliers for {entity}! These were detected using Gaussian processes. There might be other data points affected."

    def get_score_df(self, df: pd.DataFrame, variable_ids: List[int], variable_mapping: Dict[int, int]) -> pd.DataFrame:
        # Convert to long format
        df_wide = (
            df.melt(id_vars=["entity_name", "year"]).set_index(["entity_name", "variable_id"]).dropna().sort_index()
        )

        # Create a processing queue with (entity_name, variable_id) pairs
        items = _processing_queue(
            list(df_wide.index.unique()),
        )

        start_time = time.time()

        results = []

        # Iterate through each (entity_name, variable_id) pair in the processing queue
        for entity_name, variable_id in tqdm(items):
            # Stop processing if the maximum time is reached
            if self.max_time is not None and (time.time() - start_time) > self.max_time:
                log.info("Max processing time reached, stopping further processing.")
                break

            # Get the data for the current entity and variable
            group = df_wide.loc[(entity_name, variable_id)]

            # Skip if the series has only one or fewer data points
            if len(group) <= 1:
                continue

            # Prepare the input features (X) and target values (y) for Gaussian Process
            X, y = self.get_Xy(pd.Series(group["value"].values, index=group["year"]))

            # Skip if the target values have zero standard deviation (i.e., all values are identical)
            if y.std() == 0:
                continue

            if self.n_jobs == 1:
                # Fit the Gaussian Process model and make predictions
                z = self.fit_predict_z(X, y)
                z = pd.DataFrame({"anomaly_score": np.abs(z), "year": group["year"].values}, index=group.index)
                results.append(z)
            else:
                # Add it to a list for parallel processing
                results.append((self, X, y, group, start_time))

        # Process results in parallel
        # NOTE: There's a lot of overhead in parallelizing this, so the gains are minimal on my laptop. It could be
        #  better on a staging server.
        if self.n_jobs != 1:
            with Pool(self.n_jobs) as pool:
                # Split the workload evenly across the number of jobs
                chunksize = len(items) // self.n_jobs + 1
                results = pool.starmap(self._fit_parallel, results, chunksize=chunksize)

        log.info("Finished processing", elapsed=round(time.time() - start_time, 2))

        if not results:
            return pd.DataFrame()

        df_score_long = pd.concat(results).reset_index()

        # Normalize the anomaly scores by mapping interval (0, 3+) to (0, 1)
        df_score_long["anomaly_score"] = np.minimum(df_score_long["anomaly_score"] / 3, 1)

        return df_score_long

    @staticmethod
    def _fit_parallel(obj: "AnomalyGaussianProcessOutlier", X, y, group, start_time):
        # Stop early
        if obj.max_time and (time.time() - start_time) > obj.max_time:
            return pd.DataFrame()
        z = obj.fit_predict_z(X, y)
        z = pd.DataFrame({"anomaly_score": np.abs(z), "year": group["year"].values}, index=group.index)
        return z

    def get_Xy(self, series: pd.Series) -> tuple[np.ndarray, np.ndarray]:
        X = series.index.values.reshape(-1, 1)
        y = series.values
        return X, y  # type: ignore

    def fit_predict_z(self, X, y) -> np.ndarray:
        # t = time.time()
        mean_pred, std_pred = self.fit_predict(X, y)
        # Calculate the Z-score for each point (standard score)
        z = (y - mean_pred) / std_pred
        # log.info(
        #     "Fitted GP",
        #     variable_id=variable_id,
        #     entity_name=entity_name,
        #     n_samples=len(X),
        #     elapsed=round(time.time() - t, 2),
        # )
        return z

    def fit_predict(self, X, y):
        # normalize data... but is it necessary?
        X_mean = np.mean(X)
        y_mean = np.mean(y)
        y_std = np.std(y)
        assert y_std > 0, "Standard deviation of y is zero"

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

        # print kernel
        # log.info(f"Optimized Kernel: {gp.kernel_}")

        # Make predictions with confidence intervals
        mean_pred, std_pred = gp.predict(X_normalized, return_std=True)  # type: ignore

        # Denormalize
        mean_pred = mean_pred * y_std + y_mean
        std_pred = std_pred * y_std  # type: ignore

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
