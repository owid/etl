import numpy as np
import pandas as pd


def remove_sparse_timeseries(df: pd.DataFrame, cols: list[str], min_data_points: int) -> pd.DataFrame:
    """
    For each country go through each column, if there are {min_data_points} or fewer values which are non-zero and non-NA then the whole time-series is set to NA.
    For example if min_data_points = 5, then any country-column with fewer than 5 datapoints will be set to NA.
    """
    countries = df["country"].drop_duplicates()

    for country in countries:
        for col in cols:
            if df.loc[(df["country"] == country), col].fillna(0).astype(bool).sum() <= min_data_points:
                df.loc[(df["country"] == country), col] = np.NaN

    return df
