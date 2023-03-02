import numpy as np
import pandas as pd


def remove_strings_of_zeros(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """
    For each country go through each column, if the sum = 0 then change all the values to NA. This is to
    prevent long strings of 0s showing up in the grapher.
    """
    #
    countries = df["country"].drop_duplicates()

    for country in countries:
        for col in cols:
            if df.loc[(df["country"] == country), col].sum() == 0:
                df.loc[(df["country"] == country), col] = np.NaN

    return df
