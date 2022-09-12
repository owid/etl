import numpy as np
import pandas as pd


def create_units(df: pd.DataFrame) -> pd.DataFrame:

    conds = [
        ((df["measure"] == "DALYs (Disability-Adjusted Life Years)") & (df["metric"] == "Rate")),
        ((df["measure"] == "DALYs (Disability-Adjusted Life Years)") & (df["metric"] == "Number")),
        ((df["measure"] == "DALYs (Disability-Adjusted Life Years)") & (df["metric"] == "Percent")),
        ((df["measure"] == "Deaths") & (df["metric"] == "Number")),
        ((df["measure"] == "Deaths") & (df["metric"] == "Rate")),
        ((df["measure"] == "Deaths") & (df["metric"] == "Percent")),
        ((df["measure"] == "Prevalence") & (df["metric"] == "Number")),
        ((df["measure"] == "Prevalence") & (df["metric"] == "Rate")),
        ((df["measure"] == "Prevalence") & (df["metric"] == "Percent")),
        ((df["measure"] == "Incidence") & (df["metric"] == "Number")),
        ((df["measure"] == "Incidence") & (df["metric"] == "Rate")),
        ((df["measure"] == "Incidence") & (df["metric"] == "Percent")),
    ]

    choices = [
        "DALYs per 100,000 people",
        "DALYs",
        "%",
        "deaths",
        "deaths per 100,000 people",
        "%",
        "",
        "",
        "%",
        "",
        "",
        "%",
    ]
    df["metric"] = np.select(conds, choices)
    return df
