"""
EXTRACT POVERTY PROJECTIONS FROM FLOURISH VISUALIZATION

This script extract data about poverty projections from Flourish visualizations available in this article: https://blogs.worldbank.org/en/developmenttalk/end-extreme-poverty-getting-back-pre-covid-19-reduction-rates-not-enough

Visualization is available here https://flo.uri.sh/visualisation/11612886/embed?auto=1

I couldn't make the script work, but I extracted the data manually and saved it in the data_dictionary.json file.

Steps:
    1. Update the data_dictionary.json file if necessary.
    2. Run this script to extract the data and save it as a CSV file.
    3. Create a snapshot of the dataset.
        python snapshots/wb/2024-06-26/poverty_projections_share_regions.py --path-to-file snapshots/wb/2024-06-26/poverty_projections_share_regions.csv

Based on https://stackoverflow.com/questions/62031809/extracting-javascript-variables-into-python-dictionaries
"""

import json
from pathlib import Path

import pandas as pd

# Set directory path
PARENT_DIR = Path(__file__).parent.absolute()

# Define country list
with open(PARENT_DIR / "data_dictionary.json", "r") as f:
    DATA_DICT = json.load(f)

# Open dictionary as DataFrame
df_flourish = pd.DataFrame.from_dict(DATA_DICT["data"])

# Split values into columns
values = df_flourish["value"].apply(pd.Series)

# Concatenate DataFrames, horizontally
df = pd.concat([df_flourish, values], axis=1)

# Rename columns and drop value
df = df.rename(
    columns={
        "filter": "region",
        "label": "year",
        0: "estimate",
        1: "projection",
    }
)

df = df.drop(columns=["value"])

df.to_csv(PARENT_DIR / "poverty_projections_share_regions.csv", index=False)
