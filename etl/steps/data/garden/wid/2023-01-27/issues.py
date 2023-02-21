# %% [markdown]
# # Checks to WID data
# The data here is harmonized to the country and year format Our World in Data uses

import pandas as pd

# %%
from owid.catalog import Dataset

from etl.paths import DATA_DIR

# # To show the entire output of the dataframess
# pd.set_option("display.max_rows", None)

# Variable type suffixes
vars = ["pretax", "posttax_dis", "posttax_nat", "wealth"]

# Decile prefixes to evaluate
deciles = ["p0p10", "p10p20", "p20p30", "p30p40", "p40p50", "p50p60", "p60p70", "p70p80", "p80p90", "p90p100"]

# Metrics to evaluate
metric = ["avg", "thr", "share"]

ds = Dataset(DATA_DIR / "garden" / "wid" / "2023-01-27" / "world_inequality_database")
print(ds.table_names)

df = ds["world_inequality_database"]
df = pd.DataFrame(df)

# %% [markdown]
# ## Check Gini values

# %%
# Check Gini values
# For each type of income/wealth
for v in vars:
    # Define column
    col = f"p0p100_gini_{v}"
    # Filter only ginis lower than 0 or higher than 1
    df_review = df[(df[col] > 1) | (df[col] < 0)]

    # Print no issues if no values have been found
    if len(df_review) == 0:
        print(f"No issues for {v}")

    # Print the observations with issues if values are found
    else:
        print(f"There are {len(df_review)} observations with issues in {v}:")
        print(df_review[[col]])

# %% [markdown]
# ## Check sum of shares

# %%
# CHECK SUM OF SHARES

# For each income/wealth variable
for v in vars:
    # Set columns to evaluate
    cols = [f"{s}_share_{v}" for s in deciles]
    # Get sum of shares
    df["sum"] = df[cols].sum(1)
    # Count the nulls between the 10 decile share variables
    df["null_check"] = df[cols].isnull().sum(1)

    # Exclude sum of shares = 0
    df_review = df[df["sum"] != 0]
    # Only keep shares with no null values
    df_review = df_review[df_review["null_check"] == 0]
    # Look for unusual sum values
    df_review = df_review[(df_review["sum"] > 100.2) | (df_review["sum"] < 99.8)]

    # Print no issues if nothing is found
    if len(df_review) == 0:
        print(f"No issues for {v}")

    # Print the observations with issues otherwise
    else:
        print(f"There are {len(df_review)} observations with issues in {v}:")
        print(df_review["sum"])

# %% [markdown]
# ## Check unusual share values

# %%
# CHECK UNUSUAL SHARE VALUES

# Get all the share variables
share_vars = list(df.filter(like="share"))

# For each share variable
for sh in share_vars:

    # Filter values below 0 and over 100
    df_review = df[(df[sh] > 100) | (df[sh] < 0)]

    # Print no issues if there's nothing found
    if len(df_review) == 0:
        print(f"No issues for {sh}")

    # Print the observations with issues otherwise
    else:
        print(f"There are {len(df_review)} observations with issues in {sh}:")
        print(df_review[[sh]])

# %% [markdown]
# ## Check unusal share, average and threshold values

# %%
# CHECK UNUSUAL SHARE, AVG, THR VALUES

for m in metric:

    # Get all the variables
    metric_vars = list(df.filter(like=m))

    # For each of the metric variables
    for mv in metric_vars:

        # Filter values below 0 and over 100
        df_review = df[df[mv] < 0]

        # Print no issues if there's nothing found
        if len(df_review) == 0:
            print(f"No issues for {mv}")

        # Print the observations with issues otherwise
        else:
            print(f"There are {len(df_review)} observations with issues in {mv}:")
            print(df_review[[mv]])

# %% [markdown]
# ## Check monotonicity

# %%
# CHECK MONOTONICITY

# Start with an empty monotonicity check variable list
check_vars = []

for v in vars:
    for m in metric:
        for i in range(len(deciles)):

            if i <= 8:
                col1 = f"{deciles[i]}_{m}_{v}"
                col2 = f"{deciles[i+1]}_{m}_{v}"

                col_check = f"monotonicity_check_{i}"
                check_vars.append(col_check)
                df[col_check] = df[col2] >= df[col1]

        df["monotonicity_check"] = df[check_vars].all(1)

        cols = [f"{s}_{m}_{v}" for s in deciles]

        # Count the nulls between the 10 decile share variables
        df["null_check"] = df[cols].isnull().sum(1)

        df_review = df[~df["monotonicity_check"]]

        df_review = df_review[df_review["null_check"] == 0]

        # Print no issues if there's nothing found
        if len(df_review) == 0:
            print(f"No issues for {m}, {v}")

        # Print the observations with issues otherwise
        else:
            print(f"There are {len(df_review)} observations with issues in {m}, {v}:")
            print(df_review.index)

# %%
