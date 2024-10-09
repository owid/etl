# NOTE: Just as a temporary solution for this release, I am adding the regions to the key file via this code.
# The extraction code originally removed the region_name column from the file, so I am adding it back here.
# The pip_api.py code is already updated to include the region_name column in the key file, but we would need to re-run thousands of queries to get the data again.


from pathlib import Path

import pandas as pd

# Set path of this script
PARENT_DIR = Path(__file__).parent

# # Load the key file.
df_key = pd.read_csv(f"{PARENT_DIR}/world_bank_pip.csv")

# Get data from the most common query
df_country = pd.read_csv(
    "https://api.worldbank.org/pip/v1/pip?povline=2.15&country=all&year=all&fill_gaps=false&welfare_type=all&reporting_level=all&ppp_version=2017&version=20240627_2017_01_02_PROD&release_version=20240627&format=csv"
)

# Rename country_name and reporting_year columns
df_country = df_country.rename(columns={"country_name": "country", "reporting_year": "year"})

# Merge the data with the key file
df_key = pd.merge(
    df_key,
    df_country[["country", "year", "reporting_level", "welfare_type", "region_name"]],
    on=["country", "year", "reporting_level", "welfare_type"],
    how="left",
)

# Save the key file with the region_name column
df_key.to_csv(f"{PARENT_DIR}/world_bank_pip_with_regions.csv", index=False)
