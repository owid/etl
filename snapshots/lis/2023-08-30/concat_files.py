"""
This code concatenates the equivalized and per capita versions of the LIS files, to deal with less snapshots.
The files do not come concatenated by default due to constraints in the LISSY platform
LIS files are currently uploaded locally. Please see instructions in `lissy_ineq_pov.do`
"""

from pathlib import Path

import pandas as pd

file_list = ["keyvars", "abs_poverty", "distribution"]
age_list = ["", "_adults"]

NUMBER_OF_PERCENTILE_FILES = 5

DIRECTORY = Path(__file__).parent

# Concatenate main files
for file in file_list:
    for age in age_list:
        df_equivalized = pd.read_csv(f"{DIRECTORY}/lis_{file}_equivalized{age}.csv")
        df_pc = pd.read_csv(f"{DIRECTORY}/lis_{file}_pc{age}.csv")

        df = pd.concat([df_equivalized, df_pc], ignore_index=True)
        df.to_csv(f"{DIRECTORY}/lis_{file}{age}.csv", index=False)

# Concatenate percentile files
for age in age_list:
    # Initialize empty dataframes
    df_equivalized = pd.DataFrame()
    df_pc = pd.DataFrame()
    for i in range(1, NUMBER_OF_PERCENTILE_FILES + 1):
        df_e = pd.read_csv(f"{DIRECTORY}/lis_percentiles_equivalized{age}_{i}.csv")
        df_p = pd.read_csv(f"{DIRECTORY}/lis_percentiles_pc{age}_{i}.csv")

        df_equivalized = pd.concat([df_equivalized, df_e], ignore_index=True)
        df_pc = pd.concat([df_pc, df_p], ignore_index=True)

    df = pd.concat([df_equivalized, df_pc], ignore_index=True)
    df.to_csv(f"{DIRECTORY}/lis_percentiles{age}.csv", index=False)
