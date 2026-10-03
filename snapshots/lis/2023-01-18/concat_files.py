"""
This code concatenates the equivalized and per capita versions of the LIS files, to deal with less snapshots.
The files do not come concatenated by default due to constraints in the LISSY platform
LIS files are currently uploaded locally. Please see instructions in `lissy_ineq_pov.do`
"""

from pathlib import Path

import pandas as pd

aggregation_list = ["equivalized", "pc"]
file_list = ["keyvars", "abs_poverty", "distribution"]

DIRECTORY = Path(__file__).parent

for file in file_list:
    df_equivalized = pd.read_csv(f"{DIRECTORY}/lis_{file}_equivalized.csv")
    df_pc = pd.read_csv(f"{DIRECTORY}/lis_{file}_pc.csv")

    df = pd.concat([df_equivalized, df_pc], ignore_index=True)
    df.to_csv(f"{DIRECTORY}/lis_{file}.csv", index=False)
