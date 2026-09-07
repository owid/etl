"""Script to create a snapshot of dataset.

The Global Cancer Observatory tool "Cancers Attributable to Infections" loads three JSON files: estimates by
infectious agent (for all cancers combined), by cancer site (for all agents combined), and totals. This script
fetches the three files and stacks them into one CSV, keeping the source's own labels and column names.
"""

from pathlib import Path

import pandas as pd
import requests
from owid.datautils.io import df_to_file

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# The estimates apply attributable fractions to GLOBOCAN cancer incidence estimates for this year.
YEAR = 2020
# Labels used by the source for the totals.
ALL_AGENTS = "All infectious agents"
ALL_CANCERS = "All cancers but non-melanoma skin cancer (C00-97, but C44)"


def run(upload: bool = True) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"cancer/{SNAPSHOT_VERSION}/gco_infections.csv")
    base_url = snap.metadata.origin.url_download

    dataframes = []
    for json_file in ["by-agent.json", "by-cancers.json", "all.json"]:
        response = requests.get(base_url + json_file, timeout=120)
        response.raise_for_status()
        df = pd.DataFrame(response.json())
        df["year"] = YEAR

        if json_file == "by-agent.json":
            df["cancer"] = ALL_CANCERS
        elif json_file == "by-cancers.json":
            df["agent"] = ALL_AGENTS
        elif json_file == "all.json":
            df = df.rename(columns={"site": "cancer"})
        dataframes.append(df)
    df_all = pd.concat(dataframes, ignore_index=True)

    # Write the combined file, add it to DVC and upload it to R2.
    df_to_file(df_all, file_path=snap.path)
    snap.dvc_add(upload=upload)
