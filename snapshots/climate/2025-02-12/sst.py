"""Script to create a snapshot of dataset."""

from io import StringIO
from pathlib import Path

import click
import pandas as pd
import requests
from owid.datautils.io import df_to_file

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
BASE_URL_ONI = "https://www.cpc.ncep.noaa.gov/data/indices/oni.ascii.txt"
BASE_URL = "https://www.cpc.ncep.noaa.gov/data/indices/sstoi.indices"


def season_to_month(season: str) -> int:
    """
    Convert the season string to the corresponding month.

    Parameters:
    season (str): The season string (e.g., "DJF").

    Returns:
    int: The corresponding month (1-12).
    """
    season_to_month_map = {
        "DJF": 2,  # December-January-February -> January
        "JFM": 3,  # January-February-March -> February
        "FMA": 4,  # February-March-April -> March
        "MAM": 5,  # March-April-May -> April
        "AMJ": 6,  # April-May-June -> May
        "MJJ": 7,  # May-June-July -> June
        "JJA": 8,  # June-July-August -> July
        "JAS": 9,  # July-August-September -> August
        "ASO": 10,  # August-September-October -> September
        "SON": 11,  # September-October-November -> October
        "OND": 12,  # October-November-December -> November
        "NDJ": 1,  # November-December-January -> December
    }
    return season_to_month_map[season]


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"climate/{SNAPSHOT_VERSION}/sst.csv")

    dfs = []
    for url in [BASE_URL, BASE_URL_ONI]:
        response = requests.get(url)

        data = response.text

        # Skip header lines and read into a DataFrame
        data_io = StringIO(data)
        df = pd.read_csv(data_io, sep="\s+", skiprows=1, header=None)
        if url == BASE_URL_ONI:
            columns = ["month", "year", "oni", "oni_anomaly"]
            df.columns = columns
            df["month"] = df["month"].apply(season_to_month)
            # Add 1 to the year if the month is January because of the way the data is structured
            df.loc[df["month"] == 1, "year"] += 1
        else:
            # Assign column names
            columns = [
                "year",
                "month",
                "nino1_2",
                "nino1_2_anomaly",
                "nino3",
                "nino3_anomaly",
                "nino4",
                "nino4_anomaly",
                "nino3_4",
                "nino3_4_anomaly",
            ]
            df.columns = columns

        dfs.append(df)
    df = pd.merge(dfs[0], dfs[1], on=["year", "month"], how="outer")
    df_to_file(df, file_path=snap.path)
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
