"""Script to access and download seversal CSVs, concatenate them and save as one snapshot of dataset 'Projections of Educational Attainment (Lee and Lee, 2015)'."""

from pathlib import Path

import click
import pandas as pd
from owid.datautils.io import df_to_file

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"education/{SNAPSHOT_VERSION}/education_barro_lee_projections.csv")
    all_dfs = get_data()
    df_to_file(all_dfs, file_path=snap.path)
    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


def get_data() -> pd.DataFrame:
    """
    Retrieve datasets for various age groups from different CSV files and create a combined dataframe.

    Returns:
    pd.DataFrame: Combined dataframe containing data from multiple sources.
    """
    # Create an empty DataFrame to store the combined data
    all_dfs = pd.DataFrame()

    # URLs to projections dataset files
    urls = [
        "https://barrolee.github.io/BarroLeeDataSet/OUP/OUP_proj_MF1564_v1.csv",
        "https://barrolee.github.io/BarroLeeDataSet/OUP/OUP_proj_F1564_v1.csv",
        "https://barrolee.github.io/BarroLeeDataSet/OUP/OUP_proj_M1564_v1.csv",
        "https://barrolee.github.io/BarroLeeDataSet/OUP/OUP_proj_MF1524_v1.csv",
        "https://barrolee.github.io/BarroLeeDataSet/OUP/OUP_proj_F1524_v1.csv",
        "https://barrolee.github.io/BarroLeeDataSet/OUP/OUP_proj_M1524_v1.csv",
        "https://barrolee.github.io/BarroLeeDataSet/OUP/OUP_proj_MF2564_v1.csv",
        "https://barrolee.github.io/BarroLeeDataSet/OUP/OUP_proj_F2564_v1.csv",
        "https://barrolee.github.io/BarroLeeDataSet/OUP/OUP_proj_M2564_v1.csv",
    ]

    for i, url in enumerate(urls):
        # Read data from each URL
        df_add = pd.read_csv(url)

        # Concatenate the data to the main dataframe
        all_dfs = pd.concat([all_dfs, df_add])

    return all_dfs


if __name__ == "__main__":
    main()
