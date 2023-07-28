"""Script to create a snapshot of dataset 'Country Activity Tracker: Artificial Intelligence (Center for Security and Emerging Technology, 2023)'."""

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
    snap = Snapshot(f"artificial_intelligence/{SNAPSHOT_VERSION}/cset.csv")
    common_path = "/Users/veronikasamborska/Downloads/owid_cat_data_20230717/"

    files = {
        "companies": ["companies_yearly_disclosed.csv", "companies_yearly_estimated.csv"],
        "patents": ["patents_yearly_applications.csv", "patents_yearly_granted.csv"],
        "articles": ["publications_yearly_articles.csv", "publications_yearly_citations.csv"],
    }

    all_dfs = []
    for field, file_ids in files.items():
        all_dfs.append(read_and_clean_data(file_ids, common_path, field))

    result = pd.concat(all_dfs)
    df_to_file(result, file_path=snap.path)
    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


def read_and_clean_data(file_ids, common_path, field_name):
    all_dfs_list = []
    for id in file_ids:
        df_add = pd.read_csv(common_path + id)
        if "estimated" in id:
            df_add.rename(columns={"disclosed_investment": "disclosed_investment_estimated"}, inplace=True)
        all_dfs_list.append(df_add)

    merged_df = all_dfs_list[0]
    for df in all_dfs_list[1:]:
        merged_df = pd.merge(merged_df, df, on=["year", "country", "field"])

    merged_df.rename(columns={"field": field_name}, inplace=True)
    return merged_df


if __name__ == "__main__":
    main()
