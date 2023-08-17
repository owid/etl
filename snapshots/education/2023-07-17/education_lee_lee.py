"""Script to access and download seversal CSVs, concatenate them and save as one snapshot of dataset 'Human Capital in the Long Run (Lee and Lee, 2016)'."""

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
    snap = Snapshot(f"education/{SNAPSHOT_VERSION}/education_lee_lee.xlsx")
    all_dfs = get_data()
    df_to_file(all_dfs, file_path=snap.path)
    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


def get_data() -> pd.DataFrame:
    """
    Retrieve datasets for various age groups from different URLs and create a combined dataframe.

    Returns:
    pd.DataFrame: Combined dataframe containing data from multiple sources.
    """
    # Enrollment Data
    all_dfs_enrol = pd.DataFrame()
    urls_enrol = [
        "https://barrolee.github.io/BarroLeeDataSet/LeeLee/LeeLee_enroll_MF.xls",
        "https://barrolee.github.io/BarroLeeDataSet/LeeLee/LeeLee_enroll_F.xls",
        "https://barrolee.github.io/BarroLeeDataSet/LeeLee/LeeLee_enroll_M.xls",
    ]
    sex_column = ["MF", "F", "M", "MF", "F", "M", "MF", "F", "M"]

    for i, url in enumerate(urls_enrol):
        # Read data from each URL
        df_add = pd.read_excel(url, header=7)

        # Drop rows where all columns are NaN
        df_add.dropna(how="all", inplace=True)

        # Fill NaN values in 'Country' column with previous non-NaN value
        df_add["Country"].fillna(method="ffill", inplace=True)
        df_add["Sex"] = sex_column[i]
        df_add["age_group"] = "not specified"
        # Concatenate the data to the main dataframe
        all_dfs_enrol = pd.concat([all_dfs_enrol, df_add])

    # Attainment Data
    urls_attainment = [
        "https://barrolee.github.io/BarroLeeDataSet/LeeLee/LeeLee_attain_MF1564.xls",
        "https://barrolee.github.io/BarroLeeDataSet/LeeLee/LeeLee_attain_F1564.xls",
        "https://barrolee.github.io/BarroLeeDataSet/LeeLee/LeeLee_attain_M1564.xls",
        "https://barrolee.github.io/BarroLeeDataSet/LeeLee/LeeLee_attain_MF1524.xls",
        "https://barrolee.github.io/BarroLeeDataSet/LeeLee/LeeLee_attain_F1524.xls",
        "https://barrolee.github.io/BarroLeeDataSet/LeeLee/LeeLee_attain_M1524.xls",
        "https://barrolee.github.io/BarroLeeDataSet/LeeLee/LeeLee_attain_MF2564.xls",
        "https://barrolee.github.io/BarroLeeDataSet/LeeLee/LeeLee_attain_F2564.xls",
        "https://barrolee.github.io/BarroLeeDataSet/LeeLee/LeeLee_attain_M2564.xls",
    ]
    all_dfs_attainment = pd.DataFrame()
    for i, url in enumerate(urls_attainment):
        # Read data from each URL
        df_add = pd.read_excel(url, header=7)

        # Drop rows where all columns are NaN
        df_add.dropna(how="all", inplace=True)

        # Fill NaN values in 'Country' column with previous non-NaN value
        df_add["Country"].fillna(method="ffill", inplace=True)

        # Replace column names with meaningful names
        columns_to_replace = {
            "Highest level attained": "Primary, total",
            "Unnamed: 6": "Primary, completed",
            "Unnamed: 7": "Secondary, total",
            "Unnamed: 8": "Secondary, completed",
            "Unnamed: 9": "Tertiary, total",
            "Unnamed: 10": "Tertiary, completed",
        }
        df_add.rename(columns=columns_to_replace, inplace=True)

        # Remove unnecessary rows and reset index
        df_add = df_add.iloc[3:].reset_index(drop=True)

        # Rename columns
        df_add.rename(columns={"Age Group": "starting_age", "Unnamed: 3": "finishing_age"}, inplace=True)
        df_add["age_group"] = df_add["starting_age"].astype(str) + "-" + df_add["finishing_age"].astype(str)
        df_add.drop(["starting_age", "finishing_age"], axis=1, inplace=True)

        # Remove columns with NaN in column name
        df_add.columns = [col for col in df_add.columns if str(col) != "nan"]

        # Remove 'nan' string from column names
        df_add.columns = [col.replace("nan", "") for col in df_add.columns]

        df_add["Sex"] = sex_column[i]

        # Concatenate the data to the main dataframe
        all_dfs_attainment = pd.concat([all_dfs_attainment, df_add])

    # Merge Enrollment and Attainment Data based on common columns
    df_merged = pd.merge(
        all_dfs_attainment,
        all_dfs_enrol,
        on=["Year", "Country", "Region", "Sex", "age_group"],
        how="outer",
    )

    return df_merged


if __name__ == "__main__":
    main()
