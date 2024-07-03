"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import owid.catalog.processing as pr
from owid.datautils.io import df_to_file

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"artificial_intelligence/{SNAPSHOT_VERSION}/ai_investment.csv")
    all_dfs = get_data()
    df_to_file(all_dfs, file_path=snap.path)
    # Download data from source, add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


def get_data():
    """
    Fetches data from a Google Drive folder for each provided Google Drive ID and concatenates the data into a single DataFrame.

    Returns:
        pd.DataFrame: Concatenated DataFrame containing the fetched data.

    Raises:
        IOError: If there is an error in fetching or concatenating the data.

    """
    common_path = "https://drive.google.com/uc?export=download&id="
    # IDs of the download files (in Google Drive) (Investment by focus area 1 - by country and 2 - World)
    ids = [
        "13iuXuFMyn7BYjZ83l7NpVpPT5LDjM9xl",  # 4.3.1 corporate
        "18g2Li2wbimD2Xb08p8aOPsHEuD43LqpJ",  # 4.3.3 generative
        "1ujGe-xe6e1ZHPAHmYT6rURfBoSHLC0-R",  # 4.3.4. companies
        "14QguByXKnKMPx1amnILPwqoP8DE7PAkq",  # 4.3.14. companies by geographic region
        "1kCOsxJqKgBO-nSe0ApW5JbRvCi_DryLf",  # 4.3.10 total private investment by geographic region
        "1hrEMTrCyl6CcWrhgipNo0zboZUinLR8P",  # 4.3.16 investment by focus area - World
        "1W12tO7AxcSQkJMfPg4MsVz_QqlloCqGO",  # 4.3.17 investment by focus area and region
    ]
    df_list = []

    try:
        # Fetch data from Google Drive and store in a list of DataFrames
        for i, drive_id in enumerate(ids):
            df_add = pr.read_csv(common_path + drive_id)
            if "Investment activity" not in df_add.columns:
                if i == 1:
                    df_add["Investment activity"] = "Generative AI"
                elif i == 2 or i == 3:
                    df_add["Investment activity"] = "Companies"
                elif i == 4:
                    df_add["Investment activity"] = "Private Investment"
            if "Geographic Area" in df_add.columns:
                df_add = df_add.rename(columns={"Geographic Area": "Geographic area"})
            if "Geographic area" not in df_add.columns:
                df_add["Geographic area"] = "World"
            if "Focus area" in df_add.columns:
                df_add = df_add.rename(columns={"Focus area": "Investment activity"})

            df_add = df_add.rename(columns={"Investment activity": "Investment type"})
            if "Total investment (in billions of U.S. dollars)" in df_add.columns:
                df_add["variable_name"] = "Total investment (in billions of U.S. dollars)"
                df_add = df_add.rename(columns={"Total investment (in billions of U.S. dollars)": "value"})
            elif "Number of newly funded AI companies in the world" in df_add.columns:
                df_add["variable_name"] = "Number of newly funded AI companies"
                df_add = df_add.rename(columns={"Number of newly funded AI companies in the world": "value"})
            elif "Number of newly funded AI companies" in df_add.columns:
                df_add["variable_name"] = "Number of newly funded AI companies"
                df_add = df_add.rename(columns={"Number of newly funded AI companies": "value"})
            df_list.append(df_add)

        # Concatenate the DataFrames from the list
        all_dfs = pr.concat(df_list)

        return all_dfs
    except Exception as e:
        raise IOError("Error in fetching or concatenating the data: " + str(e))


if __name__ == "__main__":
    main()
