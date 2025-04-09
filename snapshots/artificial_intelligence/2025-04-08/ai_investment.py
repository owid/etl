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
    # IDs of the download files (in Google Drive)
    ids = [
        "17kxPQJBOzvy_HVBdOYSC2qhbHBinYim8",  # 4.3.1 corporate
        "1lxejWieSsIHrZeVGHk3Dq6Phpt5RXOfT",  # 4.3.3 generative
        "1zjuHQqw65KSJ8mMYjbk28x2c5_5OGVcJ",  # 4.3.4. companies
        "1dqG01vBBdK2Nv2qmPCBXpgYdV0UYLxUU",  # 4.3.14. companies by geographic region
        "13U0Ok9zyQ0xyUqPO7jJFrhPmQnpI0PKI",  # 4.3.10 total private investment by geographic region
        "1JS6vtYQfTR2w1wbDxFT5KGWcxeCBxUkZ",  # 4.3.16 investment by focus area - World
    ]

    df_list = []

    try:
        column_renames = {
            "Geographic Area": "Geographic area",
            "Focus area": "Investment activity",
            "Investment type": "Investment activity",
            "Investment Type": "Investment activity",
        }

        value_column_mappings = {
            "Total investment (in billions of US dollars)": "Total investment (in billions of U.S. dollars)",
            "Total investment (in billions of U.S. dollars)": "Total investment (in billions of U.S. dollars)",
            "Number of companies": "Number of newly funded AI companies",
        }

        for i, drive_id in enumerate(ids):
            df_add = pr.read_csv(common_path + drive_id)

            # Standardize column names
            df_add = df_add.rename(columns={k: v for k, v in column_renames.items() if k in df_add.columns})

            # Fill missing 'Investment activity' based on file index
            if "Investment activity" not in df_add.columns:
                investment_mapping = {1: "Generative AI", 2: "Companies", 3: "Companies", 4: "Private Investment"}
                df_add["Investment activity"] = investment_mapping.get(i, "Unknown")

            # Ensure 'Geographic area' exists
            if "Geographic area" not in df_add.columns:
                df_add["Geographic area"] = "World"

            # Rename value column and assign variable name
            for original_col, variable_name in value_column_mappings.items():
                if original_col in df_add.columns:
                    df_add["variable_name"] = variable_name
                    df_add = df_add.rename(columns={original_col: "value"})
                    break  # Only one match expected per file

            df_list.append(df_add)

            # Concatenate the DataFrames from the list
            all_dfs = pr.concat(df_list)

        return all_dfs
    except Exception as e:
        raise IOError("Error in fetching or concatenating the data: " + str(e))


if __name__ == "__main__":
    main()
