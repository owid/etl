"""Script to create a snapshot of dataset."""

import io
import zipfile
from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

BASE_GITHUB_URL = "https://raw.githubusercontent.com/owid/notebooks/024e43a8141fb948c005acd2e5157b7d4508ae31/HannahRitchie/ipcc-scenarios/inputs/"


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(upload: bool) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"emissions/{SNAPSHOT_VERSION}/ipcc_scenarios.zip")

    # Helper function to load CSV files
    def load_csv(filename: str) -> pd.DataFrame:
        return pd.read_csv(BASE_GITHUB_URL + filename)

    # Define files to load
    files = {
        "annotations": "annotations.csv",
        "ipcc": "ipcc.csv",
        "raw": "raw.csv",
        "scenario_naming": "scenario_naming.csv",
        "variable_naming": "variable_naming.csv",
    }

    # Load all files
    dataframes = {name: load_csv(filename) for name, filename in files.items()}

    # Add files to ZIP file
    zip_path = snap.path
    zip_path.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "w") as zipf:
        for name, df in dataframes.items():
            # Write DataFrame to in-memory buffer
            buffer = io.StringIO()
            df.to_csv(buffer, index=False)

            # Add buffer content to ZIP file
            zipf.writestr(f"{name}.csv", buffer.getvalue())

    # Save snapshot.
    snap.create_snapshot(filename=zip_path, upload=upload)


if __name__ == "__main__":
    run()
