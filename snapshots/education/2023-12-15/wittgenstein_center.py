"""Script to create a snapshot of dataset.

The data was obtained with permission from Samir KS kcsamir@gmail.com. The data is not publicly available yet as it's not the final version.
Only two csv files are imported here:
    1. Data containing total population split by country, age groups, educational level and sex. The original file name was PROJresult_AGE_SSP2_V12.csv indicating that it's the SSP2 scenario and version 12 of the data.
    2. Dictionary containing the codes for the countries, educational level and the age groups.

This dataset will be used to calculate the global projections for the total share of population with some or no formal education.
"""

from pathlib import Path

import click

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
@click.option(
    "--path-to-file-wittgenstein-center-data", prompt=True, type=str, help="Path to local data file with the data"
)
@click.option(
    "--path-to-file-wittgenstein-center-dictionary",
    prompt=True,
    type=str,
    help="Path to local data file - dictionary with values for the data",
)
def main(
    path_to_file_wittgenstein_center_data: str,
    path_to_file_wittgenstein_center_dictionary: str,
    upload: bool,
) -> None:
    snapshot_paths = [
        f"education/{SNAPSHOT_VERSION}/wittgenstein_center_data.csv",
        f"education/{SNAPSHOT_VERSION}/wittgenstein_center_dictionary.csv",
    ]

    path_to_files = [
        path_to_file_wittgenstein_center_data,
        path_to_file_wittgenstein_center_dictionary,
    ]
    for meta_path, file_path in zip(snapshot_paths, path_to_files):
        snap = Snapshot(meta_path)
        # Ensure destination folder exists.
        snap.path.parent.mkdir(exist_ok=True, parents=True)
        # Copy local data file to snapshots data folder.
        snap.path.write_bytes(Path(file_path).read_bytes())
        # Add file to DVC and upload to S3.
        snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
