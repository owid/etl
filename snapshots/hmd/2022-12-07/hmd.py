"""Get HMD full dataset into Snapshots.

The file needs to be manually downloaded first, as it requires user registration. Instructions:
    - Visit https://www.mortality.org/ and register
    - Visit https://www.mortality.org/Data/ZippedDataFiles
    - Download the complete dataset by clicking on the button "All HMD statistics" (note that the wording or web layout may have changed)
    - Obtain the publication date in the section "Previous Versions" or hardcoded in the name of the downloaded file.
"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

SNAPSHOT_VERSION = "2022-12-07"


@click.command()
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(path_to_file: str, upload: bool) -> None:
    # Create new snapshot.
    snap = Snapshot(f"hmd/{SNAPSHOT_VERSION}/hmd.zip")
    # Ensure destination folder exists.
    snap.path.parent.mkdir(exist_ok=True, parents=True)
    # Copy local data file to snapshots data folder.
    snap.path.write_bytes(Path(path_to_file).read_bytes())
    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
