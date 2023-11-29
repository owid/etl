"""
Script to create a snapshot of dataset 'Trust questions in the Latinobarómetro Dataset (Latinobarómetro, 2021)'.

INSTRUCTIONS

	1. Go to the Latinobarómetro portal, https://www.latinobarometro.org/, to the Data (Datos) tab, and download all the survey files in the Stata column.
	2. Extract the dta files from the zip files and keep them in the same folder.
	3. Run latinobarometro.do in Stata from the same folder as the datasets.
	4. It generates a csv file, latinobarometro_trust.csv. Copy it to this folder.
	5. Add snapshot. The command is:
 		python snapshots/survey/{version}/latinobarometro_trust.py --path-to-file snapshots/survey/{version}/latinobarometro_trust.csv
	6. Delete csv file
	7. Run `etl latinobarometro_trust`

*/

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
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def main(path_to_file: str, upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"survey/{SNAPSHOT_VERSION}/latinobarometro_trust.csv")

    # Ensure destination folder exists.
    snap.path.parent.mkdir(exist_ok=True, parents=True)

    # Copy local data file to snapshots data folder.
    snap.path.write_bytes(Path(path_to_file).read_bytes())

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
