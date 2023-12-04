"""
Script to create a snapshot of dataset 'Trust questions in the Afrobarometer Dataset (Afrobarometer, 2023)'.

NOTE: Only Round 8 (2022) is used, because it is the only round that includes the main trust question.

INSTRUCTIONS

	1. In the Afrobarometer Merged data page (https://www.afrobarometer.org/data/merged-data/), download the Merged Round 8 Data (34 countries) (2022) file.
	2. Copy the file to this directory . Though it is a SPSS file, it can be read by Stata.
	3. Runafrobarometer_trust.do in Stata. If it fails, check the name of the dta file in the first line of the code.
	4. The code generates a csv file called afrobarometer_trust.csv. Copy this file to the snapshots/ess/{version} directory.
	5. Add snapshot. The command is:
 		python snapshots/survey/{version}/afrobarometer_trust.py --path-to-file snapshots/survey/{version}/afrobarometer_trust.csv
	6. Delete csv file (and sav file)
	7. Run `etl afrobarometer_trust`

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
    snap = Snapshot(f"survey/{SNAPSHOT_VERSION}/afrobarometer_trust.csv")

    # Ensure destination folder exists.
    snap.path.parent.mkdir(exist_ok=True, parents=True)

    # Copy local data file to snapshots data folder.
    snap.path.write_bytes(Path(path_to_file).read_bytes())

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
