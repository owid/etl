"""Script to create a snapshot of the Integrated Values Survey dataset.

NOTE: For now I am extracting data on trust, but this will be expanded in 2024 to include other variables.

INSTRUCTIONS

	1.	Follow the instructions to construct the IVS file from WVS and EVS microdata here: https://www.worldvaluessurvey.org/WVSEVStrend.jsp
			The files required are the WVS and EVS trend files, and the merge syntax file (in our case in Stata). Keep these files in the same folder.
	2.	Run the EVS_WVS_Merge_Syntax_stata 4.do file in Stata. This will generate the IVS main dataset.
	3.	Run ivs_create_file.do in Stata. It will generate the file ivs.csv
	4.	Add snapshot. Currently the command is
 			python snapshots/ivs/{date}/integrated_values_survey.py --path-to-file snapshots/ivs/{date}/ivs.csv
	5.	Delete csv file
	6.	Run `etl wvs_trust`
"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def main(path_to_file: str, upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"ivs/{SNAPSHOT_VERSION}/integrated_values_survey.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
