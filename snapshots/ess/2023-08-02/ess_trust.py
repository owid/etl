"""
Script to create a snapshot of dataset 'Trust questions in the European Social Survey dataset (ESS, 2023)'.

NOTE: For now I will keep only rounds >=9, because to make the other work I need to merge the datasets with several other files

INSTRUCTIONS

	1. In the ESS Data Portal, register and download all the survey files from the Data Wizard, https://ess-search.nsd.no/CDW/RoundCountry.
	2. Extract the dta file from the zip file.
	3. Run this ess_trust.do in Stata. If it fails, check the name of the dta file in the first line of the code.
	4. The output is given in Stata's output window. Copy and paste it into a csv file, called `ess_trust.csv`.
	5. Add snapshot. The command is:
 		python snapshots/ess/{version}/ess_trust.py --path-to-file snapshots/ess/{version}/ess_trust.csv
	6. Delete csv file
	7. Run `etl ess_trust`

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
    snap = Snapshot(f"ess/{SNAPSHOT_VERSION}/ess_trust.csv")

    # Ensure destination folder exists.
    snap.path.parent.mkdir(exist_ok=True, parents=True)

    # Copy local data file to snapshots data folder.
    snap.path.write_bytes(Path(path_to_file).read_bytes())

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
