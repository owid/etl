"""Script to create a snapshot of dataset.
To download the csv file go to https://gco.iarc.fr/overtime/en/dataviz/trends?populations=752_32_36_40_48_112_124_152_156_170_188_191_196_203_208_218_233_246_250_276_352_356_372_380_376_392_414_428_410_440_470_474_528_554_578_608_616_630_634_705_724_756_764_792_800_840_8260_8261_8262_8263_8401_8402&sexes=2&types=0&multiple_populations=1&group_populations=0&cancers=16
and click on the Downloads button --> csv.
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
    snap = Snapshot(f"cancer/{SNAPSHOT_VERSION}/gco_cancer_over_time_cervical.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
