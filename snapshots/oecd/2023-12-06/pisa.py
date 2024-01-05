"""Script to create a snapshot of dataset 'PISA Database' on educational attainment from OECD.

Data is available from:  https://pisadataexplorer.oecd.org/ide/idepisa/variables.aspx

Steps to download:


    - Select the following parameters:
        - Subject: Reading, Mathematics, Science
        - Overall score: Overall Reading , Overall Mathematics, Overall Science
        - Group: International, OECD, Partners (select all countries and regions)
        - Grouping variable: All students, Student Standardized Gender (select both)
        - Need to download each excel file separately for each subject and student group (as of Dec 2023 donwloading all in one file didn't work)

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
    "--path-to-file-pisa_math_all", prompt=True, type=str, help="Path to local data file - math scores, both sexes."
)
@click.option(
    "--path-to-file-pisa_science_all",
    prompt=True,
    type=str,
    help="Path to local data file - science scores, both sexes.",
)
@click.option(
    "--path-to-file-pisa_reading_all",
    prompt=True,
    type=str,
    help="Path to local data file - reading scores, both sexes.",
)
@click.option(
    "--path-to-file-pisa_math_boys_girls",
    prompt=True,
    type=str,
    help="Path to local data file - math scores, both sexes.",
)
@click.option(
    "--path-to-file-pisa_science_boys_girls",
    prompt=True,
    type=str,
    help="Path to local data file - science scores, both sexes.",
)
@click.option(
    "--path-to-file-pisa_reading_boys_girls",
    prompt=True,
    type=str,
    help="Path to local data file - reading scores, both sexes.",
)
def main(
    path_to_file_pisa_math_all: str,
    path_to_file_pisa_science_all: str,
    path_to_file_pisa_reading_all: str,
    path_to_file_pisa_math_boys_girls: str,
    path_to_file_pisa_science_boys_girls: str,
    path_to_file_pisa_reading_boys_girls: str,
    upload: bool,
) -> None:
    snapshot_paths = [
        f"oecd/{SNAPSHOT_VERSION}/pisa_math_all.xls",
        f"oecd/{SNAPSHOT_VERSION}/pisa_science_all.xls",
        f"oecd/{SNAPSHOT_VERSION}/pisa_reading_all.xls",
        f"oecd/{SNAPSHOT_VERSION}/pisa_math_boys_girls.xls",
        f"oecd/{SNAPSHOT_VERSION}/pisa_science_boys_girls.xls",
        f"oecd/{SNAPSHOT_VERSION}/pisa_reading_boys_girls.xls",
    ]

    path_to_files = [
        path_to_file_pisa_math_all,
        path_to_file_pisa_science_all,
        path_to_file_pisa_reading_all,
        path_to_file_pisa_math_boys_girls,
        path_to_file_pisa_science_boys_girls,
        path_to_file_pisa_reading_boys_girls,
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
