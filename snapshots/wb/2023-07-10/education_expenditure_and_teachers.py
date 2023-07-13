"""Script to create a snapshot of dataset 'World Bank Education Statistics: Learning Outcomes (2018)'."""

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
    """
    Instructions:
    1. Visit the following URL:  https://databank.worldbank.org/reports.aspx?source=Education%20Statistics#
    2. Select the following indicators:
        'Percentage of qualified teachers in pre-primary education, both sexes (%)'
        'Proportion of teachers with the minimum required qualifications in pre-primary education, both sexes (%)'
        'Pupil-trained teacher ratio in pre-primary education (headcount basis)'
        'Pupil-qualified teacher ratio in pre-primary education (headcount basis)'
        'Percentage of qualified teachers in primary education, both sexes (%)'
        'Proportion of teachers with the minimum required qualifications in primary education, both sexes (%)'
        'Pupil-qualified teacher ratio in primary education (headcount basis)'
        'Pupil-trained teacher ratio in primary education (headcount basis)'
        'Percentage of qualified teachers in secondary education, both sexes (%)'
        'Proportion of teachers with the minimum required qualifications in secondary education, both sexes (%)'
        'Pupil-qualified teacher ratio in secondary (headcount basis)'
        'Pupil-trained teacher ratio in secondary education (headcount basis)'
        'Annual statutory teacher salaries in public institutions in USD. Upper Secondary. Starting salary'
        'Annual statutory teacher salaries in public institutions in USD. Primary. Starting salary'
        'Annual statutory teacher salaries in public institutions in USD. Pre-Primary. Starting salary'
        'Annual statutory teacher salaries in public institutions in USD. Lower Secondary. Starting salary'
        'Expenditure on education as % of total government expenditure (%)'
        'Government expenditure on education as % of GDP (%)'
        'Government expenditure on pre-primary education as % of GDP (%)'
        'Government expenditure on primary education as % of GDP (%)'
        'Government expenditure on secondary education as % of GDP (%)'
        'Government expenditure on tertiary education as % of GDP (%)'
        'Volume of official development assistance flows for scholarships by sector and type of study, constant US$
    3. In the top right corner, click on the "Download" button.
    4. Choose the "CSV" format and initiate the download.

    Note: Ensure that the downloaded dataset contains the desired PISA scores and associated information.
    """
    # Create a new snapshot.
    snap = Snapshot(f"wb/{SNAPSHOT_VERSION}/education_expenditure_and_teachers.csv")

    # Ensure destination folder exists.
    snap.path.parent.mkdir(exist_ok=True, parents=True)

    # Copy local data file to snapshots data folder.
    snap.path.write_bytes(Path(path_to_file).read_bytes())

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
