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
    1. Visit the following URL: https://databank.worldbank.org/reports.aspx?source=Education%20Statistics#
    2. Select the following indicators:
        'Human Capital Index (HCI): Learning-Adjusted Years of School, Female'
        'Human Capital Index (HCI): Learning-Adjusted Years of School, Total'
        'Human Capital Index (HCI): Learning-Adjusted Years of School, Male'
        'PISA: Mean performance on the mathematics scale. Female'
        'PISA: Mean performance on the reading scale'
        'PISA: Mean performance on the reading scale. Male'
        'PISA: Mean performance on the science scale. Female'
        'PISA: Mean performance on the mathematics scale. Male'
        'PISA: Mean performance on the mathematics scale'
        'PISA: Mean performance on the reading scale. Female'
        'PISA: Mean performance on the science scale'
        'PISA: Mean performance on the science scale. Male'
        'TIMSS: Mean performance on the science scale for fourth grade students, total'
        'TIMSS: Mean performance on the science scale for fourth grade students, female'
        'TIMSS: Mean performance on the science scale for eighth grade students, male'
        'TIMSS: Mean performance on the mathematics scale for fourth grade students, total'
        'TIMSS: Mean performance on the mathematics scale for fourth grade students, female'
        'TIMSS: Mean performance on the mathematics scale for eighth grade students, male'
        'TIMSS: Mean performance on the mathematics scale for eighth grade students, female'
        'TIMSS: Mean performance on the mathematics scale for eighth grade students, total'
        'TIMSS: Mean performance on the mathematics scale for fourth grade students, male'
        'TIMSS: Mean performance on the science scale for eighth grade students, female'
        'TIMSS: Mean performance on the science scale for eighth grade students, total'
        'TIMSS: Mean performance on the science scale for fourth grade students, male'
        'Harmonized Test Scores, Female' 'Harmonized Test Scores, Total'
        'Harmonized Test Scores, Male'
        'PIRLS: Mean performance on the reading scale, male'
        'PIRLS: Mean performance on the reading scale, female'
        'PIRLS: Mean performance on the reading scale, total'
    3. In the top right corner, click on the "Download" button.
    4. Choose the "CSV" format and initiate the download.

    Note: Ensure that the downloaded dataset contains the desired PISA scores and associated information.
    """
    # Create a new snapshot.
    snap = Snapshot(f"wb/{SNAPSHOT_VERSION}/education_learning_outcomes.csv")

    # Ensure destination folder exists.
    snap.path.parent.mkdir(exist_ok=True, parents=True)

    # Copy local data file to snapshots data folder.
    snap.path.write_bytes(Path(path_to_file).read_bytes())

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
