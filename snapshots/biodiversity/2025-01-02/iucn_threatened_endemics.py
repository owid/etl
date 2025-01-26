from pathlib import Path

import click
import pdfplumber

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def main(path_to_file: str, upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"biodiversity/{SNAPSHOT_VERSION}/iucn_threatened_endemics.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()


def extract_tables():
    # Extract tables from the PDF
    with pdfplumber.open("/Users/fionaspooner/Downloads/2024-2_RL_Table_8a.pdf") as pdf:
        first_page = pdf.pages[0]
        first_page.extract_tables()
        print(first_page.chars[0])
    # tables = camelot.read_pdf("/Users/fionaspooner/Downloads/2024-2_RL_Table_8a.pdf", pages="all")

    # Save each extracted table to a CSV file
    for i, table in enumerate(tables):
        table.to_csv(f"table_{i}.csv")

    print(f"Extracted {len(tables)} tables")
