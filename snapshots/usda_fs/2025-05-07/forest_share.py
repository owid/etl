"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
df = pd.DataFrame(
    {
        "country": [*["United States"]] * 11,
        "year": [1630, 1907, 1920, 1940, 1953, 1963, 1977, 1987, 1997, 2007, 2012],
        "forest_share": [46.18, 33.52, 31.85, 32.6, 32.78, 33.27, 32.78, 32.38, 32.97, 32.64, 33.88],
        "source": [
            *[
                "https://web.archive.org/web/20220728061823/https://www.fia.fs.fed.us/library/brochures/docs/2000/ForestFactsMetric.pdf"
            ]
            * 2,
            *[
                "https://www.fs.usda.gov/sites/default/files/legacy_files/media/types/publication/field_pdf/forestfacts-2014aug-fs1035-508complete.pdf"
            ]
            * 9,
        ],
    }
)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(upload: bool) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"usda_fs/{SNAPSHOT_VERSION}/forest_share.csv")

    # Save snapshot.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    run()
