"""Script to create a snapshot of dataset."""

from io import StringIO
from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"agriculture/{SNAPSHOT_VERSION}/grigg_1995.csv")

    # Data extracted using chatGPT 4o (and manually inspected and corrected).
    data = """
country,1800,1810,1820,1830,1840,1850,1860,1870,1880,1890,1900,1910,1920,1930,1940,1950,1960
Belgium,2247,,,,,2238,2580,,,,,3300,,2940,,,3040
England,2349,,,,,,3240,,2773,,,2760,,2810,3060,3120,3280
Germany,2210,,,,,,2120,,,,,,,,,,2960
Finland,,,,,,,1900,,,,,3000,,2950,,,3110
Norway,,1800,,,2250,,3300,,,,,,,,,,2930
Italy,,,,,,,,2647,2197,2119,,2617,,2627,,,2730
France,1846,,1984,2118,2377,2480,2854,2875,3085,3220,3192,3323,3133,3127,,,3050
    """

    # Create a dataframe with the extracted data.
    df = pd.read_csv(StringIO(data))

    # Create snapshot.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    main()
