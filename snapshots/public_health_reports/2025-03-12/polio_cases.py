"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
DATA_CDC = pd.DataFrame(
    {
        "country": ["United States"] * 38,
        "year": [
            1910,
            1911,
            1913,
            1914,
            1915,
            1916,
            1917,
            1918,
            1919,
            1920,
            1921,
            1922,
            1923,
            1924,
            1925,
            1926,
            1927,
            1928,
            1929,
            1930,
            1931,
            1932,
            1933,
            1934,
            1935,
            1936,
            1937,
            1938,
            1939,
            1940,
            1942,
            1943,
            1944,
            1945,
            1946,
            1947,
            1948,
            1949,
        ],
        "cases": [
            5861,
            1931,
            1934,
            1220,
            1634,
            27363,
            4082,
            2493,
            1932,
            2325,
            6266,
            2222,
            3266,
            5199,
            5926,
            2528,
            10533,
            5113,
            2837,
            9188,
            15790,
            3778,
            4983,
            7519,
            10839,
            4523,
            9511,
            1712,
            7343,
            9826,
            4033,
            12449,
            19029,
            13619,
            25191,
            10734,
            27680,
            42033,
        ],
        "source": [
            "https://www.jstor.org/stable/4567726?seq=4",
            "https://www.jstor.org/stable/4567726?seq=4",
            "https://www.jstor.org/stable/4570998?seq=12",
            "https://www.jstor.org/stable/4572751?seq=7",
            "https://www.jstor.org/stable/4582892?seq=1",
            "https://www.jstor.org/stable/4582892?seq=1",
            "https://www.jstor.org/stable/4582892?seq=1",
            "https://www.jstor.org/stable/4582892?seq=1",
            "https://www.jstor.org/stable/4582892?seq=1",
            "https://www.jstor.org/stable/4582892?seq=1",
            "https://www.jstor.org/stable/4582892?seq=1",
            "https://www.jstor.org/stable/4582892?seq=1",
            "https://www.jstor.org/stable/4582892?seq=1",
            "https://www.jstor.org/stable/4582892?seq=1",
            "https://www.jstor.org/stable/4582892?seq=1",
            "https://www.jstor.org/stable/4582892?seq=1",
            "https://www.jstor.org/stable/4582892?seq=1",
            "https://www.jstor.org/stable/4582892?seq=1",
            "https://www.jstor.org/stable/4582892?seq=1",
            "https://www.jstor.org/stable/4582892?seq=1",
            "https://www.jstor.org/stable/4582892?seq=1",
            "https://www.jstor.org/stable/4582892?seq=1",
            "https://www.jstor.org/stable/4582892?seq=1",
            "https://www.jstor.org/stable/4582892?seq=1",
            "https://www.jstor.org/stable/4582892?seq=1",
            "https://www.jstor.org/stable/4582892?seq=1",
            "https://www.jstor.org/stable/4582892?seq=1",
            "https://www.jstor.org/stable/4583204?seq=1",
            "https://www.jstor.org/stable/4583620",
            "https://www.jstor.org/stable/4584013",
            "https://www.jstor.org/stable/4586491",
            "https://www.jstor.org/stable/4586983",
            "https://www.jstor.org/stable/4586983",
            "https://www.jstor.org/stable/4586983",
            "https://www.jstor.org/stable/4586983",
            "https://www.jstor.org/stable/4586983",
            "https://www.jstor.org/stable/4588131",
            "https://www.jstor.org/stable/4588131",
        ],
    }
)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"public_health_reports/{SNAPSHOT_VERSION}/polio_cases.csv")
    df = DATA_CDC
    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    main()
