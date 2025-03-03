"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Data for 1949
DATA_1949 = pd.DataFrame(
    {
        "country": ["United States"] * 14,
        "year": [1922, 1923, 1926, 1927, 1928, 1929, 1931, 1932, 1933, 1934, 1935, 1936, 1937, 1949],
        "deaths": [4012, 10450, 8607, 4433, 6146, 2923, 3576, 1941, 2813, 6986, 3907, 1267, 1501, 949],
        "source": [
            "https://www.census.gov/library/publications/1925/compendia/statab/47ed.html",
            "https://www.census.gov/library/publications/1925/compendia/statab/47ed.html",
            "https://www.census.gov/library/publications/1928/compendia/statab/50ed.html",
            "https://www.census.gov/library/publications/1929/compendia/statab/51ed.html",
            "https://www.census.gov/library/publications/1930/compendia/statab/52ed.html",
            "https://www.census.gov/library/publications/1931/compendia/statab/53ed.html",
            "https://www.census.gov/library/publications/1933/compendia/statab/55ed.html",
            "https://www.census.gov/library/publications/1934/compendia/statab/56ed.html",
            "https://www.census.gov/library/publications/1935/compendia/statab/57ed.html",
            "https://www.census.gov/library/publications/1936/compendia/statab/58ed.html",
            "https://www.census.gov/library/publications/1938/compendia/statab/59ed.html",
            "https://www.census.gov/library/publications/1939/compendia/statab/60ed.html",
            "https://www.census.gov/library/publications/1940/compendia/statab/61ed.html",
            "https://www.census.gov/library/publications/1952/compendia/statab/73ed.html",
        ],
    }
)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"health/{SNAPSHOT_VERSION}/measles_deaths_census_bureau.csv")
    df = DATA_1949

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    main()
