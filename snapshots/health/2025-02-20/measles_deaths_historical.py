"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Data for 1919
DATA_1919 = pd.DataFrame(
    {
        "country": ["United States"],
        "year": [1919],
        "deaths": [12992],
        "source": ["https://www.jstor.org/stable/4575902?seq=25"],
    }
)
# Data  for 1921
DATA_1921 = pd.DataFrame(
    {
        "country": ["United States"],
        "year": [1921],
        "deaths": [3370],
        "source": ["https://www.jstor.org/stable/4576538"],
    }
)
# Data for 1924
DATA_1924 = pd.DataFrame(
    {
        "country": ["United States"],
        "year": [1924],
        "deaths": [8370],
        "source": ["https://www.jstor.org/stable/4577735"],
    }
)
# Data for 1925
DATA_1925 = pd.DataFrame(
    {
        "country": ["United States"],
        "year": [1925],
        "deaths": [2309],
        "source": ["https://www.jstor.org/stable/4578131"],
    }
)
# Data for 1937
DATA_1937 = pd.DataFrame(
    {
        "country": ["United States"],
        "year": [1937],
        "deaths": [1395],
        "source": ["https://stacks.cdc.gov/view/cdc/69651/cdc_69651_DS1.pdf"],
    }
)

# Data for 1938

DATA_1938 = pd.DataFrame(
    {
        "country": ["United States"],
        "year": [1938],
        "deaths": [3227],
        "source": ["https://www.jstor.org/stable/4583204"],
    }
)

# Data for 1939
DATA_1939 = pd.DataFrame(
    {
        "country": ["United States"],
        "year": [1939],
        "deaths": [1171],
        "source": ["https://www.jstor.org/stable/4583620"],
    }
)

# Data for 1940
DATA_1940 = pd.DataFrame(
    {
        "country": ["United States"],
        "year": [1940],
        "deaths": [681],
        "source": ["https://www.jstor.org/stable/4584013"],
    }
)
# Data for 1949
DATA_1949 = pd.DataFrame(
    {
        "country": ["United States"],
        "year": [1949],
        "deaths": [949],
        "source": ["https://www.census.gov/library/publications/1952/compendia/statab/73ed.html"],
    }
)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"health/{SNAPSHOT_VERSION}/measles_deaths_historical.csv")
    df = pd.concat(
        [
            pd.DataFrame(DATA_1919),
            pd.DataFrame(DATA_1921),
            pd.DataFrame(DATA_1924),
            pd.DataFrame(DATA_1925),
            pd.DataFrame(DATA_1937),
            pd.DataFrame(DATA_1938),
            pd.DataFrame(DATA_1939),
            pd.DataFrame(DATA_1940),
            pd.DataFrame(DATA_1949),
        ]
    )
    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    main()
