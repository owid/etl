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
        "cases": [179829],
        "source": ["https://www.jstor.org/stable/4575902?seq=25"],
    }
)
# Data  for 1921
DATA_1921 = pd.DataFrame(
    {
        "country": ["United States"],
        "year": [1921],
        "cases": [280930],
        "source": ["https://www.jstor.org/stable/4576538"],
    }
)
# Data for 1924
DATA_1924 = pd.DataFrame(
    {
        "country": ["United States"],
        "year": [1924],
        "cases": [511305],
        "source": ["https://www.jstor.org/stable/4577735?seq=34"],
    }
)
# Data for 1925
DATA_1925 = pd.DataFrame(
    {
        "country": ["United States"],
        "year": [1925],
        "cases": [225027],
        "source": ["https://www.jstor.org/stable/4578131?seq=2"],
    }
)
# Data for 1938-1943
DATA_1938_1943 = pd.DataFrame(
    {
        "country": ["United States"] * 6,
        "year": [1938, 1939, 1940, 1941, 1942, 1943],
        "cases": [822811, 403317, 291162, 894134, 547393, 633627],
        "source": ["https://www.census.gov/library/publications/1945/compendia/statab/66ed.html"] * 6,
    }
)
# Data for 1944-1984
DATA_1944_1984 = pd.DataFrame(
    {
        "country": ["United States"] * 41,
        "year": range(1944, 1985),
        "cases": [
            630291,
            146013,
            649843,
            222375,
            615104,
            625281,
            319124,
            530118,
            683077,
            449146,
            682720,
            555156,
            611936,
            486799,
            763094,
            406162,
            441703,
            423919,
            481530,
            385156,
            458083,
            261904,
            204136,
            62705,
            22231,
            25826,
            47351,
            75290,
            32275,
            26690,
            22094,
            24374,
            41126,
            57345,
            26871,
            13597,
            13506,
            3124,
            1714,
            1497,
            2587,
        ],
        "source": ["https://www.cdc.gov/mmwr/PDF/wk/mm4253.pdf"] * 41,
    }
)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"health/{SNAPSHOT_VERSION}/measles_historical.csv")
    df = pd.concat(
        [
            pd.DataFrame(DATA_1919),
            pd.DataFrame(DATA_1921),
            pd.DataFrame(DATA_1924),
            pd.DataFrame(DATA_1925),
            pd.DataFrame(DATA_1938_1943),
            pd.DataFrame(DATA_1944_1984),
        ]
    )
    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    main()
