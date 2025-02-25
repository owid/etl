"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
# Data for 1937 - 2022 (except 1938-43 inclusive as that's from the census bureau)
DATA_CDC = pd.DataFrame(
    {
        "country": ["United States"] * 80,
        "year": [1937] + list(range(1944, 2023)),
        "deaths": [
            28356,
            14150,
            18675,
            16354,
            12262,
            9493,
            7969,
            5796,
            3983,
            2960,
            2355,
            2041,
            1984,
            1568,
            1211,
            918,
            934,
            918,
            617,
            444,
            314,
            293,
            164,
            209,
            219,
            260,
            241,
            435,
            215,
            152,
            228,
            272,
            307,
            128,
            84,
            76,
            59,
            3,
            5,
            2,
            5,
            1,
            3,
            pd.NA,
            3,
            2,
            3,
            4,
            5,
            4,
            0,
            2,
            0,
            2,
            4,
            1,
            1,
            1,
            2,
            1,
            1,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            1,
            0,
            1,
            0,
            0,
            0,
            1,
            2,
            1,
            0,
            1,
        ],
        "source": [
            "https://stacks.cdc.gov/view/cdc/69651/cdc_69651_DS1.pdf",
            *["https://www.cdc.gov/mmwr/preview/index93.html"] * 50,
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/00039679.htm",
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/00044418.htm",
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/00050719.htm",
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/00056071.htm",
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/mm4753a1.htm",
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/mm4853a1.htm",
            "https://www.jstor.org/stable/23310295",
            *["https://www.jstor.org/stable/23317332"] * 6,
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/mm5653a1.htm",
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/mm5754a1.htm",
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/mm5853a1.htm",
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/mm5953a1.htm",
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/mm6053a1.htm",
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/mm6153a1.htm",
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/mm6253a1.htm",
            "https://www.cdc.gov/mmwr/volumes/63/wr/mm6354a1.htm",
            "https://www.cdc.gov/mmwr/volumes/63/wr/mm6354a1.htm",
            *["https://wonder.cdc.gov/nndss-annual-summary.html"] * 7,
        ],
    }
)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"health/{SNAPSHOT_VERSION}/diphtheria_cases_cdc.csv")
    df = DATA_CDC
    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    main()
