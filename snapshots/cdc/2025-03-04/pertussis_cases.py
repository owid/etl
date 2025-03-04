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
        "country": ["United States"] * 79,
        "year": list(range(1944, 2023)),
        "cases": [
            109873,
            133792,
            109860,
            156517,
            74715,
            69479,
            120718,
            68687,
            45030,
            37129,
            60886,
            62786,
            31732,
            28295,
            21148,
            40005,
            14809,
            11468,
            17749,
            17135,
            13005,
            6799,
            7717,
            9718,
            4810,
            3285,
            4249,
            3036,
            3287,
            1759,
            2402,
            1738,
            1010,
            2177,
            2063,
            1623,
            1730,
            1248,
            1895,
            2463,
            2276,
            3589,
            4195,
            2823,
            3450,
            4157,
            4570,
            2719,
            4083,
            6586,
            4617,
            5137,
            7796,
            6564,
            7405,
            7288,
            7867,
            7580,
            9771,
            11647,
            25827,
            25616,
            15632,
            10454,
            13278,
            16858,
            27550,
            18719,
            48277,
            28639,
            32971,
            20762,
            17972,
            18975,
            15609,
            18617,
            6124,
            2116,
            3044,
        ],
        "source": [
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
    snap = Snapshot(f"cdc/{SNAPSHOT_VERSION}/pertussis_cases.csv")
    df = DATA_CDC
    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    main()
