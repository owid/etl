"""Script to create a snapshot of dataset.

The counts are typed in by hand from CDC publications (see the `source` column and the .dvc description).
Each update adds the new years to the `cases` and `source` lists and moves the year range forward.

- Finalized counts come from CDC's annual tables of nationally notifiable diseases ("Poliomyelitis, paralytic").
  Since 2025 these are on CDC Stacks (https://stacks.cdc.gov/cbrowse?pid=cdc%3A49375&parentId=cdc%3A49375),
  e.g. the 2023 Table 1 is https://stacks.cdc.gov/view/cdc/251097, where "—" means no reported cases.
- Years without finalized tables use the provisional year-to-date count for US residents in the year's last MMWR
  week, from the NNDSS Weekly Data on data.cdc.gov (https://data.cdc.gov/NNDSS/NNDSS-Weekly-Data/x9gk-5huc).

NOTE: www.cdc.gov and wonder.cdc.gov return 403 to browser-like user agents, but a plain request works.
"""

from pathlib import Path

import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
DATA_CDC = pd.DataFrame(
    {
        "country": ["United States"] * 76,
        "year": list(range(1950, 2026)),
        "cases": [
            33303,
            28386,
            57879,
            35592,
            38476,
            28985,
            15140,
            5485,
            5787,
            8425,
            3190,
            988,
            762,
            396,
            106,
            61,
            106,
            40,
            53,
            18,
            31,
            17,
            29,
            7,
            7,
            8,
            12,
            17,
            9,
            26,
            8,
            6,
            8,
            15,
            8,
            7,
            9,
            9,
            9,
            9,
            6,
            9,
            6,
            3,
            8,
            7,
            7,
            6,
            3,
            2,
            0,
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
            0,
            0,
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
            # 2023 (finalized), 2024 and 2025 (provisional).
            0,
            0,
            0,
        ],
        "source": [
            *["https://www.globalhealthchronicles.org/items/show/6303"] * 11,
            *["https://www.cdc.gov/mmwr/preview/index93.html"] * 33,
            *["https://www.cdc.gov/mmwr/preview/mmwrhtml/mm5853a1.htm"] * 8,
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/mm5153a1.htm",
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/mm5254a1.htm",
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/mm5353a1.htm",
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/mm5453a1.htm",
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/mm5553a1.htm",
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/mm5653a1.htm",
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/mm5754a1.htm",
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/mm5853a1.htm",
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/mm5953a1.htm",
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/mm6053a1.htm",
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/mm6153a1.htm",
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/mm6253a1.htm",
            "https://www.cdc.gov/mmwr/volumes/63/wr/mm6354a1.htm",
            "https://www.cdc.gov/mmwr/volumes/64/wr/mm6453a1.htm",
            *["https://wonder.cdc.gov/nndss-annual-summary.html"] * 7,
            "https://stacks.cdc.gov/view/cdc/251097",
            *["https://data.cdc.gov/NNDSS/NNDSS-Weekly-Data/x9gk-5huc"] * 2,
        ],
    }
)


def run(upload: bool = True) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"cdc/{SNAPSHOT_VERSION}/polio_cases.csv")

    # Add the hand-typed data to DVC and upload to S3.
    snap.create_snapshot(upload=upload, data=DATA_CDC)
