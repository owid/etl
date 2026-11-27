"""Script to create a snapshot of dataset.

The counts are typed in by hand from CDC publications (see the `source` column and the .dvc description).
Each update adds the new years to the `cases` and `source` lists and moves the year range forward.

Which count we take for each year:
- 1944-1983: all reported cases ("Poliomyelitis, total"), from Table 6 of the MMWR Summary of Notifiable Diseases,
  United States, 1993 (https://www.cdc.gov/mmwr/preview/mmwrhtml/00035381.htm). This keeps one definition from the
  earliest years (Public Health Reports, all reported cases) until 1983.
- 1984 onward: paralytic cases only ("Poliomyelitis, paralytic"); 1984-1993 from the same 1993 summary. By the early
  1980s total and paralytic counts are almost identical (1983: 15 and 15), so the switch barely moves the series.

- Finalized counts come from CDC's annual tables of nationally notifiable diseases ("Poliomyelitis, paralytic").
  Since 2025 these are on CDC Stacks (https://stacks.cdc.gov/cbrowse?pid=cdc%3A49375&parentId=cdc%3A49375),
  e.g. the 2023 Table 1 is https://stacks.cdc.gov/view/cdc/251097, where "—" means no reported cases.
- Years without finalized tables use the provisional year-to-date count for US residents in the year's last MMWR
  week, from the NNDSS Weekly Data on data.cdc.gov (https://data.cdc.gov/NNDSS/NNDSS-Weekly-Data/x9gk-5huc).

NOTE: www.cdc.gov and wonder.cdc.gov return 403 to browser-like user agents, but a plain request works.
NOTE: In the weekly data, the US-residents row is labeled "US RESIDENTS" up to 2024 and "U.S. Residents" from 2025,
and some years end in MMWR week 53, so match the label case-insensitively and use each year's last week.
A count of zero shows as an empty value with the flag "-".
"""

from pathlib import Path

import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
DATA_CDC = pd.DataFrame(
    {
        "country": ["United States"] * 82,
        "year": list(range(1944, 2026)),
        "cases": [
            # 1944-1983: all reported cases (paralytic and nonparalytic).
            19029,
            13624,
            25698,
            10827,
            27726,
            42033,
            33300,
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
            1312,
            910,
            449,
            122,
            72,
            113,
            41,
            53,
            20,
            33,
            21,
            31,
            8,
            7,
            8,
            14,
            18,
            15,
            34,
            9,
            6,
            8,
            15,
            # 1984 onward: paralytic cases only.
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
            *["https://www.cdc.gov/mmwr/preview/mmwrhtml/00035381.htm"] * 50,
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
