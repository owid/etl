"""Script to create a snapshot of dataset.

The counts are typed in by hand from CDC publications and CDC WONDER (see the `source` column and the .dvc
description). Each update adds the new years to the `deaths` and `source` lists and moves the year range forward.

- From 1999, deaths are those with acute poliomyelitis (ICD-10 code A80) as the underlying cause of death. Finalized
  years come from "Underlying Cause of Death, 2018-2024, Single Race" (https://wonder.cdc.gov/ucd-icd10-expanded.html,
  database D158); years not yet finalized come from "Provisional Mortality Statistics, 2018 through Last Week"
  (https://wonder.cdc.gov/mcd-icd10-provisional.html, database D176). Group by year and filter the underlying cause
  of death to A80.

NOTE: www.cdc.gov and wonder.cdc.gov return 403 to browser-like user agents, but a plain request works. The WONDER
API (POST https://wonder.cdc.gov/controller/datarequest/<database>) needs every form field sent, including the empty
text boxes of the multiple-cause filters, or it rejects the request.
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
        "deaths": [
            1904,
            1551,
            3145,
            1450,
            1368,
            1043,
            566,
            221,
            255,
            454,
            230,
            90,
            60,
            41,
            17,
            16,
            9,
            16,
            24,
            13,
            7,
            18,
            2,
            10,
            3,
            9,
            16,
            16,
            13,
            4,
            6,
            0,
            0,
            0,
            0,
            3,
            0,
            0,
            1,
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
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            # 2024 (finalized) and 2025 (provisional).
            0,
            0,
        ],
        "source": [
            *["https://www.globalhealthchronicles.org/items/show/6303"] * 10,
            *["https://stacks.cdc.gov/view/cdc/951"] * 8,
            *["https://wonder.cdc.gov/cmf-icd8.html"] * 11,
            *["https://wonder.cdc.gov/cmf-icd9.html"] * 20,
            *["https://wonder.cdc.gov/deaths-by-underlying-cause.html"] * 25,
            "https://wonder.cdc.gov/ucd-icd10-expanded.html",
            "https://wonder.cdc.gov/mcd-icd10-provisional.html",
        ],
    }
)


def run(upload: bool = True) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"cdc/{SNAPSHOT_VERSION}/polio_deaths.csv")

    # Add the hand-typed data to DVC and upload to S3.
    snap.create_snapshot(upload=upload, data=DATA_CDC)
