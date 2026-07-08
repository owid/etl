"""Snapshot of employment in the primary sector of fisheries and aquaculture (FAO SOFIA 2024, Table 10).

The values below are extracted by Claude (and visually inspected by a human) from Table 10 ("Employment in the primary sector of fisheries and aquaculture by geographical region and subsector, 1995-2022"), on page 61 of the print edition of The State of World Fisheries and Aquaculture 2024:
https://openknowledge.fao.org/handle/20.500.14283/cd0683en

"""

import pandas as pd

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Verbatim period labels of Table 10 (in order).
PERIODS = ["1995", "2000s", "2010s", "2020", "2022"]

# Employment in thousands of people, by subsector and region, for each period in PERIODS.
DATA = {
    "Aquaculture": {
        "World": [11169, 15912, 21879, 22151, 22086],
        "Africa": [152, 241, 498, 608, 648],
        "Asia": [10561, 15124, 20866, 21039, 20900],
        "Europe": [106, 110, 106, 102, 102],
        "Latin America and the Caribbean": [330, 415, 390, 380, 413],
        "Northern America": [11, 11, 10, 11, 11],
        "Oceania": [9, 10, 9, 10, 12],
    },
    "Inland fisheries": {
        "World": [11530, 15601, 16682, 18640, 17935],
        "Africa": [1547, 2418, 3067, 3144, 3133],
        "Asia": [9667, 12762, 13210, 15153, 14451],
        "Europe": [46, 40, 36, 37, 32],
        "Latin America and the Caribbean": [262, 375, 365, 301, 313],
        "Northern America": [7, 6, 4, 3, 3],
        "Oceania": [1, 1, 1, 1, 1],
    },
    "Marine fisheries": {
        "World": [11631, 13472, 15228, 15698, 15685],
        "Africa": [1317, 1602, 1944, 2084, 2155],
        "Asia": [8653, 10278, 11339, 11678, 11535],
        "Europe": [322, 241, 197, 180, 175],
        "Latin America and the Caribbean": [946, 1086, 1452, 1495, 1516],
        "Northern America": [313, 188, 216, 181, 226],
        "Oceania": [80, 77, 80, 79, 78],
    },
    "Unspecified": {
        "World": [6920, 6750, 6965, 6341, 6109],
        "Africa": [193, 201, 208, 205, 204],
        "Asia": [6584, 6415, 6651, 6041, 5808],
        "Europe": [82, 85, 62, 66, 69],
        "Latin America and the Caribbean": [37, 46, 40, 26, 23],
        "Northern America": [None, None, 3, 4, 4],
        "Oceania": [24, 2, 0, 0, 0],
    },
    "Fisheries and aquaculture, total": {
        "World": [41250, 51735, 60755, 62829, 61815],
        "Africa": [3209, 4462, 5717, 6042, 6141],
        "Asia": [35465, 44579, 52066, 53911, 52695],
        "Europe": [554, 477, 401, 385, 379],
        "Latin America and the Caribbean": [1576, 1923, 2246, 2202, 2265],
        "Northern America": [331, 205, 234, 199, 244],
        "Oceania": [114, 89, 90, 91, 91],
    },
}


def run(upload: bool = True) -> None:
    # Reshape the transcribed table into a long dataframe.
    records = []
    for subsector, regions in DATA.items():
        for region, values in regions.items():
            for period, value in zip(PERIODS, values):
                if value is None:
                    continue
                records.append(
                    {
                        "subsector": subsector,
                        "region": region,
                        "period": period,
                        "employment_thousands": value,
                    }
                )
    df = pd.DataFrame.from_records(records)

    # Create a new snapshot.
    snap = paths.init_snapshot()

    # Store the data and add it to DVC (and optionally upload to S3).
    snap.create_snapshot(data=df, upload=upload)
