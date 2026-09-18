"""Script to create a snapshot of dataset.

The producer publishes its market penetration estimates as text and charts on a web page, not as a data file. The
figures below are transcribed from the text and the chart data of the 2024, 2025 and 2026 editions of the report. Where
a later edition revises an earlier figure (the 2026 edition revised the EU penetration for 2024 and 2025 upwards, after
refining its estimate of the EU flock size), the latest revised figure is used. This file is the single source of truth
for the data.
"""

from pathlib import Path

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Data columns.
COLUMNS = ["country", "date", "metric", "value", "value_low", "value_high"]

# Share of commercial laying hens in production that were sexed in ovo (%).
# Values before 2024 are the producer's historical estimates, shown in the charts of the 2026 edition. Where the
# producer gives only a range, the central value is left empty.
SHARE_OF_COMMERCIAL_HENS = [
    ("European Union (27)", "2019-01-01", 0.1, None, None),
    ("European Union (27)", "2020-01-01", 0.5, None, None),
    ("European Union (27)", "2021-01-01", 1.0, None, None),
    ("European Union (27)", "2022-01-01", 1.7, None, None),
    ("European Union (27)", "2023-01-01", 7.0, 7.0, 7.0),
    ("European Union (27)", "2024-04-01", 23.0, 20.0, 27.0),
    ("European Union (27)", "2025-04-01", 33.6, 30.2, 36.6),
    ("European Union (27)", "2026-06-01", 39.9, 36.2, 43.3),
    ("Norway", "2025-04-01", None, 20.0, 25.0),
    ("Norway", "2026-06-01", 52.0, 45.0, 60.0),
    ("Switzerland", "2026-06-01", 93.0, 85.0, 100.0),
    # Reported as "less than 1%".
    ("United States", "2025-04-01", None, 0.0, 1.0),
    # Reported as "below 1%".
    ("Brazil", "2026-06-01", None, 0.0, 1.0),
]

# Share of all laying hens (including backyard flocks of fewer than 350 hens) that were sexed in ovo (%).
SHARE_OF_ALL_HENS = [
    ("European Union (27)", "2019-01-01", 0.1, None, None),
    ("European Union (27)", "2020-01-01", 0.5, None, None),
    ("European Union (27)", "2021-01-01", 1.0, None, None),
    ("European Union (27)", "2022-01-01", 1.7, None, None),
    ("European Union (27)", "2023-01-01", 7.0, 7.0, 7.0),
    ("European Union (27)", "2024-04-01", 21.0, 18.0, 23.0),
    ("European Union (27)", "2025-04-01", 29.2, 26.3, 31.7),
    ("European Union (27)", "2026-06-01", 35.4, 32.2, 38.3),
]

# Number of commercial laying hens in production that were sexed in ovo (millions).
HENS_SEXED_IN_OVO = [
    ("European Union (27)", "2024-04-01", 78.4, None, None),
    ("European Union (27)", "2025-04-01", None, 102.3, 117.9),
    ("European Union (27)", "2026-06-01", None, 125.0, 142.0),
]

# Number of female day-old chicks produced with in-ovo sexing in the EU (millions), in the 12 months ending in June of
# each year.
FEMALE_CHICKS_PRODUCED = [
    ("European Union (27)", "2022-06-01", 9.8, None, None),
    ("European Union (27)", "2023-06-01", 33.6, None, None),
    ("European Union (27)", "2024-06-01", 61.6, None, None),
    ("European Union (27)", "2025-06-01", 82.64, None, None),
    ("European Union (27)", "2026-06-01", 91.79, None, None),
]

# Cumulative number of male embryos removed before hatching thanks to in-ovo sexing, globally (millions), one value per
# month from April 2021 to June 2026. The producer notes that this series excludes China.
CUMULATIVE_MALE_EMBRYOS_REMOVED_START = (2021, 4)
CUMULATIVE_MALE_EMBRYOS_REMOVED = [
    0, 0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4,
    4.5, 5, 5.5, 6, 8.4, 10.8, 13.2, 15.6, 18, 20.4, 22.8, 25.2,
    27.6, 30, 32.4, 34.8, 39.6, 44.4, 49.2, 54, 58.8, 63.6, 68.4, 73.2,
    78, 82.8, 87.6, 92.4, 99.2, 106, 112.8, 119.6, 126.4, 133.2, 140, 146.8,
    153.6425, 160.4425, 167.2425, 174.0425, 181.3625, 188.6825, 196.0025, 203.3475, 210.6675, 217.9875, 225.3075, 232.6275,
    239.9475, 248.0575, 256.225, 264.335, 272.445, 280.58,
]  # fmt: skip


def build_records() -> list:
    records = []
    for metric, rows in [
        ("share_of_commercial_hens_sexed_in_ovo", SHARE_OF_COMMERCIAL_HENS),
        ("share_of_all_hens_sexed_in_ovo", SHARE_OF_ALL_HENS),
        ("hens_sexed_in_ovo", HENS_SEXED_IN_OVO),
        ("female_chicks_produced_with_in_ovo_sexing", FEMALE_CHICKS_PRODUCED),
    ]:
        records += [(country, date, metric, value, low, high) for country, date, value, low, high in rows]

    year, month = CUMULATIVE_MALE_EMBRYOS_REMOVED_START
    for i, value in enumerate(CUMULATIVE_MALE_EMBRYOS_REMOVED):
        _year, _month = year + (month - 1 + i) // 12, (month - 1 + i) % 12 + 1
        records.append(("World", f"{_year}-{_month:02d}-01", "cumulative_male_embryos_removed", value, None, None))
    assert records[-1][1] == "2026-06-01", "The cumulative series should end in June 2026."

    return records


def run(upload: bool = True) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"animal_welfare/{SNAPSHOT_VERSION}/in_ovo_sexing_market_penetration.csv")

    # Create the data table.
    tb = snap.read_from_records(data=build_records(), columns=COLUMNS)

    # Add file to DVC and upload to S3.
    snap.create_snapshot(data=tb, upload=upload)
