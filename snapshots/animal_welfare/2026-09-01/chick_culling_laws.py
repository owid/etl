"""Script to create a snapshot of dataset.

The data is manually curated: it contains only the countries with a (full or partial) ban on chick
culling, gathered from official sources. All other countries are assumed to have no ban (this is handled
in the garden step). The evidence for each entry, and for notable legislative activity in countries
without a ban, is curated by hand in the citation_full field of the accompanying .dvc file — keep the two
files in sync when a law changes.
"""

from pathlib import Path

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


def run(upload: bool = True) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"animal_welfare/{SNAPSHOT_VERSION}/chick_culling_laws.csv")

    # Countries with a (full or partial) ban on chick culling, and the year when the ban became (or will
    # become) effective. Sources are curated in the citation_full field of the .dvc file.
    columns = ["country", "status", "year_effective"]
    data = [
        ("Austria", "Banned", 2023),
        ("Belgium", "Partially banned", 2021),
        ("France", "Banned", 2023),
        ("Germany", "Banned", 2022),
        ("Italy", "Banned but not yet in effect", 2026),
        ("Luxembourg", "Banned", 2018),
        ("Switzerland", "Partially banned", 2020),
    ]
    tb = snap.read_from_records(data=data, columns=columns)

    # Add file to DVC and upload to S3.
    snap.create_snapshot(data=tb, upload=upload)
