from etl.helpers import PathFinder, create_dataset, get_metadata_path
from etl.snapshot import Snapshot

paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # load snapshot
    snap = Snapshot("fasttrack/latest/war_deaths_by_war_mars.csv")

    # load data
    tb = snap.read_csv()

    # Rename wars
    tb["country"] = tb["country"].replace(
        {
            "Second Sino-Japanese War": "Japanese Invasion of Manchuria",
            "Third Sino-Japanese War": "Second Sino-Japanese War",
        }
    )
    # add table, update metadata from *.meta.yml and save
    ds = create_dataset(dest_dir, tables=[tb.set_index(["country", "year"])], default_metadata=snap.metadata)

    # override metadata if necessary
    meta_path = get_metadata_path(dest_dir).with_suffix(".override.yml")
    if meta_path.exists():
        ds.update_metadata(meta_path)

    ds.save()
