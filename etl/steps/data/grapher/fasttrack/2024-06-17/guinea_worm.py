from etl.helpers import PathFinder, create_dataset, get_metadata_path
from etl.snapshot import Snapshot

paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # load snapshot
    snap = Snapshot("fasttrack/2024-06-17/guinea_worm.csv")

    # load data
    tb = snap.read_csv()

    # add table, update metadata from *.meta.yml and save
    tb = tb.format(["country", "year"])
    ds = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # override metadata if necessary
    meta_path = get_metadata_path(dest_dir).with_suffix(".override.yml")
    if meta_path.exists():
        ds.update_metadata(meta_path)

    ds.save()
