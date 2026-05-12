from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # load snapshot
    snap = Snapshot("fasttrack/2022-11-29/ai_index.csv")
    tb = snap.read_csv()

    # add table, update metadata from *.meta.yml and save
    ds = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)
    ds.save()
