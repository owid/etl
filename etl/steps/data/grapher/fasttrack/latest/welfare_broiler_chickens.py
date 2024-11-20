from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

P = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # load snapshot
    snap = Snapshot("fasttrack/latest/welfare_broiler_chickens.csv")

    # load data
    tb = snap.read(safe_types=False)

    # add table, update metadata from *.meta.yml and save
    ds = create_dataset(dest_dir, tables=[tb.set_index(["country", "year"])], default_metadata=snap.metadata)
    ds.save()
