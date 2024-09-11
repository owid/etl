import pandas as pd

from etl.helpers import PathFinder, create_dataset, get_metadata_path
from etl.snapshot import Snapshot

paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # load snapshot
    snap = Snapshot("fasttrack/latest/elephant_populations.csv")

    # load data
    tb = snap.read_csv()

    if uses_dates(tb["year"]):
        tb = tb.rename(columns={"year": "date"}).format(["country", "date"])
    else:
        tb = tb.format(["country", "year"])

    # add table, update metadata from *.meta.yml and save
    ds = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # override metadata if necessary
    meta_path = get_metadata_path(dest_dir).with_suffix(".override.yml")
    if meta_path.exists():
        ds.update_metadata(meta_path)

    ds.save()


def uses_dates(s: pd.Series) -> bool:
    return pd.to_datetime(s, errors="coerce", format="%Y-%m-%d").notnull().all()
