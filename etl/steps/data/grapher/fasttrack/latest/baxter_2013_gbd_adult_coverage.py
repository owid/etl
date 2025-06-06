import pandas as pd

from etl.helpers import PathFinder, create_dataset, get_metadata_path
from etl.snapshot import Snapshot

paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # load snapshot
    snap = Snapshot("fasttrack/latest/baxter_2013_gbd_adult_coverage.csv")

    # load data
    tb = snap.read_csv()

    ####################################################################################################################
    # Fix indicators with mixed types.
    # There was an entry for "<0.1". We'll simply remove the "<".
    tb["schizophrenia"] = tb["schizophrenia"].str.replace("<", "").astype(float)
    assert all(pd.api.types.is_numeric_dtype(tb[column]) for column in tb.drop(columns=["country", "year"]).columns)
    ####################################################################################################################

    # add table, update metadata from *.meta.yml and save
    ds = create_dataset(dest_dir, tables=[tb.set_index(["country", "year"])], default_metadata=snap.metadata)

    # override metadata if necessary
    meta_path = get_metadata_path(dest_dir).with_suffix(".override.yml")
    if meta_path.exists():
        ds.update_metadata(meta_path)

    ds.save()
