import pandas as pd
from owid import catalog

from etl.helpers import PathFinder
from etl.snapshot import Snapshot

N = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # load snapshot
    data = pd.read_csv(Snapshot("fasttrack/2022-02-28/nuclear_warhead_inventories.csv").path)

    # create empty dataframe and table
    ds = catalog.Dataset.create_empty(dest_dir)
    tb = catalog.Table(data)

    # update metadata from *.meta.yml
    ds.metadata.update_from_yaml(N.metadata_path)
    tb.update_metadata_from_yaml(N.metadata_path, N.short_name)

    # add table to dataset and save
    ds.add(tb)
    ds.save()
