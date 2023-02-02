import pandas as pd
from owid import catalog

from etl.helpers import PathFinder
from etl.snapshot import Snapshot

N = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # load snapshot
    data = pd.read_csv(Snapshot("fasttrack/2023-01-03/long_term_homicide_rates_in_europe.csv").path)

    # create empty dataframe and table
    ds = catalog.Dataset.create_empty(dest_dir)
    tb = catalog.Table(data, short_name=N.short_name)

    # add table, update metadata from *.meta.yml and save
    ds.add(tb)
    ds.update_metadata(N.metadata_path)
    ds.save()
