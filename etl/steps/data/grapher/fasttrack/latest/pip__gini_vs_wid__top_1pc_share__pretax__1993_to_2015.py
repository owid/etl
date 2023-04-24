import pandas as pd
from owid import catalog

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

P = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # load snapshot
    data = pd.read_csv(Snapshot("fasttrack/latest/pip__gini_vs_wid__top_1pc_share__pretax__1993_to_2015.csv").path)

    # create empty dataframe and table
    tb = catalog.Table(data, short_name=P.short_name)

    # add table, update metadata from *.meta.yml and save
    ds = create_dataset(dest_dir, tables=[tb])
    ds.save()
