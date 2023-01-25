import pandas as pd
from owid import catalog

from etl.helpers import PathFinder
from etl.snapshot import Snapshot

P = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # load snapshot
    data = pd.read_csv(
        Snapshot("{{cookiecutter.namespace}}/{{cookiecutter.version}}/{{cookiecutter.short_name}}.csv").path
    )

    # create empty dataframe and table
    ds = catalog.Dataset.create_empty(dest_dir)
    tb = catalog.Table(data, short_name=P.short_name)

    # add table, update metadata from *.meta.yml and save
    ds.add(tb)
    ds.update_metadata(P.metadata_path)
    ds.save()
