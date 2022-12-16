import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table

from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata


def run(dest_dir: str) -> None:
    snap = Snapshot("ggdc/2020-10-01/ggdc_maddison.xlsx")
    df = pd.read_excel(snap.path)

    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_snapshot_metadata(snap.metadata)

    t = Table(df)
    t.metadata.short_name = "private_example"
    t.metadata.title = ds.metadata.title
    t.metadata.description = ds.metadata.description

    ds.add(underscore_table(t))
    ds.save()
