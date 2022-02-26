import pandas as pd
from owid.catalog import Dataset, Table
from owid.walden import Catalog

from etl.steps.data.converters import convert_walden_metadata


def run(dest_dir: str) -> None:
    walden_ds = Catalog().find_one("_private_test", "2021", "private_test")

    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(walden_ds)

    df = pd.DataFrame({"a": [1, 2, 3]})

    t = Table(df)
    t.metadata.short_name = "private_example"
    t.metadata.title = ds.metadata.title
    t.metadata.description = ds.metadata.description

    ds.add(t)
    ds.save()
