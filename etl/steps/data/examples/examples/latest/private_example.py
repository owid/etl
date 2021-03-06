import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table
from owid.walden import Catalog

from etl.steps.data.converters import convert_walden_metadata


def run(dest_dir: str) -> None:
    walden_ds = Catalog().find_one("ggdc", "2020-10-01", "ggdc_maddison")
    df = pd.read_excel(walden_ds.ensure_downloaded())

    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(walden_ds)

    t = Table(df)
    t.metadata.short_name = "private_example"
    t.metadata.title = ds.metadata.title
    t.metadata.description = ds.metadata.description

    ds.add(underscore_table(t))
    ds.save()
