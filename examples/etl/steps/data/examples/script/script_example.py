import pandas as pd
from owid.walden import Catalog
from owid.catalog import Dataset, Table


def load_wb_income() -> pd.DataFrame:
    """Load WB income groups dataset from walden."""
    walden_ds = Catalog().find_one("wb", "2021-07-01", "wb_income")
    local_path = walden_ds.ensure_downloaded()
    return pd.read_excel(local_path)


def run(dest_dir: str) -> None:
    df = load_wb_income()

    # TODO: add harmonization

    # TODO: add population

    ds = Dataset.create_empty(dest_dir)
    ds.metadata.short_name = "dataset_example"

    t = Table(df.reset_index(drop=True))
    t.metadata.short_name = "table_example"

    ds.add(t)
    ds.save()
