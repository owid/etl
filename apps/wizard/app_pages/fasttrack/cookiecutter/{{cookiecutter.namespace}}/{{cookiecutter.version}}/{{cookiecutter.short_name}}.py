import pandas as pd

from etl.helpers import PathFinder
from etl.snapshot import Snapshot

paths = PathFinder(__file__)


def run() -> None:
    # load snapshot
    snap = Snapshot("{{cookiecutter.namespace}}/{{cookiecutter.version}}/{{cookiecutter.short_name}}.csv")

    # load data
    tb = snap.read_csv()

    # add dimensions with dim_ prefix
    dims = [c for c in tb.columns if c.startswith("dim_")]
    dims_without_prefix = [c[4:] for c in dims]

    if dims:
        tb = tb.rename(columns={d: dw for d, dw in zip(dims, dims_without_prefix)})

    if uses_dates(tb["year"]):
        tb = tb.rename(columns={"year": "date"}).format(["country", "date"] + dims_without_prefix)
    else:
        tb = tb.format(["country", "year"] + dims_without_prefix)

    # add table, update metadata from *.meta.yml and save
    ds = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # override metadata if necessary
    meta_path = paths.metadata_path.with_suffix(".override.yml")
    if meta_path.exists():
        ds.update_metadata(meta_path)

    ds.save()


def uses_dates(s: pd.Series) -> bool:
    return bool(pd.to_datetime(s, errors="coerce", format="%Y-%m-%d").notnull().all())
