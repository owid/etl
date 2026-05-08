"""Load garden tables and emit them all to the grapher dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    ds_garden = paths.load_dataset("child_labor_long_run")
    tables = [ds_garden[name] for name in ds_garden.table_names]
    ds_grapher = paths.create_dataset(tables=tables, default_metadata=ds_garden.metadata)
    ds_grapher.save()
