"""Load the WHO Mortality Database garden dataset and create the grapher dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    # Step 1: Load the fact tables, one per layer of WHO's cause tree. The `causes` lookup table stays in the
    # catalog; it is not charted.
    ds_garden = paths.load_dataset("mortality_database")
    tables = [ds_garden.read(name, reset_index=False) for name in ds_garden.table_names if name != "causes"]

    # Step 2: Save.
    ds_grapher = paths.create_dataset(tables=tables, default_metadata=ds_garden.metadata)
    ds_grapher.save()
