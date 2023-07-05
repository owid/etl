from owid import catalog

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    dataset = catalog.Dataset.create_empty(dest_dir, paths.meadow_dataset.metadata)

    table_names = paths.garden_dataset.table_names
    # if your data is in long format, you can use `grapher_helpers.long_to_wide_tables`
    # to get into wide format
    for table_name in table_names:
        table = paths.garden_dataset[table_name]
        dataset.add(table)
        dataset.save()
