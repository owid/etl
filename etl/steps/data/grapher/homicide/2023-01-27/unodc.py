from owid import catalog

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    dataset = catalog.Dataset.create_empty(dest_dir, paths.garden_dataset.metadata)

    table = paths.garden_dataset["unodc"]

    # optionally set additional dimensions
    # table = table.set_index(["sex", "income_group"], append=True)

    # if your data is in long format, you can use `grapher_helpers.long_to_wide_tables`
    # to get into wide format
    dataset.add(table)

    dataset.save()
