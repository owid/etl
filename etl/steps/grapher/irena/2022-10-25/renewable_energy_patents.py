from owid import catalog

from etl.helpers import Names

N = Names(__file__)


def run(dest_dir: str) -> None:
    # Create new Grapher dataset using metadata from garden.
    dataset = catalog.Dataset.create_empty(dest_dir, N.garden_dataset.metadata)
    # Load main table from Garden dataset.
    table = N.garden_dataset[N.garden_dataset.table_names[0]]
    # Add table to dataset and save dataset.
    dataset.add(table)
    dataset.save()
