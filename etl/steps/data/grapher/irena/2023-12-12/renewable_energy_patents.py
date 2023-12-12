from owid import catalog

from etl.helpers import PathFinder

N = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Create new Grapher dataset using metadata from garden.
    dataset = catalog.Dataset.create_empty(dest_dir, N.garden_dataset.metadata)
    # Load simplified table from Garden dataset.
    table = N.garden_dataset["renewable_energy_patents_by_technology"]
    # Add table to dataset and save dataset.
    dataset.add(table)
    dataset.save()
