"""FAOSTAT grapher step for faostat_qcl dataset."""
from .shared import catalog, get_grapher_dataset_from_file_name, get_grapher_table


def run(dest_dir: str) -> None:
    garden_dataset = get_grapher_dataset_from_file_name(__file__)

    dataset = catalog.Dataset.create_empty(dest_dir, garden_dataset.metadata)
    dataset.save()

    dataset.add(get_grapher_table(garden_dataset))
