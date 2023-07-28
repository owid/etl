from owid.catalog import Dataset
from structlog import get_logger

log = get_logger()


def run_wrapper(garden_dataset: Dataset, dataset: Dataset) -> Dataset:
    # add tables to dataset
    tables = garden_dataset.table_names
    for table in tables:
        tab = garden_dataset[table]
        dataset.add(tab)
    dataset.save()
