from owid import catalog

from etl.helpers import PathFinder

N = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Initiate grapher dataset
    dataset = catalog.Dataset.create_empty(dest_dir, N.garden_dataset.metadata)

    # get table from dataset
    table = N.garden_dataset["undp_hdr"]

    # add table to dataset
    dataset.add(table)

    # save and exit
    dataset.save()
