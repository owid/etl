from owid import catalog
from structlog import get_logger

from etl.helpers import Names

N = Names(__file__)
log = get_logger()


def run(dest_dir: str) -> None:
    dataset = catalog.Dataset.create_empty(dest_dir, N.garden_dataset.metadata)
    table = N.garden_dataset["un_igme"]
    dataset.add(table)
    dataset.save()
