import re
from typing import Optional

from owid.catalog import Dataset
from structlog import getLogger

from etl.helpers import create_dataset, grapher_checks

log = getLogger()


def run_wrapper(dest_dir: str, garden_dataset: Dataset, include: Optional[str] = None) -> None:
    # Read tables from garden dataset.
    tables = []
    for table_name in garden_dataset.table_names:
        if include and not re.search(include, table_name):
            log.warning("ihme_gbd.skip", table_name=table_name)
            continue

        tables.append(garden_dataset[table_name])

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=tables, default_metadata=garden_dataset.metadata)

    #
    # Checks.
    #
    grapher_checks(ds_grapher)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
