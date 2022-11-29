from owid import catalog
from owid.catalog import VariableMeta
from structlog import get_logger

from etl.helpers import Names

N = Names(__file__)
# N = Names("etl/steps/data/grapher/un/2021-12-20/un_igme.py")

log = get_logger()


def run(dest_dir: str) -> None:
    dataset = catalog.Dataset.create_empty(dest_dir, N.garden_dataset.metadata)
    tables = N.garden_dataset.table_names

    for table in tables:
        log.info(f"Adding...{table}")
        table_df = N.garden_dataset[table]

        VariableMeta(table_df[table], unit=table_df.metadata.unit)
        # log.info(f"Unit..{table_df.metadata.unit}")
        # optionally set additional dimensions
        # table_df = table_df.set_index(["country", "year"], append=True)

        # if you data is in long format, check gh.long_to_wide_tables
        dataset.add(table_df)

    # dataset.save()
