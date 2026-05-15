"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Dataset, Table
from shared import make_tables, table_to_clean_df
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Short name
SHORT_NAME = paths.short_name


def run(dest_dir: str) -> None:
    log.info(f"{SHORT_NAME}: starting")

    #
    # Load inputs.
    #
    log.info(f"{SHORT_NAME}: loading inputs")
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency(SHORT_NAME)
    # Read table from meadow dataset.
    tb_meadow = ds_meadow[SHORT_NAME]

    #
    # Process data.
    #
    log.info(f"{SHORT_NAME}: processing dataframe")
    tb = clean_table(tb_meadow)

    # Create a new table with the processed data.
    log.info(f"{SHORT_NAME}: generating tables")
    tables = make_tables(tb, SHORT_NAME)

    #
    # Save outputs.
    #
    log.info(f"{SHORT_NAME}: adding tables to dataset")
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=tables, default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info(f"{SHORT_NAME}: end")


def clean_table(tb: Table) -> Table:
    # Standardize names of conflict participants
    tb = table_to_clean_df(tb)
    return tb
