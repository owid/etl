import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table
from structlog import get_logger

from etl.helpers import Names
from etl.paths import DATA_DIR

log = get_logger()

# naming conventions
N = Names(__file__)


def run(dest_dir: str) -> None:
    log.info("global_primary_energy.start")

    # Load dataset from meadow.
    ds_meadow = Dataset(DATA_DIR / "meadow/smil/2017-01-01/global_primary_energy")
    tb_meadow = ds_meadow["global_primary_energy"]
    df = pd.DataFrame(tb_meadow)

    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata

    # Copy all metadata from meadow, including variable metadata.
    tb_garden = underscore_table(Table(df))
    tb_garden.metadata = tb_meadow.metadata
    for col in tb_garden.columns:
        tb_garden[col].metadata = tb_meadow[col].metadata
    # Sort data conveniently.
    tb_garden = tb_garden[sorted(tb_garden.columns)].sort_index()

    # Update metadata using yaml file.
    ds_garden.metadata.update_from_yaml(N.metadata_path)
    tb_garden.update_metadata_from_yaml(N.metadata_path, "global_primary_energy")

    # Add table to dataset.
    ds_garden.add(tb_garden)

    # Save dataset.
    ds_garden.save()

    log.info("global_primary_energy.end")
