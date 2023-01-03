"""
This code uploads Maddison Database meadow step into garden. No country modifications are included, because only the World entity is needed.
"""

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import Names
from etl.paths import DATA_DIR

MEADOW_VERSION = "2022-12-23"
# GARDEN_VERSION = MEADOW_VERSION

log = get_logger()

# naming conventions
N = Names(__file__)


def run(dest_dir: str) -> None:
    log.info("maddison_database.start")

    # read dataset from meadow
    ds_meadow = Dataset(DATA_DIR / f"meadow/ggdc/{MEADOW_VERSION}/maddison_database")
    tb_meadow = ds_meadow["maddison_database"]

    df = pd.DataFrame(tb_meadow)

    # create new dataset with the same metadata as meadow
    ds_garden = Dataset.create_empty(dest_dir, metadata=ds_meadow.metadata)

    # create new table with the same metadata as meadow and add it to dataset
    tb_garden = Table(df, like=tb_meadow)
    ds_garden.add(tb_garden)

    # update metadata from yaml file
    ds_garden.update_metadata(N.metadata_path)

    ds_garden.save()

    log.info("maddison_database.end")
