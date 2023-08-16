"""
This code uploads Maddison Database meadow step into garden. No country modifications are included, because only the World entity is needed.
"""

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.paths import DATA_DIR

MEADOW_VERSION = "2022-12-23"
# GARDEN_VERSION = MEADOW_VERSION

log = get_logger()

# naming conventions
N = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("maddison_database.start")

    # Read dataset from meadow
    ds_meadow = Dataset(DATA_DIR / f"meadow/ggdc/{MEADOW_VERSION}/maddison_database")
    tb_meadow = ds_meadow["maddison_database"]

    df = pd.DataFrame(tb_meadow)

    # Create new table with the same metadata as meadow and add it to dataset
    tb_garden = Table(df, like=tb_meadow)

    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)
    ds_garden.save()

    log.info("maddison_database.end")
