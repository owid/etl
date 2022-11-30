import json
from typing import List, cast

import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table
from owid.datautils import geo
from structlog import get_logger

from etl.helpers import Names
from etl.paths import DATA_DIR

log = get_logger()

# naming conventions
N = Names(__file__)


def run(dest_dir: str) -> None:
    log.info("penn_world_table_national_accounts.start")

    # read dataset from meadow
    ds_meadow = Dataset(DATA_DIR / "meadow/ggdc/2022-11-28/penn_world_table_national_accounts")
    tb_meadow = ds_meadow["penn_world_table_national_accounts"]

    df = pd.DataFrame(tb_meadow)

    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata

    tb_garden = underscore_table(Table(df))
    tb_garden.metadata = tb_meadow.metadata
    for col in tb_garden.columns:
        tb_garden[col].metadata = tb_meadow[col].metadata

    ds_garden.metadata.update_from_yaml(N.metadata_path)
    tb_garden.update_metadata_from_yaml(N.metadata_path, "penn_world_table_national_accounts")

    ds_garden.add(tb_garden)
    ds_garden.save()

    log.info("penn_world_table_national_accounts.end")
