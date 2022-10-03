import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table
from owid.datautils import geo
from structlog import get_logger

from etl.grapher_helpers import country_code_to_country
from etl.helpers import Names

log = get_logger()

# naming conventions
N = Names(__file__)


def run(dest_dir: str) -> None:
    log.info("ghe.start")

    # read dataset from meadow
    ds_meadow = N.meadow_dataset
    tb_meadow = ds_meadow["ghe"]

    df = pd.DataFrame(tb_meadow)

    df["country"] = country_code_to_country(df["country"])

    ds_garden = Dataset.create_empty(dest_dir, metadata=ds_meadow.metadata)

    tb_garden = clean_data(df)
    tb_garden.metadata = tb_meadow.metadata
    for col in tb_garden.columns:
        tb_garden[col].metadata = tb_meadow[col].metadata

    ds_garden.metadata.update_from_yaml(N.metadata_path)
    tb_garden.update_metadata_from_yaml(N.metadata_path, "ghe")

    ds_garden.add(tb_garden)
    ds_garden.save()

    log.info("ghe.end")


def clean_data(df: pd.DataFrame) -> Table:
    df["sex"] = df["sex"].map({"BTSX": "Both sexes", "MLE": "Male", "FMLE": "Female"})
    df = df.set_index(["country", "year", "age_group", "sex", "cause"])
    df = df.round({"daly_rate100k": 2, "daly_count": 2, "death_rate100k": 2, "death_count": 0})
    df["death_count"] = df["death_count"].astype(int)
    df = underscore_table(Table(df))
    return df
