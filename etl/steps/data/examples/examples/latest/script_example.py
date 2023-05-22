from typing import cast

import pandas as pd
from owid.catalog import Dataset, Table
from owid.walden import Catalog as WaldenCatalog

from etl.helpers import PathFinder
from etl.paths import DATA_DIR

paths = PathFinder(__file__)


def load_wb_income() -> pd.DataFrame:
    """Load WB income groups dataset from walden."""
    walden_ds = WaldenCatalog().find_one("wb", "2021-07-01", "wb_income")
    local_path = walden_ds.ensure_downloaded()
    return cast(pd.DataFrame, pd.read_excel(local_path))


def run(dest_dir: str) -> None:
    df = load_wb_income()

    # Convert iso codes to country names
    countries_regions = cast(Dataset, paths.load_dependency("regions"))["regions"]
    df["country"] = df.Code.map(countries_regions.name)

    # NOTE: For simplicity we are loading population from Maddison, but in practive
    # you would load it from `garden/owid/latest/key_indicators`, i.e.
    # indicators = Dataset(DATA_DIR / "garden/owid/latest/key_indicators")
    # population = indicators["population"]["population"].xs(2022, level="year")

    # Add population
    maddison = Dataset(DATA_DIR / "garden/ggdc/2020-10-01/ggdc_maddison")
    population = maddison["maddison_gdp"]["population"].xs(2018, level="year")
    df["population"] = df.country.map(population)

    df = df.reset_index().rename(
        columns={
            "Income group": "income_group",
        }
    )
    df = df[["country", "population", "income_group"]]

    ds = Dataset.create_empty(dest_dir)
    ds.metadata.short_name = "dataset_example"
    ds.metadata.namespace = "examples"

    t = Table(df.reset_index(drop=True))
    t.metadata.short_name = "table_example"

    ds.add(t)
    ds.save()
