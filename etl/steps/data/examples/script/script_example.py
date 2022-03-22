import pandas as pd

from owid.walden import Catalog
from owid.catalog import Dataset, Table
from etl.paths import DATA_DIR


def load_wb_income() -> pd.DataFrame:
    """Load WB income groups dataset from walden."""
    walden_ds = Catalog().find_one("wb", "2021-07-01", "wb_income")
    local_path = walden_ds.ensure_downloaded()
    return pd.read_excel(local_path)


def run(dest_dir: str) -> None:
    df = load_wb_income()

    # Convert iso codes to country names
    reference_dataset = Dataset(DATA_DIR / "reference")
    countries_regions = reference_dataset["countries_regions"]
    df["country"] = df.Code.map(countries_regions.name)

    # Add population
    indicators = Dataset(DATA_DIR / "garden/owid/latest/key_indicators")
    population = indicators["population"]["population"].xs(2022, level="year")

    df["population"] = df.country.map(population)

    df = df.reset_index().rename(
        columns={
            "Income group": "income_group",
        }
    )
    df = df[["country", "population", "income_group"]]

    ds = Dataset.create_empty(dest_dir)
    ds.metadata.short_name = "dataset_example"

    t = Table(df.reset_index(drop=True))
    t.metadata.short_name = "table_example"

    ds.add(t)
    ds.save()
