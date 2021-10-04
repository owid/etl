#
#  population.py
#  gapminder
#

import pandas as pd
from owid.catalog import Dataset, Table
from owid.walden import Catalog

from etl.steps.data.converters import convert_walden_metadata


def run(dest_dir: str) -> None:
    walden_ds = Catalog().find_one("gapminder", "2019-12-10", "population")

    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(walden_ds)
    ds.save()

    df = pd.read_excel(
        walden_ds.local_path,
        sheet_name="data-for-countries-etc-by-year",
        usecols=["name", "time", "Population"],
    ).rename(columns={"name": "country", "time": "year", "Population": "population"})
    df["year"] = df.year.astype(int)

    t = Table(df)
    t.metadata.short_name = "population"
    t.metadata.title = ds.metadata.title
    t.metadata.description = ds.metadata.description
    t.set_index(["country", "year"], inplace=True)

    ds.add(t)
