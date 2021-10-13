#
#  covid19.py
#  owid/latest/covid
#

from owid.catalog.meta import Source
import datetime as dt

import pandas as pd

from owid.catalog import Dataset, Table
from etl.helpers import downloaded

MEGAFILE_URL = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"


def run(dest_dir: str) -> None:
    d = create_dataset(dest_dir)

    with downloaded(MEGAFILE_URL) as filename:
        df = pd.read_csv(filename)

    for col in ["iso_code", "continent", "location"]:
        df[col] = df[col].astype("category")

    df.set_index(["iso_code", "date"], inplace=True)

    t = Table(df)
    t.metadata.short_name = "covid"
    d.add(t)


def create_dataset(dest_dir: str) -> Dataset:
    d = Dataset.create_empty(dest_dir)
    d.metadata.short_name = "covid19"
    d.metadata.namespace = "owid"
    d.metadata.sources = [
        Source(
            name="Multiple sources via Our World In Data",
            description="Our complete COVID-19 dataset maintained by Our World in Data. We will update it daily throughout the duration of the COVID-19 pandemic.",
            url="https://github.com/owid/covid-19-data/tree/master/public/data",
            source_data_url="https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/covid-19-data.csv",
            owid_data_url="https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/covid-19-data.csv",
            date_accessed=str(dt.date.today()),
            publication_date=str(dt.date.today()),
            publication_year=dt.date.today().year,
        )
    ]
    d.save()

    return d
