#
#  covid19.py
#  owid/latest/covid
#

import datetime as dt

import pandas as pd
from owid.catalog import Dataset, License, Origin, Table

MEGAFILE_URL = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"


def run(dest_dir: str) -> None:
    d = create_dataset(dest_dir)

    df = pd.read_csv(MEGAFILE_URL)

    df["date"] = pd.to_datetime(df.date)

    for col in ["iso_code", "continent", "location"]:
        df[col] = df[col].astype("category")

    df.set_index(["iso_code", "date"], inplace=True)

    t = Table(df)
    t.metadata.short_name = "covid"
    d.add(t)


def create_dataset(dest_dir: str) -> Dataset:
    d = Dataset.create_empty(dest_dir)
    d.metadata.version = "latest"
    d.metadata.short_name = "covid"
    d.metadata.namespace = "owid"
    d.metadata.origins = [
        Origin(
            producer="Various sources",
            title="COVID-19 dataset",
            description="Our complete COVID-19 dataset maintained by Our World in Data. We will update it daily throughout the duration of the COVID-19 pandemic.",
            attribution_short="OWID",
            url_main="https://github.com/owid/covid-19-data/tree/master/public/data",
            url_download=MEGAFILE_URL,
            date_accessed=str(dt.date.today()),
            date_published="latest",
            license=License(
                name="Other (Attribution)",
                url="https://github.com/owid/covid-19-data/tree/master/public/data#license",
            ),
        )
    ]
    d.save()
    return d
