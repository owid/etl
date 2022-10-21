import json
from typing import cast

import pandas as pd
from owid.catalog import Dataset

from etl.paths import REFERENCE_DATASET

REGIONS = ["World", "Asia", "Africa", "North America", "South America", "Europe", "Oceania"]


def calculate_region_sums(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate our own totals according to OWID continent definitions. Input dataframe must
    have columns named "year" and "country". The output dataframe will add continents and
    world to the dataframe with column totals.
    """
    assert {"year", "country"} < set(df.columns)
    df = cast(
        pd.DataFrame,
        df[-df.country.isin(REGIONS)],
    )

    countries = Dataset(REFERENCE_DATASET)["countries_regions"]

    continent_rows = []
    for code, row in countries.iterrows():
        if pd.isnull(row.members):
            continue

        members = json.loads(row.members)
        for member in members:
            # use ['name'] instead of .name, since .name references name of the object,
            # not the actual value
            continent_rows.append({"country": countries.loc[member]["name"], "continent": row["name"]})

    continent_list = pd.DataFrame.from_records(continent_rows)

    continents = (
        df.merge(continent_list, on="country")
        .groupby(["continent", "year"], as_index=False)
        .sum(numeric_only=True)
        .rename(columns={"continent": "country"})
    )

    world = (
        df.loc[df.country.isin(continent_list.country)]
        .drop(["country"], axis=1)
        .groupby("year")
        .sum()
        .reset_index()
        .assign(country="World")
    )

    return pd.concat([df, continents, world], ignore_index=True)
