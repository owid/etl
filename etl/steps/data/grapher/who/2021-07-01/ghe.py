# To run with subset only: GHE_SUBSET_ONLY=1 etl grapher/who/2021-07-01/ghe --grapher
import os
from typing import cast

import pandas as pd
from owid import catalog
from owid.datautils import dataframes

from etl.helpers import PathFinder

N = PathFinder(__file__)


def run(dest_dir: str) -> None:
    dataset = catalog.Dataset.create_empty(dest_dir, N.garden_dataset.metadata)
    dataset.save()

    table = N.garden_dataset["estimates"]

    # Since this script expects a certain structure make sure it is actually met
    expected_primary_keys = [
        "country_code",
        "year",
        "ghe_cause_title",
        "sex_code",
        "agegroup_code",
    ]
    if table.primary_key != expected_primary_keys:
        raise Exception(
            f"GHE Table to transform to grapher contained unexpected primary key dimensions: {table.primary_key} instead of {expected_primary_keys}"
        )

    # We want to export all columns except causegroup and level (for now)
    columns_to_export = [
        "deaths",
        "deaths_rate",
        "deaths_100k",
        "daly",
        "daly_rate",
        "daly_100k",
    ]

    if set(columns_to_export).difference(set(table.columns)):
        raise Exception(
            f"GHE table to transform to grapher did not contain the expected columns but instead had: {list(table.columns)}"
        )

    table.reset_index(inplace=True)
    if "GHE_SUBSET_ONLY" in os.environ:
        table = select_subset_causes(table)
    else:
        table = table
    table[columns_to_export] = (
        table[columns_to_export]
        .astype(float)
        .round({"deaths": 0, "deaths_rate": 2, "deaths_100k": 2, "daly": 2, "daly_rate": 2, "daly_100k": 2})
    )
    table["deaths"] = table["deaths"].astype(int)
    # convert codes to country names
    code_to_country = cast(catalog.Dataset, N.load_dependency("regions"))["regions"]["name"].to_dict()
    table["country"] = dataframes.map_series(table["country_code"], code_to_country, warn_on_missing_mappings=True)

    table = table.drop(["country_code"], axis=1)
    table = table.set_index(["country", "year", "ghe_cause_title", "sex_code", "agegroup_code"])

    table.update_metadata_from_yaml(N.metadata_path, "estimates")

    for column in columns_to_export:
        # use dataset source
        table[column].metadata.sources = dataset.metadata.sources

        # Use short names as titles
        table[column].metadata.title = column

    table = table.loc[:, columns_to_export]

    dataset.add(table)


def select_subset_causes(table: pd.DataFrame) -> pd.DataFrame:
    table = table[(table["sex_code"] == "both") & (table["agegroup_code"] == "ALLAges")]

    return table
