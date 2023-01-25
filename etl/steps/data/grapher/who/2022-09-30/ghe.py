# To run with subset only: GHE_SUBSET_ONLY=1 etl grapher/who/2022-09-30/ghe --grapher
import os

import pandas as pd
from owid import catalog

from etl.helpers import PathFinder

N = PathFinder(__file__)


def run(dest_dir: str) -> None:
    dataset = catalog.Dataset.create_empty(dest_dir, N.garden_dataset.metadata)
    dataset.save()

    table = N.garden_dataset["ghe"]

    # Since this script expects a certain structure make sure it is actually met
    expected_primary_keys = ["country", "year", "age_group", "sex", "cause"]
    if table.primary_key != expected_primary_keys:
        raise Exception(
            f"GHE Table to transform to grapher contained unexpected primary key dimensions: {table.primary_key} instead of {expected_primary_keys}"
        )

    # We want to export all columns except causegroup and level (for now)
    columns_to_export = [
        "death_count",
        "death_rate100k",
        "daly_count",
        "daly_rate100k",
    ]

    if set(columns_to_export).difference(set(table.columns)):
        raise Exception(
            f"GHE table to transform to grapher did not contain the expected columns but instead had: {list(table.columns)}"
        )

    table.reset_index(inplace=True)
    table = table.drop(columns="flag_level")
    # There are many diaggregations by age and sex we don't want all of them at the moment so we have this option to only load the subset
    if "GHE_SUBSET_ONLY" in os.environ:
        table = select_subset_causes(table)
    else:
        table = table
    # Calculating global totals of deaths and daly's for each disease
    table = add_global_totals(table)

    table = table.set_index(["country", "year", "cause", "sex", "age_group"])

    table.update_metadata_from_yaml(N.metadata_path, "ghe")

    for column in columns_to_export:
        # use dataset source
        table[column].metadata.sources = dataset.metadata.sources

        # Use short names as titles
        table[column].metadata.title = column

    table = table.loc[:, columns_to_export]

    dataset.add(table)


def select_subset_causes(table: pd.DataFrame) -> pd.DataFrame:

    table = table[(table["sex"] == "Both sexes") & (table["age_group"] == "ALLAges")]

    return table


def add_global_totals(table: pd.DataFrame) -> pd.DataFrame:
    glob_total = table.groupby(["year", "cause"])[["daly_count", "death_count"]].sum().reset_index()
    glob_total["country"] = "World"
    glob_total["age_group"] = "ALLAges"
    glob_total["sex"] = "Both sexes"
    table = pd.concat([table, glob_total])
    return table
