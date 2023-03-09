# To run with subset only: GHE_SUBSET_ONLY=1 etl grapher/who/2022-09-30/ghe --grapher
import pandas as pd
from owid import catalog

from etl.helpers import PathFinder

N = PathFinder(__file__)
GHE_SUBSET_ONLY = True


def run(dest_dir: str) -> None:
    dataset = catalog.Dataset.create_empty(dest_dir, N.garden_dataset.metadata)
    dataset.save()

    table = N.garden_dataset["ghe"]

    # Since this script expects a certain structure make sure it is actually met
    expected_primary_keys = ["country", "year", "age_group", "sex", "cause"]
    if table.primary_key != expected_primary_keys:
        raise Exception(
            "GHE Table to transform to grapher contained unexpected primary key dimensions:"
            f" {table.primary_key} instead of {expected_primary_keys}"
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
            "GHE table to transform to grapher did not contain the expected columns but instead had:"
            f" {list(table.columns)}"
        )

    table.reset_index(inplace=True)
    table = table.drop(columns="flag_level")

    # Calculating global totals of deaths and daly's for each disease
    table = add_global_totals(table)

    # Use subset of the data for now
    if GHE_SUBSET_ONLY:
        table = select_subset_causes(table)

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
    """Selects a subset of all the data.

    There are many diaggregations by age and sex we don't want all of them at the moment so we have this option to only load the subset.

    Basically, we only keep "all ages" age-group and "both sexes" sex group. EXCEPT, when the cause is "Self-harm" where we keep all dimensions (https://github.com/owid/owid-issues/issues/759#issuecomment-1455220066)
    """
    causes_include_all = ["Self-harm"]
    table = table[
        ((table["sex"] == "Both sexes") & (table["age_group"] == "ALLAges")) | (table["cause"].isin(causes_include_all))
    ]
    return table


def add_global_totals(table: pd.DataFrame) -> pd.DataFrame:
    # Get age_group=all and sex=all (avoid duplicates)
    table_ = table[(table["sex"] == "Both sexes") & (table["age_group"] == "ALLAges")]
    # Group by year and cause, sum
    glob_total = table_.groupby(["year", "cause"])[["daly_count", "death_count"]].sum().reset_index()
    # Fill other fields
    glob_total["country"] = "World"
    glob_total["age_group"] = "ALLAges"
    glob_total["sex"] = "Both sexes"
    # Merge with complete table
    table = pd.concat([table, glob_total])
    return table
