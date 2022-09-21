import copy

import pandas as pd
from owid import catalog

from etl import grapher_helpers as gh
from etl.helpers import Names
from etl.paths import REFERENCE_DATASET

N = Names(__file__)


def run(dest_dir: str) -> None:
    dataset = catalog.Dataset.create_empty(dest_dir, gh.adapt_dataset_metadata_for_grapher(N.garden_dataset.metadata))
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
        "population",
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

    # Get the legacy_entity_id from the country_code via the countries_regions dimension table
    reference_dataset = catalog.Dataset(REFERENCE_DATASET)
    countries_regions = reference_dataset["countries_regions"]

    orig_table = copy.deepcopy(table)

    # TODO: update `gh.adapt_table_for_grapher(table)` to support country codes
    # and reuse it
    table = orig_table.merge(
        right=countries_regions[["legacy_entity_id"]],
        how="left",
        left_on="country_code",
        right_index=True,
        validate="m:1",
    )
    table.metadata = orig_table.metadata

    # Add entity_id, drop country_code
    table.reset_index(inplace=True)
    df = pd.DataFrame(table)
    table["year"] = df["year"].astype(int)
    table["entity_id"] = df["legacy_entity_id"].astype(int)
    table.drop("country_code", axis="columns", inplace=True)
    table.set_index(
        ["entity_id", "year", "ghe_cause_title", "sex_code", "agegroup_code"],
        inplace=True,
    )

    table.update_metadata_from_yaml(N.metadata_path, "estimates")

    for column in columns_to_export:
        # use dataset source
        table[column].metadata.sources = dataset.metadata.sources

        # Use short names as titles
        table[column].metadata.title = column

    table = table.loc[:, columns_to_export]

    dataset.add(table)
