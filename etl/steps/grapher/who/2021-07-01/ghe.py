from pathlib import Path
from typing import Iterable

import pandas as pd
import yaml
from owid import catalog

from etl import grapher_helpers as gh
from etl.paths import DATA_DIR, REFERENCE_DATASET


def get_grapher_dataset() -> catalog.Dataset:
    dataset = catalog.Dataset(DATA_DIR / "garden" / "who" / "2021-07-01" / "ghe")
    dataset.metadata.short_name = "ghe__2021_07_01"
    dataset.metadata.namespace = "who"
    return dataset


def get_grapher_tables(dataset: catalog.Dataset) -> Iterable[catalog.Table]:
    table = dataset["estimates"]

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
    table = table.merge(
        right=countries_regions[["legacy_entity_id"]],
        how="left",
        left_on="country_code",
        right_index=True,
        validate="m:1",
    )

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

    # Load variable descriptions and units from the annotations.yml file and
    # store them as column metadata
    script_dir = Path(__file__).parent
    with open(script_dir / "annotations.yml") as istream:
        annotations = yaml.safe_load(istream)

    for column in columns_to_export:
        annotation = annotations["variables"][column]
        table[column].metadata.description = annotation["description"]
        table[column].metadata.unit = annotation["unit"]
        table[column].metadata.short_unit = annotation["short_unit"]

    # Sanity check
    for column in columns_to_export:
        assert table[column].metadata.unit is not None, "Unit should not be None here!"

    yield from gh.yield_wide_table(table, na_action="drop")
