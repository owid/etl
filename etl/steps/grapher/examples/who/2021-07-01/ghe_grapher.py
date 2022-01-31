from owid import catalog
from collections.abc import Iterable
from pathlib import Path

from etl.paths import DATA_DIR
from etl import grapher_helpers as gh


annotations_path = Path(__file__).parent / "annotations.yml"


def get_grapher_dataset() -> catalog.Dataset:
    dataset = catalog.Dataset(DATA_DIR / "garden/who/2021-07-01/ghe")
    dataset.metadata.short_name = "ghe-2021-07-01"
    dataset.metadata.namespace = "who"
    return dataset


def get_grapher_tables(dataset: catalog.Dataset) -> Iterable[catalog.Table]:
    orig_table = dataset["estimates"]

    # Get the legacy_entity_id from the country_code via the countries_regions dimension table
    reference_dataset = catalog.Dataset(DATA_DIR / "reference")
    countries_regions = reference_dataset["countries_regions"]
    table = orig_table.merge(
        right=countries_regions[["legacy_entity_id"]],
        how="left",
        left_on="country_code",
        right_index=True,
        validate="m:1",
    )

    # Add entity_id and year
    table.reset_index(inplace=True)
    table["year"] = table["year"].astype(int)
    table["entity_id"] = table["legacy_entity_id"].astype(int)

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
    dimensions = ["entity_id", "year", "ghe_cause_title", "sex_code", "agegroup_code"]

    table = table.set_index(
        dimensions,
    )[columns_to_export]

    # Get metadata from the original table and optional annotation file
    table = gh.as_table(table, orig_table)
    table = gh.annotate_table_from_yaml(table, annotations_path)

    yield from gh.yield_wide_table(table)
