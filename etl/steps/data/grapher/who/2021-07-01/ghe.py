from owid import catalog
from collections.abc import Iterable
import pandas as pd
import slugify
import yaml
from pathlib import Path

from etl.command import DATA_DIR


def get_grapher_dataset() -> catalog.Dataset:
    dataset = catalog.Dataset(DATA_DIR / "garden" / "who" / "2021-07-01" / "ghe")
    dataset.metadata.short_name = "ghe-2021-07-01"
    dataset.metadata.namespace = "who"
    return dataset


def get_grapher_tables(dataset: catalog.Dataset) -> Iterable[catalog.Table]:
    table = dataset["estimates"]

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
    reference_dataset = catalog.Dataset(DATA_DIR / "reference")
    countries_regions = reference_dataset["countries_regions"]
    table = table.merge(
        right=countries_regions[["legacy_entity_id"]],
        how="left",
        left_on="country_code",
        right_index=True,
        validate="m:1",
    )

    table.reset_index(inplace=True)
    df = pd.DataFrame(table)
    table["year"] = df["year"].astype(int)
    table["entity_id"] = df["legacy_entity_id"].astype(int)
    table.drop("country_code", axis="columns", inplace=True)
    table.set_index(
        ["entity_id", "year", "ghe_cause_title", "sex_code", "agegroup_code"],
        inplace=True,
    )

    script_dir = Path(__file__).parent
    with open(script_dir / "annotations.yml") as istream:
        annotations = yaml.safe_load(istream)

    for column in columns_to_export:
        annotation = annotations["variables"][column]
        table[column].metadata.description = annotation["description"]
        table[column].metadata.unit = annotation["unit"]
        table[column].metadata.short_unit = annotation["short_unit"]

    for column in columns_to_export:
        assert table[column].metadata.unit is not None, "Unit should not be None here!"

    for ghe_cause_title in table.index.unique(level="ghe_cause_title").values:
        for sex_code in table.index.unique(level="sex_code").values:
            for agegroup_code in table.index.unique(level="agegroup_code").values:
                print(f"{ghe_cause_title} - {sex_code} - {agegroup_code}")
                # This is supposed to fix all dimensions except year and country_code to one excact value,
                # collapsing this part of the dataframe so that for exactly this dimension tuple all countries
                # and years are retrained and a Table with this subset is yielded
                idx = pd.IndexSlice
                cutout_table = table.loc[
                    idx[:, :, ghe_cause_title, sex_code, agegroup_code], :
                ]

                # drop the indices of the dimensions we fixed. The table to be yielded
                # should only have the year and entity_id index and one value column
                cutout_table.reset_index(level=4, drop=True, inplace=True)
                cutout_table.reset_index(level=3, drop=True, inplace=True)
                cutout_table.reset_index(level=2, drop=True, inplace=True)
                for column in columns_to_export:
                    short_name = slugify.slugify(
                        f"{column}-{ghe_cause_title}-{sex_code}-{agegroup_code}"
                    )

                    table_to_yield = cutout_table[[column]]
                    table_to_yield.metadata.short_name = short_name

                    # Safety check to see if the metadata is still intact
                    assert (
                        table_to_yield[column].metadata.unit is not None
                    ), "Unit should not be None here!"

                    yield table_to_yield
