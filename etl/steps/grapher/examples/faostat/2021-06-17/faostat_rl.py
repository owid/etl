import pandas as pd
from owid import catalog
from collections.abc import Iterable
from pathlib import Path
from typing import cast

from etl.paths import DATA_DIR
from etl import grapher_helpers as gh


annotations_path = Path(__file__).parent / "annotations.yml"


def get_grapher_dataset() -> catalog.Dataset:
    dataset = catalog.Dataset(DATA_DIR / "garden/faostat/2021-06-17/faostat_rl")
    dataset.metadata.short_name = "faostat_rl-2021-06-17"
    dataset.metadata.namespace = "faostat"
    return dataset


def get_grapher_tables(dataset: catalog.Dataset) -> Iterable[catalog.Table]:
    orig_table = dataset["land_use"]

    t = (
        pd.DataFrame(orig_table.reset_index())
        .assign(
            year=_extract_year,
            unit=_extract_unit,
            value=lambda df: df.Value,
            variable=lambda df: df.Item,
            entity_id=_extract_entity_id,
        )
        .pipe(_cleanup)
        .set_index(["entity_id", "year", "Flag"])[["value", "unit", "variable"]]
    )

    # Get metadata from the original table and optional annotation file
    t = gh.as_table(t, orig_table)
    annot = gh.Annotation.load_from_yaml(annotations_path)

    yield from gh.yield_long_table(t, annot=annot)


def _extract_year(df: pd.DataFrame) -> pd.Series:
    return cast(pd.Series, df.Year.astype(int))


def _extract_unit(df: pd.DataFrame) -> pd.Series:
    return cast(pd.Series, df.Element.astype(str) + "/" + df.Unit.astype(str))


def _extract_entity_id(df: pd.DataFrame) -> pd.Series:
    reference_dataset = catalog.Dataset(DATA_DIR / "reference")
    countries_regions = reference_dataset["countries_regions"]
    country_map = countries_regions.set_index("name")["legacy_entity_id"]
    return cast(pd.Series, df.Area.map(country_map))


def _cleanup(df: pd.DataFrame) -> pd.DataFrame:
    return df.dropna(subset=["entity_id"]).astype({"entity_id": int})
