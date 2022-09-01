from pathlib import Path
from typing import Iterable

import structlog
from owid.catalog import Dataset, Table

from etl import grapher_helpers as gh
from etl.paths import DATA_DIR

log = structlog.get_logger()


def get_grapher_dataset() -> Dataset:
    version = Path(__file__).parent.stem
    fname = Path(__file__).stem
    namespace = Path(__file__).parent.parent.stem
    dataset = Dataset(DATA_DIR / f"garden/{namespace}/{version}/{fname}")
    # short_name should include dataset name and version
    dataset.metadata.short_name = f"{dataset.metadata.short_name}__{version.replace('-', '_')}"

    return dataset


def get_grapher_tables(dataset: Dataset) -> Iterable[Table]:
    fname = Path(__file__).stem
    table = dataset[fname]
    assert len(table.metadata.primary_key) == 2

    table.reset_index(inplace=True)
    table["entity_id"] = gh.country_to_entity_id(table["country"], create_entities=True)
    table = table.drop(columns=["country"]).set_index(["year", "entity_id"])

    for col in table.columns:
        tb = table[[col]].dropna()
        if tb.shape[0]:
            yield tb  # type: ignore
