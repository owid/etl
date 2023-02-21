from typing import List, Tuple

import pandas as pd
from owid.catalog.utils import underscore

from .sheets import PartialSnapshotMeta, parse_data_from_sheets
from .yaml_meta import YAMLMeta


def parse_data_from_csv(csv_df: pd.DataFrame) -> pd.DataFrame:
    data = parse_data_from_sheets(csv_df)

    # underscore columns from CSV
    for col in data.columns:
        data = data.rename(columns={col: underscore(col)})

    return data


def parse_metadata_from_csv(filename: str, columns: List[str]) -> Tuple[YAMLMeta, PartialSnapshotMeta]:
    filename = filename.replace(".csv", "")
    title = f"DRAFT {filename}"
    dataset_dict = {
        "title": title,
        "short_name": underscore(filename),
        "version": "latest",
        "namespace": "fasttrack",
        "description": "",
    }

    variables_dict = {
        underscore(col): {
            "title": col,
            "unit": "",
        }
        for col in columns
        if col.lower() not in ("country", "year", "entity")
    }

    return (
        YAMLMeta(**{"dataset": dataset_dict, "tables": {dataset_dict["short_name"]: {"variables": variables_dict}}}),
        PartialSnapshotMeta(
            url="",
            publication_year=None,
            license_url=None,
            license_name=None,
        ),
    )
