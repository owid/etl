from typing import Any, Dict, List, Tuple

import pandas as pd
from owid.catalog import DatasetMeta, VariableMeta
from owid.catalog.utils import underscore

from .sheets import parse_data_from_sheets


def parse_data_from_csv(csv_df: pd.DataFrame) -> pd.DataFrame:
    data = parse_data_from_sheets(csv_df)

    # underscore columns from CSV
    for col in data.columns:
        data = data.rename(columns={col: underscore(col)})

    return data


def parse_metadata_from_csv(filename: str, columns: List[str]) -> Tuple[DatasetMeta, Dict[str, VariableMeta]]:
    filename = filename.replace(".csv", "")
    title = f"DRAFT {filename}"
    dataset_dict: dict[str, Any] = {
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

    dataset_dict["sources"] = [
        dict(
            name="Unknown",
            url="",
            publication_year=None,
        )
    ]
    dataset_dict["licenses"] = [
        {
            "url": None,
            "name": None,
        }
    ]

    return DatasetMeta(**dataset_dict), {k: VariableMeta(**v) for k, v in variables_dict.items()}
