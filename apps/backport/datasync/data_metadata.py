"""Building the JSON files ETL publishes for each indicator.

Only the data file is assembled here. `<id>.metadata.json` is built by the Grapher admin
API, which writes the rows it describes and so doesn't have to read them back.
"""

from copy import deepcopy
from typing import Any

import pandas as pd
from structlog import get_logger

log = get_logger()


def variable_data(data_df: pd.DataFrame) -> dict[str, Any]:
    data_df = data_df.rename(
        columns={
            "value": "values",
            "entityId": "entities",
            "year": "years",
        }
    )
    data = data_df[["values", "years", "entities"]].to_dict(orient="list")
    data["values"] = _convert_strings_to_numeric(data["values"])
    return data  # ty: ignore


def _convert_strings_to_numeric(lst: list[str]) -> list[int | float | str]:
    """Convert strings to numeric values. String `nan` remains as string."""
    result = []
    for item in lst:
        assert isinstance(item, str)
        if item.lower() == "nan":
            num = item
        else:
            try:
                num = float(item)
                if num.is_integer():
                    num = int(num)
            except ValueError:
                num = item
        result.append(num)
    return result


def _omit_nullable_values(d: dict) -> dict:
    out = {}
    for k, v in d.items():
        if isinstance(v, list):
            if len(v) > 0:
                out[k] = v
        elif v is not None and not pd.isna(v):
            out[k] = v
    return out


def filter_out_fields_in_metadata_for_checksum(meta: dict[str, Any]) -> dict[str, Any]:
    """Drop fields that are not needed to estimate the checksum."""
    meta_ = deepcopy(meta)

    # Drop checksums, they shouldn't be part of variable metadata, otherwise we get a
    # feedback loop with changing checksums
    meta_.pop("dataChecksum", None)
    meta_.pop("metadataChecksum", None)

    # Drop all IDs. If we create the same dataset on the staging server, it might have different
    # IDs, but the metadata should be the same.
    meta_.pop("id", None)
    meta_.pop("datasetId", None)
    for origin in meta_.get("origins", []):
        origin.pop("id", None)

    # Ignore updatedAt timestamps
    meta_.pop("updatedAt", None)

    return meta_
