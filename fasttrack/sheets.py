import concurrent.futures
from collections.abc import Iterable
from typing import Any, Dict, List, cast

import pandas as pd

from .yaml_meta import YAMLMeta


class ValidationError(Exception):
    pass


def import_google_sheets(url: str) -> Dict[str, Any]:
    # these IDs are taken from template sheet
    SHEET_TO_GID = {
        "data": 409110122,
        "variables_meta": 777328216,
        "dataset_meta": 1719161864,
        "sources_meta": 1399503534,
    }

    with concurrent.futures.ThreadPoolExecutor() as executor:
        data_future = executor.submit(lambda x: pd.read_csv(x), f"{url}&gid={SHEET_TO_GID['data']}")
        variables_meta_future = executor.submit(lambda x: pd.read_csv(x), f"{url}&gid={SHEET_TO_GID['variables_meta']}")
        dataset_meta_future = executor.submit(
            lambda x: pd.read_csv(x, header=None), f"{url}&gid={SHEET_TO_GID['dataset_meta']}"
        )
        sources_meta_future = executor.submit(lambda x: pd.read_csv(x), f"{url}&gid={SHEET_TO_GID['sources_meta']}")

    return {
        "data": data_future.result(),
        "variables_meta": variables_meta_future.result(),
        "dataset_meta": dataset_meta_future.result(),
        "sources_meta": sources_meta_future.result(),
    }


def parse_data_from_sheets(data_df: pd.DataFrame) -> pd.DataFrame:

    if "entity" in data_df.columns:
        data_df = data_df.rename(columns={"entity": "country"})

    if "year" not in data_df.columns:
        raise ValidationError("Missing column 'year' in data (is it lowercase?)")

    if "country" not in data_df.columns:
        raise ValidationError("Missing column 'country' in data (is it lowercase?)")

    return data_df.set_index(["country", "year"])


def parse_metadata_from_sheets(
    dataset_meta_df: pd.DataFrame, variables_meta_df: pd.DataFrame, sources_meta_df: pd.DataFrame
) -> YAMLMeta:
    sources_dict = cast(Dict[str, Any], sources_meta_df.set_index("short_name").to_dict())
    sources_dict = {k: _prune_empty(v) for k, v in sources_dict.items()}

    dataset_dict = _prune_empty(dataset_meta_df.set_index(0)[1].to_dict())  # type: ignore
    dataset_dict["namespace"] = "fasttrack"  # or should it be owid? or institution specific?
    dataset_dict.pop("updated")
    dataset_dict.setdefault("description", "")

    # manadatory dataset fields
    for key in ("title", "short_name", "version"):
        if key not in dataset_dict:
            raise ValidationError(f"Missing mandatory field '{key}' from sheet 'dataset_meta'")

    variables_list = [_prune_empty(v) for v in variables_meta_df.to_dict(orient="records")]  # type: ignore

    # default variable values
    for variable in variables_list:
        variable.setdefault("unit", "")

    # move display.* columns to display object
    for variable in variables_list:
        for k in list(variable.keys()):
            if k.startswith("display."):
                variable.setdefault("display", {})[k[8:]] = variable.pop(k)

    # expand sources
    if "sources" in dataset_dict:
        dataset_dict["sources"] = _expand_sources(dataset_dict["sources"], sources_dict)

    for variable in variables_list:
        if "sources" in variable:
            variable["sources"] = _expand_sources(variable["sources"], sources_dict)

    variables_dict = {v.pop("short_name"): v for v in variables_list}

    _move_keys_to_the_end(dataset_dict, ["description", "sources"])

    return YAMLMeta(**{"dataset": dataset_dict, "tables": {dataset_dict["short_name"]: {"variables": variables_dict}}})


def _expand_sources(sources_name: str, sources_dict: Dict[str, Any]) -> Iterable[str]:
    return [sources_dict[source_short_name] for source_short_name in map(lambda s: s.strip(), sources_name.split(","))]


def _move_keys_to_the_end(d: Dict[str, Any], keys: List[str]) -> None:
    for key in keys:
        if key in d:
            d[key] = d.pop(key)


def _prune_empty(d: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in d.items() if v and not pd.isnull(v)}
