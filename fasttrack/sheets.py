import concurrent.futures
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple, cast

import pandas as pd
from pydantic import BaseModel

from etl.grapher_helpers import INT_TYPES

from .yaml_meta import YAMLMeta


class ValidationError(Exception):
    pass


class PartialSnapshotMeta(BaseModel):
    url: str
    publication_year: Optional[int]
    license_url: Optional[str]
    license_name: Optional[str]


# these IDs are taken from template sheet, they will be different if someone
# creates a new sheet from scratch and use those names
SHEET_TO_GID = {
    "data": 409110122,
    "raw_data": 901452831,
    "variables_meta": 777328216,
    "dataset_meta": 1719161864,
    "sources_meta": 1399503534,
}


def import_google_sheets(url: str) -> Dict[str, Any]:
    # read dataset first to check if we're using data_url instead of data sheet
    dataset_meta = pd.read_csv(f"{url}&gid={SHEET_TO_GID['dataset_meta']}", header=None)
    data_url = _get_data_url(dataset_meta, url)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        data_future = executor.submit(lambda x: pd.read_csv(x), data_url)
        variables_meta_future = executor.submit(lambda x: pd.read_csv(x), f"{url}&gid={SHEET_TO_GID['variables_meta']}")
        sources_meta_future = executor.submit(lambda x: pd.read_csv(x), f"{url}&gid={SHEET_TO_GID['sources_meta']}")

    return {
        "data": data_future.result(),
        "variables_meta": variables_meta_future.result(),
        "dataset_meta": dataset_meta,
        "sources_meta": sources_meta_future.result(),
    }


def parse_data_from_sheets(data_df: pd.DataFrame) -> pd.DataFrame:
    # lowercase columns names
    for col in data_df.columns:
        if col.lower() in ("entity", "year", "country"):
            data_df.rename(columns={col: col.lower()}, inplace=True)

    if "entity" in data_df.columns:
        data_df = data_df.rename(columns={"entity": "country"})

    if "year" not in data_df.columns:
        raise ValidationError("Missing column 'year' in data")

    if "country" not in data_df.columns:
        raise ValidationError("Missing column 'country' in data")

    # check types
    if data_df.year.dtype not in INT_TYPES:
        raise ValidationError("Column 'year' should be integer")

    return data_df.set_index(["country", "year"])


def parse_metadata_from_sheets(
    dataset_meta_df: pd.DataFrame, variables_meta_df: pd.DataFrame, sources_meta_df: pd.DataFrame
) -> Tuple[YAMLMeta, PartialSnapshotMeta]:
    sources_dict = cast(Dict[str, Any], sources_meta_df.set_index("short_name").to_dict())
    sources_dict = {k: _prune_empty(v) for k, v in sources_dict.items()}

    # publisher_source is not used anymore
    for source in sources_dict.values():
        source.pop("publisher_source", None)

    dataset_dict = _prune_empty(dataset_meta_df.set_index(0)[1].to_dict())  # type: ignore
    dataset_dict["namespace"] = "fasttrack"  # or should it be owid? or institution specific?
    dataset_dict.pop("updated")
    dataset_dict.pop("external_csv", None)
    dataset_dict.setdefault("description", "")

    try:
        if dataset_dict["version"] != "latest":
            dt.datetime.strptime(dataset_dict["version"], "%Y-%m-%d")
    except ValueError:
        raise ValidationError(f"Version `{dataset_dict['version']}` is not in YYYY-MM-DD format")

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

    # extract fields for snapshot
    # NOTE: we used to have special fields in dataset_meta for `url` and `publication_year`, but these
    # are the same fields as in source so we use these instead
    if len(dataset_dict.get("sources", [])) > 0:
        dataset_source = dataset_dict["sources"][0]
    else:
        dataset_source = {}

    partial_snapshot_meta = _prune_empty(
        {
            # "publication_year": dataset_dict.pop("publication_year", None),
            "publication_year": dataset_source.get("publication_year", None),
            "license_url": dataset_dict.pop("license_url", None),
            "license_name": dataset_dict.pop("license_name", None),
        }
    )
    partial_snapshot_meta["url"] = dataset_source.get("url", "")

    _move_keys_to_the_end(dataset_dict, ["description", "sources"])

    return (
        YAMLMeta(**{"dataset": dataset_dict, "tables": {dataset_dict["short_name"]: {"variables": variables_dict}}}),
        PartialSnapshotMeta(**partial_snapshot_meta),
    )


def _expand_sources(sources_name: str, sources_dict: Dict[str, Any]) -> List[Dict[str, Any]]:
    sources = []
    for source_short_name in map(lambda s: s.strip(), sources_name.split(",")):
        try:
            sources.append(sources_dict[source_short_name])
        except KeyError:
            raise ValidationError(f"Source with short_name `{source_short_name}` not found in `sources_meta` sheet")
    return sources


def _move_keys_to_the_end(d: Dict[str, Any], keys: List[str]) -> None:
    for key in keys:
        if key in d:
            d[key] = d.pop(key)


def _prune_empty(d: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in d.items() if v is not None and v != "" and not pd.isnull(v)}


def _get_data_url(dataset_meta: pd.DataFrame, url: str) -> str:
    """Get data url from dataset_meta field or use data sheet if dataset_meta is empty."""
    data_url = dataset_meta.set_index(0)[1].to_dict().get("external_csv")

    if data_url and not pd.isnull(data_url):
        # files on Google Drive need modified link for downloading raw csv
        if "drive.google.com" in data_url:
            data_url = data_url.replace("file/d/", "uc?id=").replace("/view?usp=share_link", "&export=download")
    else:
        # use data sheet
        data_url = f"{url}&gid={SHEET_TO_GID['data']}"

    return data_url
