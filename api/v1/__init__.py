from pathlib import Path
from typing import Any, Dict, Tuple

import requests
import structlog
import yaml
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request
from jsonschema import validate
from jsonschema.exceptions import ValidationError
from sqlmodel import Session

from apps.backport.datasync.datasync import upload_gzip_dict
from etl import config, paths
from etl import grapher_model as gm
from etl.command import main as etl_main
from etl.db import get_engine
from etl.helpers import read_json_schema
from etl.metadata_export import merge_or_create_yaml, reorder_fields
from etl.paths import SCHEMAS_DIR

from .. import utils
from .schemas import Indicator, UpdateIndicatorRequest

log = structlog.get_logger()


engine = get_engine()

DATASET_SCHEMA = read_json_schema(path=SCHEMAS_DIR / "dataset-schema.json")


v1 = APIRouter()


@v1.put("/v1/indicators/{indicator_id}")
def update_indicator(indicator_id: int, update_request: UpdateIndicatorRequest):
    # update YAML file
    with Session(engine) as session:
        indicator = gm.Variable.load_variable(session, indicator_id)

    override_yml_path, table_name, indicator_short_name = _parse_catalog_path(str(indicator.catalogPath))

    # create dictionary for metadata
    meta_dict = {"tables": {table_name: {"variables": {indicator_short_name: update_request.indicator.to_meta_dict()}}}}

    # reorder fields to have consistent YAML
    meta_dict = reorder_fields(meta_dict)

    yaml_str = merge_or_create_yaml(
        meta_dict,
        override_yml_path,
    )

    # validate YAML against our schema
    try:
        validate(instance=yaml.safe_load(yaml_str), schema=DATASET_SCHEMA)
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=e.message)

    if not update_request.dryRun:
        with open(override_yml_path, "w") as f:
            f.write(yaml_str)

    if not update_request.dryRun:
        _update_indicator_in_r2(update_request.dataApiUrl, indicator_id, update_request.indicator)

    if update_request.triggerETL:
        config.GRAPHER_FILTER = f"^{indicator_short_name}$"
        etl_main(
            steps=[str(indicator.catalogPath).rsplit("/", 1)[0]], grapher=True, workers=1, dry_run=update_request.dryRun
        )

    return {"yaml": yaml_str}


def _parse_catalog_path(catalog_path: str) -> Tuple[Path, str, str]:
    catalog_path, indicator_short_name = catalog_path.split("#")
    _path, table_name = catalog_path.rsplit("/", 1)

    base_path = paths.STEP_DIR / "data" / _path
    step_path = base_path.with_suffix(".py")
    override_yml_path = base_path.with_suffix(".meta.override.yml")

    if not step_path.exists():
        print("Bad luck")

    with open(step_path, "r") as f:
        step_contents = f.read()
        if "create_dataset" not in step_contents:
            print("Bad luck")

    return override_yml_path, table_name, indicator_short_name


def _get_baked_variables_path(data_api_url: str) -> str:
    if data_api_url.startswith("https://api-staging.owid.io"):
        data_api_env = data_api_url.split("/")[3]
        return f"s3://owid-api-staging/{data_api_env}/v1/indicators"
    elif data_api_url == "https://api.ourworldindata.org/v1/indicators":
        data_api_env = "production"
        return "s3://owid-api/v1/indicators"
    else:
        raise HTTPException(status_code=400, detail="Invalid dataApiUrl")


def _update_indicator_in_r2(data_api_url: str, indicator_id: int, indicator: Indicator) -> None:
    baked_variables_path = _get_baked_variables_path(data_api_url)

    # download JSON file and update it
    indicator_dict = requests.get(f"{data_api_url}/{indicator_id}.metadata.json").json()

    _deep_update(indicator_dict, indicator.dict(exclude_none=True))

    indicator_dict = utils.prune_none(indicator_dict)

    # update JSON file in Data API
    log.info("upload_to_r2", path=f"{baked_variables_path}/{indicator_id}.metadata.json")
    upload_gzip_dict(indicator_dict, f"{baked_variables_path}/{indicator_id}.metadata.json")


def _deep_update(original: Dict[Any, Any], update: Dict[Any, Any]) -> None:
    """
    Recursively update a dict with nested dicts.

    :param original: The original dictionary to be updated.
    :param update: The dictionary containing updates.
    """
    for key, value in update.items():
        if isinstance(value, dict) and key in original:
            _deep_update(original[key], value)
        else:
            original[key] = value
