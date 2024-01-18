from pathlib import Path
from typing import Any, Dict, Tuple

import requests
import structlog
import yaml
from fastapi import APIRouter, BackgroundTasks, HTTPException
from jsonschema import validate
from jsonschema.exceptions import ValidationError
from sqlmodel import Session

from apps.backport.datasync.datasync import upload_gzip_dict
from etl import config, paths
from etl import grapher_model as gm
from etl.command import main as etl_main
from etl.db import get_engine
from etl.files import yaml_dump
from etl.helpers import read_json_schema
from etl.metadata_export import merge_or_create_yaml, reorder_fields
from etl.paths import SCHEMAS_DIR

from .. import utils
from .schemas import Indicator, UpdateIndicatorRequest

log = structlog.get_logger()


engine = get_engine()

DATASET_SCHEMA = read_json_schema(path=SCHEMAS_DIR / "dataset-schema.json")


v1 = APIRouter()


@v1.get("/v1/health")
def health() -> dict:
    return {"status": "ok"}


@v1.put("/v1/indicators/{indicator_id}")
def update_indicator(indicator_id: int, update_request: UpdateIndicatorRequest, background_tasks: BackgroundTasks):
    _validate_data_api_url(update_request.dataApiUrl)

    # update YAML file
    with Session(engine) as session:
        db_indicator = gm.Variable.load_variable(session, indicator_id)

    if db_indicator.catalogPath is None:
        raise HTTPException(403, "Only indicators from the ETL can be edited. Contact us if you really need this.")

    override_yml_path, table_name, indicator_short_name = _parse_catalog_path(str(db_indicator.catalogPath))

    # convert incoming indicator updates into dictionary
    meta_dict = _indicator_metadata_dict(update_request.indicator, table_name, indicator_short_name)

    yaml_str = merge_or_create_yaml(
        meta_dict,
        override_yml_path,
    )

    # reorder YAML and dump it back again in nice format
    yaml_str = yaml_dump(reorder_fields(yaml.safe_load(yaml_str)))
    assert yaml_str

    # validate YAML against our schema
    try:
        validate(instance=yaml.safe_load(yaml_str), schema=DATASET_SCHEMA)
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=e.message)

    if not update_request.dryRun:
        with open(override_yml_path, "w") as f:
            f.write(yaml_str)

    if not update_request.dryRun:
        _update_indicator_in_r2(db_indicator, update_request.indicator)

    if update_request.triggerETL:
        background_tasks.add_task(_trigger_etl, indicator_short_name, db_indicator, update_request.dryRun)

    return {"yaml": yaml_str}


def _trigger_etl(indicator_short_name: str, db_indicator: gm.Variable, dry_run: bool):
    config.GRAPHER_FILTER = f"^{indicator_short_name}$"
    etl_main(
        steps=[str(db_indicator.catalogPath).rsplit("/", 1)[0]],
        grapher=True,
        workers=1,
        dry_run=dry_run,
    )


def _validate_data_api_url(data_api_url: str) -> None:
    if config.DATA_API_URL.strip("/") != data_api_url.strip("/"):
        raise HTTPException(
            422,
            f"Admin API uses Data API endpoint {data_api_url.strip('/')} which is incompatible with endpoint in ETL {config.DATA_API_URL}",
        )


def _indicator_metadata_dict(indicator: Indicator, table_name: str, indicator_short_name: str) -> Dict[str, Any]:
    indicator_update_dict = indicator.to_meta_dict()
    update_period_days = indicator_update_dict.pop("update_period_days", None)

    # create dictionary for metadata
    meta_dict = {"tables": {table_name: {"variables": {indicator_short_name: indicator_update_dict}}}}

    if update_period_days:
        meta_dict["dataset"] = {"update_period_days": update_period_days}

    return meta_dict


def _parse_catalog_path(catalog_path: str) -> Tuple[Path, str, str]:
    catalog_path, indicator_short_name = catalog_path.split("#")
    _path, table_name = catalog_path.rsplit("/", 1)

    base_path = paths.STEP_DIR / "data" / _path
    step_path = base_path.with_suffix(".py")
    override_yml_path = base_path.with_suffix(".meta.override.yml")

    if not step_path.exists():
        raise HTTPException(
            403, f"Dataset doesn't have file {step_path} and cannot be edited. Contact us if you really need this."
        )

    with open(step_path, "r") as f:
        step_contents = f.read()
        if "create_dataset" not in step_contents:
            raise HTTPException(
                403,
                "Dataset doesn't use `create_dataset` function and cannot be edited. Contact us if you really need this.",
            )

    return override_yml_path, table_name, indicator_short_name


def _update_indicator_in_r2(db_indicator: gm.Variable, indicator: Indicator) -> None:
    # download JSON file and update it
    indicator_dict = requests.get(db_indicator.s3_metadata_path(typ="http")).json()

    _deep_update(indicator_dict, indicator.dict(exclude_none=True))

    indicator_dict = utils.prune_none(indicator_dict)

    # update JSON file in Data API
    log.info("upload_to_r2", path=db_indicator.s3_metadata_path(typ="s3"))
    upload_gzip_dict(indicator_dict, db_indicator.s3_metadata_path(typ="s3"))


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
