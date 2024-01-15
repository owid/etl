from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import requests
import structlog
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlmodel import Session

from apps.backport.datasync.datasync import upload_gzip_dict
from etl import config, paths
from etl import grapher_model as gm
from etl.command import main as etl_main
from etl.db import get_engine
from etl.metadata_export import merge_or_create_yaml, reorder_fields

log = structlog.get_logger()

engine = get_engine()


class Indicator(BaseModel):
    name: Optional[str] = None
    # description: str | None = None
    # price: float
    # tax: float | None = None


class UpdateIndicatorRequest(BaseModel):
    indicator: Indicator
    dataApiUrl: str
    dryRun: bool = False
    triggerETL: bool = False


app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.put("/api/indicators/{indicator_id}")
def update_indicator(indicator_id: int, update_request: UpdateIndicatorRequest):
    if not update_request.dryRun:
        _update_indicator_in_r2(update_request.dataApiUrl, indicator_id, update_request.indicator.dict())

    # update YAML file
    with Session(engine) as session:
        indicator = gm.Variable.load_variable(session, indicator_id)

    override_yml_path, table_name, indicator_short_name = _parse_catalog_path(str(indicator.catalogPath))

    if override_yml_path.exists():
        raise NotImplementedError()

    # TODO: implement translation from JSON to YAML

    meta_dict = {"tables": {table_name: {"variables": {indicator_short_name: {"title": "ZZZ"}}}}}

    # reorder fields to have consistent YAML
    meta_dict = reorder_fields(meta_dict)

    yaml_str = merge_or_create_yaml(
        meta_dict,
        override_yml_path,
    )

    if not update_request.dryRun:
        with open(override_yml_path, "w") as f:
            f.write(yaml_str)

    if update_request.triggerETL:
        config.GRAPHER_FILTER = f"^{indicator_short_name}$"
        etl_main(
            steps=[str(indicator.catalogPath).rsplit("/")[0]],
            grapher=True,
            workers=1,
            force=True,
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


def _update_indicator_in_r2(data_api_url: str, indicator_id: int, indicator_update: dict) -> None:
    baked_variables_path = _get_baked_variables_path(data_api_url)

    # download JSON file and update it
    indicator_dict = requests.get(f"{data_api_url}/{indicator_id}.metadata.json").json()

    _deep_update(indicator_dict, indicator_update)

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
