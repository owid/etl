from pathlib import Path
from typing import Any, Dict

import requests
import structlog
import yaml
from fastapi import APIRouter, BackgroundTasks, HTTPException
from git import PushInfo
from git.repo import Repo
from jsonschema import validate
from jsonschema.exceptions import ValidationError
from sqlalchemy.exc import NoResultFound
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


@v1.get("/api/v1/health")
def health() -> dict:
    return {"status": "ok"}


@v1.put("/api/v1/indicators")
def update_indicator(update_request: UpdateIndicatorRequest, background_tasks: BackgroundTasks):
    _validate_data_api_url(update_request.dataApiUrl)

    # if no changes, return empty YAML
    if update_request.indicator.to_meta_dict() == {}:
        return {"yaml": ""}

    # load indicator by catalog path
    db_indicator = _load_and_validate_indicator(update_request.catalogPath)

    # convert incoming indicator updates into dictionary
    meta_dict = _indicator_metadata_dict(update_request.indicator, db_indicator)

    # create YAML override string
    yaml_str = _generate_yaml_string(meta_dict, db_indicator.override_yaml_path)

    # NOTE: the order of all updating "tracks" is random, we can rethink it if we find
    # ourselves running into race conditions.
    if not update_request.dryRun:
        with open(db_indicator.override_yaml_path, "w") as f:
            f.write(yaml_str)

        # try to commit and push before overwriting file in R2
        if config.ETL_API_COMMIT:
            _commit_and_push(db_indicator.override_yaml_path, ":robot: Metadata update by Admin")

    if not update_request.dryRun:
        _update_indicator_in_r2(db_indicator, update_request.indicator)

    # trigger ETL in the background (this is usually fast, but for some datasets this could take a while)
    if update_request.triggerETL:
        background_tasks.add_task(_trigger_etl, db_indicator, update_request.dryRun)

    return {"yaml": yaml_str}


def _generate_yaml_string(meta_dict: Dict[str, Any], override_yml_path: Path) -> str:
    yaml_str = merge_or_create_yaml(
        meta_dict,
        override_yml_path,
        delete_empty=True,
    )

    # reorder YAML and dump it back again in nice format
    yaml_str = yaml_dump(reorder_fields(yaml.safe_load(yaml_str)))
    assert yaml_str

    # validate YAML against our schema
    try:
        validate(instance=yaml.safe_load(yaml_str), schema=DATASET_SCHEMA)
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=e.message)

    return yaml_str


def _load_and_validate_indicator(catalog_path: str) -> gm.Variable:
    # update YAML file
    with Session(engine) as session:
        try:
            db_indicator = gm.Variable.load_from_catalog_path(session, catalog_path)
        except NoResultFound:
            raise HTTPException(
                404,
                f"Indicator with catalogPath {catalog_path} not found.",
            )

    if db_indicator.catalogPath is None:
        raise HTTPException(403, "Only indicators from the ETL can be edited. Contact us if you really need this.")

    if not db_indicator.step_path.exists():
        raise HTTPException(
            403,
            f"Dataset doesn't have file {db_indicator.step_path} and cannot be edited. Contact us if you really need this.",
        )

    with open(db_indicator.step_path, "r") as f:
        step_contents = f.read()
        if "create_dataset" not in step_contents:
            raise HTTPException(
                403,
                "Dataset doesn't use `create_dataset` function and cannot be edited. Contact us if you really need this.",
            )

    return db_indicator


def _commit_and_push(file_path: Path, commit_message: str) -> None:
    repo = Repo(paths.BASE_DIR)
    repo.git.add(file_path)

    # Check if there are changes staged for commit
    if not repo.index.diff("HEAD"):
        log.info("No changes to commit", file_path=file_path)
        return

    repo.index.commit(commit_message)
    log.info("git.commit", file_path=file_path)
    origin = repo.remote(name="origin")
    origin.fetch()
    repo.git.rebase("origin/master")
    push_info_list = origin.push()

    # Check each PushInfo result for errors or rejections
    for info in push_info_list:
        if info.flags & PushInfo.ERROR:
            raise GitError(f"Push failed with error: {info.summary}")
        elif info.flags & PushInfo.REJECTED:
            raise GitError(f"Push rejected: {info.summary}")
        else:
            log.info(f"Pushed successfully: {info.summary}")

    log.info("git.push", msg=commit_message)


def _trigger_etl(db_indicator: gm.Variable, dry_run: bool) -> None:
    config.GRAPHER_FILTER = f"^{db_indicator.shortName}$"
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


def _indicator_metadata_dict(indicator: Indicator, db_indicator: gm.Variable) -> Dict[str, Any]:
    indicator_update_dict = indicator.to_meta_dict()
    update_period_days = indicator_update_dict.pop("update_period_days", None)

    # if indicator has dimensions, use its original name
    original_short_name = (db_indicator.dimensions or {}).get("originalShortName")
    short_name = original_short_name or db_indicator.shortName

    # create dictionary for metadata
    meta_dict = {"tables": {db_indicator.table_name: {"variables": {short_name: indicator_update_dict}}}}

    if update_period_days:
        meta_dict["dataset"] = {"update_period_days": update_period_days}

    # remove empty description keys
    for table in meta_dict["tables"].values():
        for variable in table["variables"].values():
            if "description_key" in variable:
                variable["description_key"] = [k for k in variable["description_key"] if k]

    return meta_dict


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


class GitError(Exception):
    pass
