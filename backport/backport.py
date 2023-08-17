import datetime as dt
import json
from typing import Any, List, Optional

import click
import pandas as pd
import structlog
from git.exc import GitCommandError
from git.repo import Repo
from owid.catalog import Source
from sqlalchemy.engine import Engine
from sqlmodel import Session

from backport.datasync.data_metadata import (
    _variable_metadata,
    variable_data,
    variable_data_df_from_s3,
)
from backport.datasync.datasync import upload_gzip_dict
from etl import config
from etl import grapher_model as gm
from etl import paths
from etl.backport_helpers import GrapherConfig
from etl.db import get_engine
from etl.files import checksum_str
from etl.snapshot import Snapshot, SnapshotMeta

from . import utils

config.enable_bugsnag()

log = structlog.get_logger()


@click.command()
@click.option("--dataset-id", type=int, required=True)
@click.option(
    "--force/--no-force",
    default=False,
    type=bool,
    help="Force overwrite even if checksums match",
)
@click.option(
    "--dry-run/--no-dry-run",
    default=False,
    type=bool,
    help="Do not add dataset to a catalog on dry-run",
)
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to S3 as snapshot",
)
@click.option(
    "--data-metadata/--skip-data-metadata",
    default=False,
    type=bool,
    help="Upload data & metadata JSON of variable to R2",
)
def backport_cli(
    dataset_id: int,
    force: bool = False,
    dry_run: bool = False,
    upload: bool = True,
    data_metadata: bool = False,
) -> None:
    return backport(
        dataset_id=dataset_id,
        force=force,
        dry_run=dry_run,
        upload=upload,
        data_metadata=data_metadata,
    )


class PotentialBackport:
    dataset_id: int
    ds: gm.Dataset
    config: GrapherConfig
    md5_config: str
    variable_ids: List[int]

    def __init__(self, dataset_id: int):
        self.dataset_id = dataset_id

    def load(self, engine: Engine) -> None:
        # get data from database
        with Session(engine) as session:
            ds = gm.Dataset.load_dataset(session, self.dataset_id)
            vars = gm.Dataset.load_variables_for_dataset(session, self.dataset_id)

            self.variable_ids = [v.id for v in vars if v.id]

            # get sources for dataset and all variables
            sources = gm.Source.load_sources(session, dataset_id=ds.id, variable_ids=self.variable_ids)

        self.ds = ds
        self.config = GrapherConfig.from_grapher_objects(ds, vars, sources)
        self.md5_config = checksum_str(self.config.to_json())

    @property
    def short_name(self) -> str:
        return utils.create_short_name(self.dataset_id, self.ds.name)

    @property
    def public(self) -> bool:
        return not self.ds.isPrivate

    @property
    def config_snapshot(self) -> Snapshot:
        return Snapshot(f"backport/latest/{self.short_name}_config.json")

    def needs_update(self) -> bool:
        # find existing entry in catalog
        try:
            snap = self.config_snapshot
        except FileNotFoundError:
            return True

        # compare checksums
        # since `dataEditedAt` is part of config, its checksum should change
        try:
            return snap.metadata.md5 != self.md5_config
        except ValueError:
            return True

    def upload(self, upload: bool, dry_run: bool, engine: Engine) -> None:
        repo = Repo(paths.BASE_DIR)

        config_metadata = _snapshot_config_metadata(self.ds, self.short_name, self.public)
        config_metadata.save()
        try:
            _upload_config_to_snapshot(
                self.config,
                config_metadata,
                dry_run,
                upload,
            )
        except (KeyboardInterrupt, Exception) as e:
            # rollback metadata file
            try:
                repo.git.checkout("HEAD", config_metadata.path)
            except GitCommandError:
                pass
            raise e

        # upload values to snapshot
        df = _load_values(engine, self.variable_ids)
        values_metadata = _snapshot_values_metadata(self.ds, self.short_name, self.public)
        values_metadata.save()
        try:
            _upload_values_to_snapshot(
                df,
                values_metadata,
                dry_run,
                upload,
            )
        except (KeyboardInterrupt, Exception) as e:
            # rollback metadata file
            try:
                repo.git.checkout("HEAD", values_metadata.path)
            except GitCommandError:
                pass
            raise e


def backport(
    dataset_id: int,
    force: bool = False,
    dry_run: bool = False,
    upload: bool = True,
    data_metadata: bool = False,
    engine: Optional[Engine] = None,
) -> None:
    lg = log.bind(dataset_id=dataset_id)

    engine = engine or get_engine()

    dataset = PotentialBackport(dataset_id)
    lg.info("backport.loading_dataset")
    dataset.load(engine)

    if not force:
        if not dataset.needs_update():
            lg.info(
                "backport.skip",
                short_name=dataset.short_name,
                reason="checksums match",
                checksum=dataset.md5_config,
            )
            lg.info("backport.finished")
            return

    dataset.upload(upload, dry_run, engine)

    lg.info(
        "backport.upload",
        upload=upload,
        dry_run=dry_run,
        public=dataset.public,
    )

    if data_metadata:
        _upload_data_metadata(lg, dataset.short_name, dry_run)
        lg.info(
            "backport.data_metadata.finished",
            dry_run=dry_run,
        )

    lg.info("backport.finished")


def _upload_data_metadata(lg: Any, backport_short_name: str, dry_run: bool) -> None:
    lg.info("backport.data_metadata.pull_snapshots.start")
    snap_values = Snapshot(f"backport/latest/{backport_short_name}_values.feather")
    snap_values.pull()
    snap_config = Snapshot(f"backport/latest/{backport_short_name}_config.json")
    snap_config.pull()
    lg.info("backport.data_metadata.pull_snapshots.end")

    values = (
        pd.read_feather(snap_values.path)
        .rename(
            columns={
                "entity_id": "entityId",
                "entity_name": "entityName",
                "entity_code": "entityCode",
                "variable_name": "variableName",
            }
        )
        .astype({"value": str})
    )

    with open(snap_config.path, "r") as f:
        config = json.load(f)

    variables = config["variables"]
    sources = config["sources"]
    dataset = config["dataset"]

    for db_variable_row in variables:
        assert db_variable_row["schemaVersion"] == 1, "Only metadata schema version 1 is supported"

        # find source and fill missing data
        source = [s for s in sources if s["id"] == db_variable_row["sourceId"]][0]
        db_variable_row["sourceName"] = source["name"]
        db_variable_row["sourceDescription"] = source["description"]

        # get dataset and fill missing data
        db_variable_row["datasetName"] = dataset["name"]
        db_variable_row["datasetVersion"] = dataset["version"]
        db_variable_row["updatePeriodDays"] = dataset["updatePeriodDays"]
        db_variable_row["nonRedistributable"] = dataset["nonRedistributable"]

        # encode dicts as JSON
        db_variable_row["display"] = json.dumps(db_variable_row["display"])
        db_variable_row["sourceDescription"] = json.dumps(db_variable_row["sourceDescription"])

        # use date types
        db_variable_row["createdAt"] = pd.to_datetime(db_variable_row["createdAt"])
        db_variable_row["updatedAt"] = pd.to_datetime(db_variable_row["updatedAt"])

        # add values
        var_data = values[values.variable_id == db_variable_row["id"]]

        # artificial variable with id just to get s3 paths
        db_var = gm.Variable(
            id=db_variable_row["id"],
            datasetId=1,
            unit="",
            coverage="",
            timespan="",
            sourceId=0,
            display={},
            dimensions=None,
        )

        upload_variable_data = variable_data(var_data)
        if not dry_run:
            upload_gzip_dict(upload_variable_data, db_var.s3_data_path(), r2=True)

        upload_variable_metadata = _variable_metadata(
            db_variable_row=db_variable_row,
            variable_data=var_data,
            db_origins_df=pd.DataFrame(),
            db_topic_tags=[],
            db_faqs=[],
        )
        if not dry_run:
            upload_gzip_dict(upload_variable_metadata, db_var.s3_metadata_path(), r2=True)


def _snapshot_values_metadata(ds: gm.Dataset, short_name: str, public: bool) -> SnapshotMeta:
    """Create walden dataset for grapher dataset values.
    These datasets are not meant for direct consumption from the catalog, but rather
    for postprocessing in etl.
    :param short_name: short name of the dataset in catalog
    """
    return SnapshotMeta(
        namespace="backport",
        short_name=f"{short_name}_values",
        version="latest",
        name=ds.name,
        description=ds.description,
        source=Source(
            name="Our World in Data catalog backport",
            published_by="Our World in Data catalog backport",
            url=f"https://owid.cloud/admin/datasets/{ds.id}",
            publication_date="latest",
            date_accessed=dt.datetime.utcnow(),
        ),
        file_extension="feather",
        is_public=public,
    )


def _snapshot_config_metadata(ds: gm.Dataset, short_name: str, public: bool) -> SnapshotMeta:
    """Create walden dataset for grapher dataset variables and metadata."""
    config = _snapshot_values_metadata(ds, short_name, public)
    config.short_name = short_name + "_config"
    config.name = f"Grapher metadata for {short_name}"
    config.file_extension = "json"

    if ds.isArchived == 1:
        config.name += " (archived)"

    return config


def _load_values(engine: Engine, variable_ids: list[int]) -> pd.DataFrame:
    """Get data values of a variable."""
    q = """
    select
        v.id as variable_id,
        v.name as variable_name
    from variables as v
    where v.id in %(variable_ids)s
    """
    df = variable_data_df_from_s3(engine, variable_ids=variable_ids)
    df = df.rename(
        columns={
            "entityId": "entity_id",
            "variableId": "variable_id",
            "entityName": "entity_name",
            "entityCode": "entity_code",
        }
    )
    vf: pd.DataFrame = pd.read_sql(q, engine, params={"variable_ids": variable_ids})
    df = df.merge(vf, on="variable_id")

    # try converting values to float if possible, this can make the data 50% smaller
    # if successful
    df["value"] = df["value"].astype(float, errors="ignore")

    # use categoricals for smaller files (significant reduction even with compression)
    df = df.astype(
        {
            "variable_id": "category",
            "variable_name": "category",
            "entity_id": "category",
            "entity_name": "category",
            "entity_code": "category",
        }
    )

    # special case for empty dataframes (would fail otherwise when saving to feather)
    if df.empty:
        df = df.reset_index(drop=True)

    return df


def _upload_config_to_snapshot(
    config: GrapherConfig,
    snap_meta: SnapshotMeta,
    dry_run: bool,
    upload: bool,
) -> None:
    snap = Snapshot(snap_meta.uri)
    snap.path.parent.mkdir(parents=True, exist_ok=True)
    with open(snap.path, "w") as f:
        f.write(config.to_json())

    if not dry_run:
        snap.dvc_add(upload=upload)


def _upload_values_to_snapshot(
    df: pd.DataFrame,
    snap_meta: SnapshotMeta,
    dry_run: bool,
    upload: bool,
) -> None:
    snap = Snapshot(snap_meta.uri)
    df.to_feather(snap.path, compression="lz4")
    if not dry_run:
        snap.dvc_add(upload=upload)


if __name__ == "__main__":
    # Example (run against staging DB):
    #   backport --dataset-id 5426 --dry-run --force
    backport_cli()
