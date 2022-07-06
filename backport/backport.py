import os
import tempfile
import datetime as dt
from typing import Optional, cast

import click
import pandas as pd
import structlog
from owid.catalog.utils import validate_underscore
from owid.walden import Catalog as WaldenCatalog
from owid.walden.catalog import Dataset as WaldenDataset
from owid.walden.ingest import add_to_catalog
from sqlalchemy.engine import Engine

from etl.db import get_engine
from etl import config
from etl.files import checksum_str
from etl.grapher_model import (
    GrapherConfig,
    GrapherDatasetModel,
    GrapherSourceModel,
    GrapherVariableModel,
)

config.enable_bugsnag()

WALDEN_NAMESPACE = os.environ.get("WALDEN_NAMESPACE", "backport")

log = structlog.get_logger()

engine = get_engine()

# preload walden catalog to improve performance (initializing the catalog takes a while)
walden_catalog = WaldenCatalog()


def _walden_values_metadata(ds: GrapherDatasetModel, short_name: str) -> WaldenDataset:
    """Create walden dataset for grapher dataset values.
    These datasets are not meant for direct consumption from the catalog, but rather
    for postprocessing in etl.
    :param short_name: short name of the dataset in catalog
    """
    return WaldenDataset(
        namespace=WALDEN_NAMESPACE,
        short_name=f"{short_name}_values",
        name=ds.name,
        date_accessed=dt.datetime.utcnow().isoformat(),
        description=ds.description,
        source_name="Our World in Data catalog backport",
        url=f"https://owid.cloud/admin/datasets/{ds.id}",
        publication_date="latest",
        file_extension="feather",
    )


def _walden_config_metadata(
    ds: GrapherDatasetModel, short_name: str, origin_md5: str
) -> WaldenDataset:
    """Create walden dataset for grapher dataset variables and metadata."""
    config = _walden_values_metadata(ds, short_name)
    config.short_name = short_name + "_config"
    config.name = f"Grapher metadata for {short_name}"
    config.file_extension = "json"
    config.origin_md5 = origin_md5
    return config


def _load_values(engine: Engine, variable_ids: list[int]) -> pd.DataFrame:
    """Get data values of a variable."""
    q = """
    select
        d.entityId as entity_id,
        d.variableId as variable_id,
        -- it would be more efficient to load entity name and variable name separately and
        -- then join it before uploading to walden
        e.name as entity_name,
        e.code as entity_code,
        v.name as variable_name,
        d.year,
        d.value as value
    from data_values as d
    join entities as e on e.id = d.entityId
    join variables as v on v.id = d.variableId
    where d.variableId in %(variable_ids)s
    """
    df: pd.DataFrame = pd.read_sql(q, engine, params={"variable_ids": variable_ids})

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


def _load_config(
    ds: GrapherDatasetModel,
    vars: list[GrapherVariableModel],
    sources: list[GrapherSourceModel],
) -> GrapherConfig:
    """Get the configuration of a variable."""
    return GrapherConfig(
        dataset=ds,
        variables=vars,
        sources=sources,
    )


def _upload_config_to_walden(
    config: GrapherConfig,
    meta: WaldenDataset,
    dry_run: bool,
    upload: bool,
    public: bool,
) -> None:
    with tempfile.NamedTemporaryFile(mode="w") as f:
        f.write(config.json())
        f.flush()

        if not dry_run:
            add_to_catalog(meta, f.name, upload, public=public)


def _upload_values_to_walden(
    df: pd.DataFrame,
    meta: WaldenDataset,
    dry_run: bool,
    upload: bool,
    public: bool,
) -> None:
    with tempfile.NamedTemporaryFile(mode="wb") as f:
        df.to_feather(f.name, compression="lz4")
        if not dry_run:
            add_to_catalog(meta, f.name, upload, public=public)


def _checksum_match(short_name: str, md5: str) -> bool:
    try:
        walden_ds = walden_catalog.find_one(short_name=short_name)
    except KeyError:
        # datasets not found in catalog
        return False

    return (walden_ds.origin_md5 or "") == md5


def _walden_timestamp(short_name: str) -> dt.datetime:
    t = walden_catalog.find_one(short_name=short_name).date_accessed
    return cast(dt.datetime, pd.to_datetime(t))


def _create_short_name(
    short_name: Optional[str], dataset_id: int, variable_id: Optional[int]
) -> str:
    """Create sensible short name for dataset."""
    validate_underscore(short_name, "short-name")
    # prepend dataset id to short name
    return f"dataset_{dataset_id}_{short_name}"


def backport(
    dataset_id: int,
    short_name: str,
    variable_id: Optional[int] = None,
    force: bool = False,
    dry_run: bool = False,
    upload: bool = True,
) -> None:
    lg = log.bind(dataset_id=dataset_id)

    # get data from database
    lg.info("backport.loading_dataset")
    ds = GrapherDatasetModel.load_dataset(engine, dataset_id)
    lg.info("backport.loading_variable", variable_id=variable_id or "all")
    if variable_id:
        vars = [GrapherVariableModel.load_variable(engine, variable_id)]
    else:
        # load all variables from a dataset
        vars = GrapherDatasetModel.load_variables_for_dataset(engine, dataset_id)
    variable_ids = [v.id for v in vars]

    # get sources for dataset and all variables
    lg.info("backport.loading_sources")
    sources = GrapherSourceModel.load_sources(
        engine, dataset_id=ds.id, variable_ids=variable_ids
    )

    short_name = _create_short_name(short_name, dataset_id, variable_id)

    config = _load_config(ds, vars, sources)

    # get checksums of config
    md5_config = checksum_str(config.json(sort_keys=True, indent=0))

    if not force:
        # first check config checksum
        if _checksum_match(f"{short_name}_config", md5_config):
            # then check dataEditedAt field
            if ds.dataEditedAt < _walden_timestamp(f"{short_name}_config"):
                lg.info(
                    "backport.skip",
                    short_name=short_name,
                    reason="checksums match",
                    checksum=md5_config,
                )
                return
            else:
                # since `dataEditedAt` is part of config, its checksum should change and _checksum_match
                # should return False... if this is not the case, something is wrong
                raise AssertionError("This should never happen")

    # don't make private datasets public
    public = not ds.isPrivate

    # upload config to walden
    lg.info("backport.upload_config", upload=upload, dry_run=dry_run, public=public)
    _upload_config_to_walden(
        config,
        _walden_config_metadata(ds, short_name, md5_config),
        dry_run,
        upload,
        public=public,
    )

    # upload values to walden
    lg.info("backport.loading_values", variables=variable_ids)
    df = _load_values(engine, variable_ids)
    lg.info(
        "backport.upload_values",
        size=len(df),
        upload=upload,
        dry_run=dry_run,
        public=public,
    )
    _upload_values_to_walden(
        df,
        _walden_values_metadata(ds, short_name),
        dry_run,
        upload,
        public=public,
    )

    lg.info("backport.finished")


@click.command()
@click.option("--dataset-id", type=int, required=True)
@click.option("--variable-id", type=int)
@click.option(
    "--short-name",
    type=str,
    help="Short name of a dataset, must be under_score",
    required=True,
)
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
    help="Upload dataset to Walden",
)
def backport_cli(
    dataset_id: int,
    short_name: str,
    variable_id: Optional[int] = None,
    force: bool = False,
    dry_run: bool = False,
    upload: bool = True,
) -> None:
    return backport(
        dataset_id=dataset_id,
        short_name=short_name,
        variable_id=variable_id,
        force=force,
        dry_run=dry_run,
        upload=upload,
    )


if __name__ == "__main__":
    # Example (run against staging DB):
    #   backport --dataset-id 5426 --variable-id 244087 --name political_regimes --dry-run --force
    #   or entire dataset
    #   backport --dataset-id 5426 --short-name political_regimes --force
    backport_cli()
