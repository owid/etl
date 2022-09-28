import datetime as dt
import os
import tempfile
from typing import List, cast

import click
import pandas as pd
import structlog
from owid.walden import Catalog as WaldenCatalog
from owid.walden.catalog import Dataset as WaldenDataset
from owid.walden.ingest import add_to_catalog
from sqlalchemy.engine import Engine
from sqlmodel import Session

from etl import config
from etl import grapher_model as gm
from etl.backport_helpers import GrapherConfig
from etl.db import get_engine
from etl.files import checksum_file, checksum_str

from . import utils

config.enable_bugsnag()

WALDEN_NAMESPACE = os.environ.get("WALDEN_NAMESPACE", "backport")

log = structlog.get_logger()

engine = get_engine()

# preload walden catalog to improve performance (initializing the catalog takes a while)
walden_catalog = WaldenCatalog()


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
    help="Upload dataset to Walden",
)
def backport_cli(
    dataset_id: int,
    force: bool = False,
    dry_run: bool = False,
    upload: bool = True,
) -> None:
    return backport(
        dataset_id=dataset_id,
        force=force,
        dry_run=dry_run,
        upload=upload,
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

            self.variable_ids = [v.id for v in vars]  # type: ignore

            # get sources for dataset and all variables
            sources = gm.Source.load_sources(session, dataset_id=ds.id, variable_ids=self.variable_ids)

        self.ds = ds
        self.config = _load_config(ds, vars, sources)
        self.md5_config = checksum_str(self.config.json(sort_keys=True, indent=0))

    @property
    def short_name(self) -> str:
        return utils.create_short_name(self.dataset_id, self.ds.name)

    @property
    def public(self) -> bool:
        return not self.ds.isPrivate

    def needs_update(self) -> bool:
        # find existing entry in catalog
        try:
            walden_ds = walden_catalog.find_one(short_name=f"{self.short_name}_config")
        except KeyError:
            # datasets not found in catalog
            return True

        # fastrack does not upload data to S3 and leaves owid_data_url empty, if we find
        # such a dataset, we backport and upload it again
        if not walden_ds.owid_data_url:
            return True

        # compare checksums
        if walden_ds.origin_md5 == self.md5_config:
            # then check dataEditedAt field
            if self.ds.dataEditedAt < _walden_timestamp(f"{self.short_name}_config"):
                return False
            else:
                # since `dataEditedAt` is part of config, its checksum should change and _checksum_match
                # should return False... if this is not the case, something is wrong
                raise AssertionError("This should never happen")
        else:
            return True

    def upload(self, upload: bool, dry_run: bool) -> None:
        _upload_config_to_walden(
            self.config,
            _walden_config_metadata(self.ds, self.short_name, self.md5_config, self.public),
            dry_run,
            upload,
            public=self.public,
        )

        # upload values to walden
        df = _load_values(engine, self.variable_ids)
        _upload_values_to_walden(
            df,
            _walden_values_metadata(self.ds, self.short_name, self.public),
            dry_run,
            upload,
            public=self.public,
        )


def backport(
    dataset_id: int,
    force: bool = False,
    dry_run: bool = False,
    upload: bool = True,
) -> None:
    lg = log.bind(dataset_id=dataset_id)

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

    dataset.upload(upload, dry_run)

    lg.info(
        "backport.upload",
        upload=upload,
        dry_run=dry_run,
        public=dataset.public,
    )

    lg.info("backport.finished")


def _load_config(
    ds: gm.Dataset,
    vars: list[gm.Variable],
    sources: list[gm.Source],
) -> GrapherConfig:
    """Get the configuration of a variable."""
    return GrapherConfig(
        dataset=ds,
        variables=vars,
        sources=sources,
    )


def _walden_values_metadata(ds: gm.Dataset, short_name: str, public: bool) -> WaldenDataset:
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
        is_public=public,
    )


def _walden_config_metadata(ds: gm.Dataset, short_name: str, origin_md5: str, public: bool) -> WaldenDataset:
    """Create walden dataset for grapher dataset variables and metadata."""
    config = _walden_values_metadata(ds, short_name, public)
    config.short_name = short_name + "_config"
    config.name = f"Grapher metadata for {short_name}"
    config.file_extension = "json"
    config.origin_md5 = origin_md5

    # use md5 as the version, thus inserting it into the Walden path; this is to avoid
    # overwriting existing datasets and breaking past versions of the ETL
    config.version = origin_md5

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

        # use the md5 checksum as the version in the backport, thus inserting it into the
        # walden path; this is to avoid overwriting existing data and breaking a running ETL
        md5 = checksum_file(f.name)
        meta.version = md5

        if not dry_run:
            add_to_catalog(meta, f.name, upload, public=public)


def _walden_timestamp(short_name: str) -> dt.datetime:
    t = walden_catalog.find_one(short_name=short_name).date_accessed
    return cast(dt.datetime, pd.to_datetime(t))


if __name__ == "__main__":
    # Example (run against staging DB):
    #   backport --dataset-id 5426 --dry-run --force
    backport_cli()
