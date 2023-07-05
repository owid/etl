from typing import Optional, cast

import numpy as np
import pandas as pd
import structlog
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table
from pydantic import BaseModel

from etl import grapher_model as gm
from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_grapher_dataset, convert_grapher_variable

SPARSE_DATASET_VARIABLES_CHUNKSIZE = 1000

log = structlog.get_logger()


class GrapherConfig(BaseModel):
    dataset: gm.Dataset
    variables: list[gm.Variable]
    # NOTE: sources can belong to dataset or variable
    sources: list[gm.Source]

    def to_json(self) -> str:
        return self.json(sort_keys=True, indent=0)

    @classmethod
    def from_grapher_objects(
        cls,
        ds: gm.Dataset,
        vars: list[gm.Variable],
        sources: list[gm.Source],
    ) -> "GrapherConfig":
        return cls(
            dataset=ds,
            variables=vars,
            sources=sources,
        )


def load_values(short_name: str) -> pd.DataFrame:
    snap = Snapshot(f"backport/latest/{short_name}_values.feather")
    return cast(pd.DataFrame, pd.read_feather(snap.path))


def load_config(short_name: str) -> GrapherConfig:
    snap = Snapshot(f"backport/latest/{short_name}_config.json")
    return GrapherConfig.parse_file(snap.path)


def long_to_wide(df: pd.DataFrame, prune: bool = True) -> pd.DataFrame:
    """Convert backported table from long to wide format.

    :params prune: Drop columnd entity_id, entity_code and rename entity_name to country.
    """
    # convert to wide format
    long_mem_usage_mb = df.memory_usage().sum() / 1e6

    if prune:
        df = df.rename(columns={"entity_name": "country"}).pivot(
            index=["year", "country"],
            columns="variable_name",
            values="value",
        )
    else:
        df = df.pivot(
            index=["year", "entity_name", "entity_id", "entity_code"],
            columns="variable_name",
            values="value",
        )

    # report compression ratio if the file is larger than >1MB
    # NOTE: memory usage can further drop later after repack_frame is called
    wide_mem_usage_mb = df.memory_usage().sum() / 1e6 if not df.empty else 0
    if wide_mem_usage_mb > 1:
        log.info(
            "create_wide_table",
            wide_mb=wide_mem_usage_mb,
            long_mb=long_mem_usage_mb,
            density=f"{df.notnull().sum().sum() / (df.shape[0] * df.shape[1]):.1%}",
            compression=f"{wide_mem_usage_mb / long_mem_usage_mb:.1%}",
        )

    return df


def create_wide_table(values: pd.DataFrame, short_name: str, config: GrapherConfig) -> Table:
    """Convert backported table from long to wide format."""
    t = Table(long_to_wide(values, prune=False), short_name=short_name)

    # add variables metadata
    # NOTE: some datasets such as `dataset_5438_global_health_observatory__world_health_organization__2021_12`
    #   would benefit from compression metadata as it is almost as large as the data itself (uncompressed)
    variable_dict = {v.name: v for v in config.variables}
    variable_source_dict = {s.id: s for s in config.sources}

    for col in t.columns:
        variable = variable_dict[col]
        t[col].metadata = convert_grapher_variable(variable, variable_source_dict[variable.sourceId])

    # NOTE: collision happens for dataset 5629 with column names
    # Indicator:On-premise sales restrictions to intoxicated persons (archived) - Beverage Types:Spirits
    # Indicator:On-premise sales restrictions to intoxicated persons - Archived - Beverage Types:Spirits
    return underscore_table(t, collision="rename")


def create_dataset(dest_dir: str, short_name: str, new_short_name: Optional[str] = None) -> Dataset:
    """Create Dataset from backported dataset in Snapshot. Convert
    it into wide format and add metadata."""
    new_short_name = new_short_name or short_name

    values = load_values(short_name)
    config = load_config(short_name)

    # put sources belonging to a dataset but not to a variable into dataset metadata
    variable_source_ids = {v.sourceId for v in config.variables}
    ds_sources = [s for s in config.sources if s.datasetId == config.dataset.id and s.id not in variable_source_ids]

    # create dataset with metadata
    ds = Dataset.create_empty(dest_dir, convert_grapher_dataset(config.dataset, ds_sources, short_name))

    tables = []

    # if table is too sparse, split it into multiple tables to make them fit in memory
    # this happens very rarely, e.g. for datasets
    #   dataset_5520_united_nations_sustainable_development_goals__united_nations__2022_02
    #   dataset_5438_global_health_observatory__world_health_organization__2021_12
    # ideally we would have custom scripts for those datasets doing grouping in a logical way
    # rather than by variable ids
    n_variables = len(set(values.variable_id))
    wide_size = n_variables * len(set(values.entity_id))
    long_size = len(values)

    if wide_size >= 1e6 and wide_size / long_size > 0.5:
        log.warning(
            "create_dataset.sparse_dataset",
            short_name=short_name,
        )

        # group it by chunks
        for variable_ids in np.array_split(
            values.variable_id.unique().sort_values(),
            int(n_variables / SPARSE_DATASET_VARIABLES_CHUNKSIZE),
        ):
            variable_ids = variable_ids.astype(int)
            chunk = values.loc[values.variable_id.isin(variable_ids)]
            t = create_wide_table(
                chunk,
                f"variable_ids_{variable_ids.min()}_to_{variable_ids.max()}",
                config,
            )
            tables.append(t)
    else:
        t = create_wide_table(values, new_short_name, config)
        tables.append(t)

    # create tables
    for t in tables:
        if "year" in t.columns:
            t = t.rename(columns={"year": "year_"})

        ds.add(t)

    return ds
