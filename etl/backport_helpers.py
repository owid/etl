from typing import cast

import structlog
import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table
from owid.walden import Catalog as WaldenCatalog

from etl.grapher_model import GrapherConfig
from etl.steps.data.converters import convert_grapher_dataset, convert_grapher_variable


from owid.catalog import VariableMeta


log = structlog.get_logger()


def load_values(short_name: str) -> pd.DataFrame:
    walden_ds = WaldenCatalog().find_one(short_name=f"{short_name}_values")
    local_path = walden_ds.ensure_downloaded()
    return cast(pd.DataFrame, pd.read_feather(local_path))


def load_config(short_name: str) -> GrapherConfig:
    walden_ds = WaldenCatalog().find_one(short_name=f"{short_name}_config")
    local_path = walden_ds.ensure_downloaded()
    return GrapherConfig.parse_file(local_path)


def create_wide_table(
    values: pd.DataFrame, short_name: str, config: GrapherConfig
) -> Table:
    """Convert backported table from long to wide format."""
    # convert to wide format
    long_mem_usage_mb = values.memory_usage().sum() / 1e6
    df = values.pivot(
        index=["entity_name", "year"], columns="variable_name", values="value"
    )

    # report compression ratio if the file is larger than >1MB
    # NOTE: memory usage can further drop later after repack_frame is called
    wide_mem_usage_mb = df.memory_usage().sum() / 1e6
    if wide_mem_usage_mb > 1:
        log.info(
            "create_wide_table",
            short_name=short_name,
            wide_mb=wide_mem_usage_mb,
            long_mb=long_mem_usage_mb,
            compression=f"{wide_mem_usage_mb / long_mem_usage_mb:.1%}",
        )

    # TODO: what about Table metadata? should I reuse what I have in a dataset?
    t = Table(df)
    t.metadata.short_name = short_name

    # add variables metadata
    for col in t.columns:
        t[col].metadata = get_metadata_for_variable_name(config, col)

    return underscore_table(t)


def create_dataset(dest_dir: str, short_name: str) -> Dataset:
    """Create Dataset from backported dataset in walden. Convert
    it into wide format and add metadata."""
    values = load_values(short_name)
    config = load_config(short_name)

    # put sources belonging to a dataset but not to a variable into dataset metadata
    variable_source_ids = {v.sourceId for v in config.variables}
    ds_sources = [
        s
        for s in config.sources
        if s.datasetId == config.dataset.id and s.id not in variable_source_ids
    ]

    # create dataset with metadata
    ds = Dataset.create_empty(
        dest_dir, convert_grapher_dataset(config.dataset, ds_sources, short_name)
    )

    # in rare cases when wide table would be too sparse, we keep the dataset in long format
    n_variables = len(set(values.variable_id))
    wide_size = n_variables * len(set(values.entity_id))
    long_size = len(values)

    tables = []

    if wide_size >= 1e6 and wide_size / long_size > 0.5:
        # log.info(
        #     "create_dataset.long_format", short_name=short_name, wide_size=wide_size
        # )
        # # keep config in additional_info for later use
        # ds.metadata.additional_info["grapher_config"] = config.dict()

        # t = Table(values)
        # t.metadata.short_name = short_name
        # tables.append(t)

        # group it by 1000 variables
        import numpy as np

        for variable_ids in np.array_split(
            values.variable_id.unique().sort_values(), n_variables / 1000
        ):
            chunk = values.loc[values.variable_id.isin(variable_ids)]
            t = create_wide_table(
                chunk,
                f"variable_ids_{variable_ids.min()}_to_{variable_ids.max()}",
                config,
            )
            tables.append(t)
    else:
        t = create_wide_table(values, short_name, config)
        tables.append(t)

    # create tables
    for t in tables:
        ds.add(t)

    return ds


def get_metadata_for_variable_name(
    config: GrapherConfig, variable_name: str
) -> VariableMeta:
    variable = [v for v in config.variables if v.name == variable_name][0]
    variable_source = [s for s in config.sources if s.id == variable.sourceId][0]
    return convert_grapher_variable(
        variable,
        variable_source,
    )
