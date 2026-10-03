"""Minimal compatibility shim for archived snapshot scripts.

The legacy `apps/backport/` pipeline was removed; this module only retains
`long_to_wide` because ~370 archived `snapshots/.../*.py` scripts still
import it. The cached feather files produced by those scripts continue to
flow through the active pipeline.
"""

import pandas as pd
import structlog

log = structlog.get_logger()


def long_to_wide(df: pd.DataFrame, prune: bool = True) -> pd.DataFrame:
    """Convert backported table from long to wide format.

    :params prune: Drop columns entity_id, entity_code and rename entity_name to country.
    """
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

    df.columns.name = None

    wide_mem_usage_mb = df.memory_usage().sum() / 1e6 if not df.empty else 0
    if wide_mem_usage_mb > 1:
        log.info(
            "long_to_wide",
            wide_mb=wide_mem_usage_mb,
            long_mb=long_mem_usage_mb,
            density=f"{df.notnull().sum().sum() / (df.shape[0] * df.shape[1]):.1%}",
            compression=f"{wide_mem_usage_mb / long_mem_usage_mb:.1%}",
        )

    return df
