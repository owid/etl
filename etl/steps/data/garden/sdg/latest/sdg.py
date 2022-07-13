import pandas as pd
import structlog
import copy
from pathlib import Path
from typing import List, Optional
from collections import defaultdict

from owid.catalog import Dataset, Table, Variable
from owid.catalog.utils import concat_variables
from etl.paths import DATA_DIR

log = structlog.get_logger(__name__)


SDG_SOURCES_FILE = Path(__file__).parent / "sdg_sources.csv"


def _load_sdg_sources() -> pd.DataFrame:
    sdg_sources = pd.read_csv(SDG_SOURCES_FILE)

    sdg_sources["goal"] = sdg_sources.indicator.str.split(".").str[0]

    # drop duplicate goal-variable pairs
    sdg_sources = sdg_sources.drop_duplicates(subset=["goal", "variable_id"])

    return sdg_sources


def _get_variable_from_backported_table(
    table: Table, variable_id: str
) -> Optional[Variable]:
    """Get variable from backported table."""
    for col in table.columns:
        var_id = table[col].metadata.additional_info["grapher_meta"]["id"]
        if var_id == variable_id:
            # return a copy
            v = Variable(table[col].dropna())
            v.metadata = copy.deepcopy(table[col].metadata)
            return v
    else:
        return None


def _indicator_prefix(name: str, indicator: str) -> str:
    """Create new variable name by adding indicator prefix (if data comes directly from UN
    and prefix already exists then replace it)."""
    if name.startswith("_"):
        name = name.split("__", 1)[1]

    return f"indicator_{indicator.replace('.', '_').lower()}__{name}"


def run(dest_dir: str) -> None:
    """Assemble SDG dataset."""
    sdg_sources = _load_sdg_sources()

    vars: dict[str, List[Variable]] = defaultdict(list)

    # group by datasets to make sure we load each one only once
    for dataset_name, sdg_group in sdg_sources.groupby("dataset_name"):
        ds = Dataset(DATA_DIR / "backport/owid/latest" / dataset_name)

        # Since ds[table] reads from a feather file, it becomes the bottleneck in
        # runtime. Caching saves us from repeated reads
        table_cache: dict[str, Table] = {}

        # go over all indicators from that dataset
        for r in sdg_group.itertuples():

            # iterate over all tables in a dataset (backported datasets would
            # usually have only one)
            for table_name in ds.table_names:
                if table_name in table_cache:
                    table = table_cache[table_name]
                else:
                    table = ds[table_name]
                    table_cache[table_name] = table

                log.info(
                    "sdg.run", indicator=r.indicator, variable_name=r.variable_name
                )
                v = _get_variable_from_backported_table(table, r.variable_id)
                if v is not None:
                    v.name = _indicator_prefix(v.name, r.indicator)
                    vars[r.goal].append(v)
                    # variable found, continue with another indicator
                    break
            else:
                raise Exception(f"Variable {r.variable_id} not found in tables")

    # create new dataset
    new_ds = Dataset.create_empty(dest_dir)
    new_ds.metadata.namespace = "sdg"
    new_ds.metadata.short_name = "sustainable_development_goals"

    # every goal has its own table with variables
    for goal, variables in vars.items():
        t = concat_variables(variables)
        t.metadata.short_name = f"sustainable_development_goal_{goal}"

        # sort by indicator name
        t = t.sort_index(axis=1)

        new_ds.add(t)

    new_ds.save()
