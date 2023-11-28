import copy
import re
from collections import defaultdict
from pathlib import Path
from typing import List, Optional

import pandas as pd
import structlog
from owid.catalog import Dataset, Table, Variable

from etl.paths import DATA_DIR

log = structlog.get_logger(__name__)


SDG_SOURCES_FILE = Path(__file__).parent / "sdg_sources.csv"


def concat_variables(variables: List[Variable]) -> Table:
    """Concatenate variables into a single table keeping all metadata."""
    t = Table(pd.concat(variables, axis=1))
    for v in variables:
        if v.name:
            t._fields[v.name] = v.metadata
    return t


def _load_sdg_sources() -> pd.DataFrame:
    sdg_sources = pd.read_csv(SDG_SOURCES_FILE)

    sdg_sources["goal"] = sdg_sources.indicator.str.split(".").str[0]

    # drop duplicate goal-variable pairs
    sdg_sources = sdg_sources.drop_duplicates(subset=["goal", "variable_id"])

    return sdg_sources


def _get_variable_from_backported_table(table: Table, variable_id: str) -> Optional[Variable]:
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


def _get_variable_from_table(table: Table, variable_title: str) -> Optional[Variable]:
    """Get variable from table based on variable's title."""
    for col in table.columns:
        var_title = table[col].metadata.title
        if var_title == variable_title:
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


def _align_multiindex(index: pd.Index) -> pd.Index:
    """Align multiindex (year, entity_name, entity_id, entity_code) to (country, year)."""
    index = index.rename([n.replace("entity_name", "country") for n in index.names])
    return pd.MultiIndex.from_arrays(
        [
            index.get_level_values("country").astype("category"),
            index.get_level_values("year").astype(int),
        ],
        names=("country", "year"),
    )


def run(dest_dir: str) -> None:
    """Assemble SDG dataset."""
    sdg_sources = _load_sdg_sources()

    vars: dict[str, List[Variable]] = defaultdict(list)

    # group by datasets to make sure we load each one only once
    for dataset_name, sdg_group in sdg_sources.groupby("dataset_name"):
        # kludge: if dataset is World Bank WDI, then grab metadata from the
        # corresponding garden dataset
        regex = re.search(r"world_development_indicators__world_bank__(\d{4}_\d{2}_\d{2})$", dataset_name)
        if regex:
            from_backport = False
            version = regex.groups()[0].replace("_", "-")
            ds = Dataset(DATA_DIR / f"garden/worldbank_wdi/{version}/wdi")
        else:
            from_backport = True
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
                    table.index = _align_multiindex(table.index)

                    table_cache[table_name] = table

                log.info("sdg.run", indicator=r.indicator, variable_name=r.variable_name)
                if from_backport:
                    v = _get_variable_from_backported_table(table, r.variable_id)
                else:
                    v = _get_variable_from_table(table, r.variable_name)
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
