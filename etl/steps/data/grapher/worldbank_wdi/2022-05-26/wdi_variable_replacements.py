"""Generates a mapping of old->new grapher variable Ids.

Generates a mapping of variable replacements that links a grapher variable from
an old version of the World Development Indicators to a grapher variable for
the newest version of the World Development Indicators.

For use after the grapher step has been executed.

Usage:

    $ python -m etl.steps.grapher.worldbank_wdi.{version}.wdi_variable_replacements

    // output:
    // wdi.variable_replacements.json
    {
        // old_id: new_id
        "157342": 445693,
        "2037": 445693,
        ...
    }
"""

import json
from importlib import import_module
from pathlib import Path
from typing import cast

import pandas as pd
import structlog
from owid.catalog import Dataset

from etl.db_utils import get_connection
from etl.paths import DATA_DIR

VariableMatcher = getattr(
    import_module(
        f'etl.steps.data.garden.{Path(__file__).parent.as_posix().split("/grapher/")[1].replace("/", ".")}.wdi'
    ),
    "VariableMatcher",
)

log = structlog.get_logger()

OUTPATH = Path(__file__).parent / "wdi.variable_replacements.json"


def main() -> None:
    fname = Path(__file__).stem.split("_")[0]

    dataset_id = fetch_new_dataset_id()

    vm = VariableMatcher()
    df_vars = fetch_new_variables()

    variable_replacements = {}
    for _, var in df_vars.iterrows():
        # var = df_vars.loc[var_code].to_dict()
        grapher_vars = vm.find_grapher_variables(var["name"])
        if grapher_vars:
            new_id = var["id"]
            for v in grapher_vars:
                if v["datasetId"] != dataset_id:
                    old_id = v["id"]
                    assert old_id not in variable_replacements
                    variable_replacements[old_id] = new_id
        else:
            log.warning(
                f"Variable does not match an existing {fname} variable name in the grapher",
                variable_name=var["name"],
            )

    with open(OUTPATH, "w") as f:
        json.dump(variable_replacements, f, indent=2)


def fetch_new_dataset_id() -> int:
    version = Path(__file__).parent.stem
    fname = Path(__file__).stem.split("_")[0]
    namespace = Path(__file__).parent.parent.stem
    dataset = Dataset(DATA_DIR / f"garden/{namespace}/{version}/{fname}")
    q = f"SELECT id FROM datasets WHERE name = '{dataset.metadata.short_name}' AND version = '{version}'"
    datasets = pd.read_sql(q, get_connection())
    assert datasets.shape[0] == 1
    dataset_id = int(datasets.squeeze())
    return dataset_id


def fetch_new_variables() -> pd.DataFrame:
    dataset_id = fetch_new_dataset_id()
    query = f"""
        SELECT id, name
        FROM variables
        WHERE datasetId = {dataset_id}
    """
    df = pd.read_sql(query, get_connection())
    return cast(pd.DataFrame, df)


if __name__ == "__main__":
    main()
