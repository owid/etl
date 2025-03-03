import re
from collections import defaultdict
from typing import Dict, List, Set

from owid.catalog import Dataset, Table

from etl.db import read_sql
from etl.paths import DATA_DIR


def records_to_dictionary(records, key: str):
    """Transform: [{key: ..., a: ..., b: ...}, ...] -> {key: {a: ..., b: ...}, ...}."""

    dix = {}
    for record in records:
        assert key in record, f"`{key}` not found in record: {record}!"
        dix[record[key]] = {k: v for k, v in record.items() if k != key}

    return dix


def get_tables_by_name_mapping(dependencies: Set[str]) -> Dict[str, List[Table]]:
    """Dictionary mapping table short name to table object.

    Note that the format is {"table_name": [tb], ...}. This is because there could be collisions where multiple table names are mapped to the same table (e.g. two datasets could have a table with the same name).
    """
    tb_name_to_tb = defaultdict(list)

    for dep in dependencies:
        ## Ignore non-grapher dependencies
        if not re.match(r"^(data|data-private)://grapher/", dep):
            continue

        uri = re.sub(r"^(data|data-private)://", "", dep)
        ds = Dataset(DATA_DIR / uri)
        for table_name in ds.table_names:
            tb_name_to_tb[table_name].append(ds.read(table_name, load_data=False))

    return tb_name_to_tb


def validate_indicators_in_db(indicators, engine):
    """Check that indicators are in DB!"""
    q = """
    select
        id,
        catalogPath
    from variables
    where catalogPath in %(indicators)s
    """
    df = read_sql(q, engine, params={"indicators": tuple(indicators)})
    missing_indicators = set(indicators) - set(df["catalogPath"])
    if missing_indicators:
        raise ValueError(f"Missing indicators in DB: {missing_indicators}")
