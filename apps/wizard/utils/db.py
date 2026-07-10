"""Interact with Wizard-specific tables in our MySQL database.

NOTE: Tables tracking user interaction with Wizard live in our main database, with names 'wiz__*'. In the future, we might want to have these tables in another database in staging.

"""

import datetime as dt
import time

import pandas as pd
import structlog
from sqlalchemy import text
from sqlalchemy.orm import Session

from etl.config import OWID_ENV, OWIDEnv
from etl.db import get_engine, read_sql, to_sql
from etl.grapher.model import Anomaly

log = structlog.get_logger()

# DB config
TB_VARMAP = "wiz__variable_mapping"


class WizardDB:
    @classmethod
    def delete_variable_mapping(cls) -> None:
        """Delete variable mapping."""
        if cls.table_exists(TB_VARMAP):
            query = f"DELETE FROM {TB_VARMAP};"
            engine = get_engine()
            with Session(engine) as s:
                s.execute(text(query))
                s.commit()

    @classmethod
    def add_variable_mapping(
        cls, mapping: dict[int, int], dataset_id_old: int, dataset_id_new: int, comments: str = ""
    ) -> None:
        """Add a mapping to TB_VARMAP.

        This table should have columns 'id_old' (key), 'id_new' (value), 'timestamp', and 'dataset_id_old' and 'dataset_id_new'.

        If a mapping for an id_old already exists, it will be replaced with the new mapping.
        """
        timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        # Remove any existing mappings for these id_old values before inserting
        if mapping and cls.table_exists(TB_VARMAP):
            id_old_list = list(mapping.keys())
            placeholders = ", ".join([":id_" + str(i) for i in range(len(id_old_list))])
            params = {f"id_{i}": id_old for i, id_old in enumerate(id_old_list)}
            query = f"DELETE FROM {TB_VARMAP} WHERE id_old IN ({placeholders})"
            engine = get_engine()
            with Session(engine) as s:
                s.execute(text(query), params)
                s.commit()

        # Build dataframe
        query_params = [
            {
                "id_old": id_old,
                "id_new": id_new,
                "timestamp": timestamp,
                "dataset_id_old": dataset_id_old,
                "dataset_id_new": dataset_id_new,
                "comments": comments,
            }
            for id_old, id_new in mapping.items()
        ]
        df = pd.DataFrame(query_params)

        # Insert in table
        to_sql(df, TB_VARMAP, if_exists="append", index=False)

    @classmethod
    def get_variable_mapping_raw(cls) -> pd.DataFrame:
        """Get the mapping from TB_VARMAP."""
        if cls.table_exists(TB_VARMAP):
            return read_sql(f"SELECT * FROM {TB_VARMAP};")
        return pd.DataFrame()

    @classmethod
    def get_variable_mapping(cls) -> dict[int, int]:
        """Get variable mapping.

        This mapping can be the result of multiple mappings.

        Example: you upgrade indicators twice, the mapping will be the result of the two mappings.

        First mapping is: 1 -> 4 and 2 -> 5
        Second mapping is: 4 -> 2

        Then, the resulting mapping is 1 -> 2, 2 -> 5, and 4 -> 2.

        """
        df = cls.get_variable_mapping_raw()

        if df.empty:
            return {}

        mapping = simplify_varmap(df)

        return mapping

    @classmethod
    def table_exists(cls, tb_name: str):
        """Check if table exists in the database."""
        query = """
        SELECT *
        FROM information_schema.tables
        WHERE table_schema = DATABASE();
        """
        df = read_sql(query)
        return tb_name in set(df["TABLE_NAME"])

    @classmethod
    def load_anomalies(cls, dataset_ids: list[int], _owid_env: OWIDEnv = OWID_ENV) -> list[Anomaly]:
        t = time.time()
        with Session(_owid_env.engine) as s:
            anomalies = Anomaly.load_anomalies(s, dataset_ids)
        log.info("load_anomalies", t=time.time() - t)
        return anomalies


def simplify_varmap(df):
    groups = df.groupby("timestamp")

    mapping = {}
    # Iterate over each 'submitted mapping'
    for group in groups:
        # Get mapping for a certain timestamp
        mapping_ = group[1][["id_old", "id_new"]].set_index("id_old")["id_new"].to_dict()

        # Initialize the mapping
        if mapping == {}:
            mapping = mapping_
            continue

        # Sanity check that: there is no key in mapping_ already present in mapping
        if any(k in mapping for k in mapping_):
            raise ValueError(
                "The variable mapping has an unexpected format. An indicator is being upgraded multiple times."
            )

        # Update the mapping sequentially
        for k, v in mapping.items():
            if v in mapping_:
                mapping[k] = mapping_[v]

        # Update with new mappings
        mapping = mapping | mapping_

    # Remove self-mappings
    mapping_no_identical = {k: v for k, v in mapping.items() if k != v}

    return mapping_no_identical
