"""Interact with custom interim database.

NOTE: We are currently migrating some of the logic to our actual MySQL database. The goal is to have a tracking of user interaction with Wizard in our main database. For now, we will be storing new tables with names 'wizard__*' in the main database. In the future, we might want to have these tables in another database in staging.

Some of the tools here rely on a local temporary sqlite database. This database is a custom and temporary database used to store data in a server. Not intended for production use.

"""

import datetime as dt
import hashlib
import os
import time
from contextlib import contextmanager
from typing import Any, Dict, Generator, List, Literal, Optional, Tuple

import pandas as pd
import streamlit as st
import structlog
from sqlalchemy import text
from sqlalchemy.orm import Session

from apps.wizard.utils.paths import STREAMLIT_SECRETS, WIZARD_DB
from etl.config import OWID_ENV, OWIDEnv
from etl.db import get_engine, read_sql, to_sql
from etl.grapher_model import Anomaly

log = structlog.get_logger()

# DB is set up
DB_IS_SET_UP = STREAMLIT_SECRETS.exists() & WIZARD_DB.exists()
# DB config
DB_NAME = "wizard"
TB_USAGE = "expert_usage"
TB_PR = "pull_requests"
TB_NS = "etl_news"
TB_VARMAP = "wiz__variable_mapping"
# Window accepted values
WND_LITERAL = Literal["1d", "7d"]


class WizardDB:
    @classmethod
    def add_usage(
        cls,
        question: str,
        answer: str,
        cost: float,
        user: Optional[str] = None,
        feedback: Optional[int] = None,
        feedback_text: Optional[str] = None,
    ) -> None:
        """Add usage entry to database table."""
        if DB_IS_SET_UP:
            query = f"INSERT INTO {TB_USAGE} (id, date, user, question, answer, feedback, feedback_text) VALUES (:id, :date, :user, :question, :answer, :feedback, :feedback_text);"
            code = hashlib.sha1(os.urandom(4), usedforsecurity=False).hexdigest()[:2]
            query_params = {
                "id": f"{round(time.time())}{code}",
                "date": dt.datetime.now(dt.timezone.utc),
                "user": user,
                "question": question,
                "cost": cost,
                "answer": answer,
                "feedback": feedback,
                "feedback_text": feedback_text,
            }
            with create_session(DB_NAME) as s:
                s.execute(
                    text(query),
                    params=query_params,
                )
                s.commit()

    @classmethod
    def add_pr(
        cls,
        data_values,
    ) -> None:
        """Add PR data to database table."""
        if DB_IS_SET_UP:
            # Prepare query
            fields = (
                "id",
                "number",
                "date_created",
                "date_merged",
                "title",
                "username",
                "description",
                "merged",
                "labels",
                "url_merge_commit",
                "url_diff",
                "url_patch",
                "url_html",
            )
            query = _prepare_query_insert(TB_PR, fields)  # type: ignore
            # Get IDs to update
            ids = tuple(str(data["id"]) for data in data_values)
            # Insert in table
            with create_session(DB_NAME) as s:
                s.execute(text(f"DELETE FROM pull_requests WHERE ID IN {ids};"))
                s.execute(
                    text(query),
                    data_values,
                )
                s.commit()

    @classmethod
    @st.cache_data()
    def get_pr(cls, num_days: int = 7) -> pd.DataFrame:
        """Get PR data from database."""
        data = []
        if DB_IS_SET_UP:
            with create_session(DB_NAME) as s:
                query = f"SELECT * FROM {TB_PR} WHERE date_created >= :date_created;"
                data = s.execute(
                    text(query),
                    {"date_created": dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=num_days)},
                )
                data = data.fetchall()
        return pd.DataFrame(data)

    @classmethod
    def add_news_summary(cls, summary: str, cost: float, window_type: WND_LITERAL) -> None:
        """Add GPT summary entry."""
        if DB_IS_SET_UP:
            fields = ("id", "date", "cost", "summary", "window_type")
            query = _prepare_query_insert(TB_NS, fields)  # type: ignore
            code = hashlib.sha1(window_type.encode()).hexdigest()[:5]
            query_params = {
                "id": f"{round(time.time())}{code}",
                "date": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%XZ"),
                "cost": cost,
                "summary": summary,
                "window_type": window_type,
            }
            with create_session(DB_NAME) as s:
                s.execute(
                    text(query),
                    params=query_params,
                )
                s.commit()

    @classmethod
    @st.cache_data()
    def get_news_summary(cls, window_type: WND_LITERAL) -> Tuple[str, str, str] | None:
        """Get nmews (latest) from DB."""
        data = []
        if DB_IS_SET_UP:
            with create_session(DB_NAME) as s:
                # query = f"SELECT SUMMARY, DATE FROM {TB_NS} WHERE DATE(DATE) = DATE('now', 'utc') AND window_type = :window_type;"
                query = f"""SELECT SUMMARY, DATE, COST, ABS(strftime('%s', DATE) - strftime('%s', 'now')) AS time_difference
                FROM {TB_NS}
                WHERE window_type = :window_type
                ORDER BY time_difference ASC
                LIMIT 1;
                """
                data = s.execute(text(query), params={"window_type": window_type})
                data = data.fetchall()
        if data:
            if len(data) > 1:
                raise ValueError("Multiple entries for the same day.")
            return (
                data[0][0],
                data[0][1],
                data[0][2],
            )

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
        cls, mapping: Dict[int, int], dataset_id_old: int, dataset_id_new: int, comments: str = ""
    ) -> None:
        """Add a mapping to TB_VARMAP.

        This table should have columns 'id_old' (key), 'id_new' (value), 'timestamp', and 'dataset_id_old' and 'dataset_id_new'.
        """
        timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

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
    def get_variable_mapping(cls) -> Dict[int, int]:
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
        WHERE table_schema = 'owid';
        """
        df = read_sql(query)
        return tb_name in set(df["TABLE_NAME"])

    @classmethod
    def load_anomalies(cls, dataset_ids: List[int], _owid_env: OWIDEnv = OWID_ENV) -> List[Anomaly]:
        t = time.time()
        with Session(_owid_env.engine) as s:
            anomalies = Anomaly.load_anomalies(s, dataset_ids)
        log.info("load_anomalies", t=time.time() - t)
        return anomalies


@contextmanager
def create_session(db_name: str) -> Generator[Session, None, None]:
    conn = st.connection(db_name)
    with conn.session as s:
        yield s


def _prepare_query_insert(tb_name: str, fields: Tuple[Any]) -> str:
    # Prepare query
    values = ":" + ", :".join(fields)
    query = f"INSERT INTO {tb_name} {fields} VALUES ({values});"
    return query


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
