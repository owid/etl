"""Interact with database."""
import datetime as dt
import hashlib
import os
import time
from typing import Optional

import pandas as pd
import streamlit as st

from apps.wizard.utils.paths import STREAMLIT_SECRETS, WIZARD_DB

# DB is set up
DB_IS_SET_UP = STREAMLIT_SECRETS.exists() & WIZARD_DB.exists()
# DB config
DB_NAME = "wizard"
TB_USAGE = "expert_usage"
TB_PR = "pull_requests"


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
            conn = st.connection(DB_NAME)
            with conn.session as s:
                s.execute(
                    query,
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
            values = ":" + ", :".join(fields)
            query = f"INSERT INTO pull_requests {fields} VALUES ({values});"
            # Get IDs to update
            ids = tuple(str(data["id"]) for data in data_values)
            # Insert in table
            conn = st.connection(DB_NAME)
            with conn.session as s:
                s.execute(f"DELETE FROM pull_requests WHERE ID IN {ids};")
                s.execute(
                    query,
                    data_values,
                )
                s.commit()

    @classmethod
    def get_pr(cls, num_days: int = 7):
        """Get PR data from database."""
        data = []
        if DB_IS_SET_UP:
            conn = st.connection(DB_NAME)
            with conn.session as s:
                query = f"SELECT * FROM {TB_PR} WHERE date_created >= :date_created;"
                data = s.execute(
                    query,
                    {"date_created": dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=num_days)},
                )
                data = data.fetchall()
        return pd.DataFrame(data)
