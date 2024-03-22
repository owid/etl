"""Interact with database."""
import datetime as dt
import hashlib
import os
import time
from typing import Optional

import streamlit as st

from apps.wizard.utils.paths import STREAMLIT_SECRETS, WIZARD_DB

# DB is set up
DB_IS_SET_UP = STREAMLIT_SECRETS.exists() & WIZARD_DB.exists()
# DB config
DB_NAME = "wizard"
TB_USAGE = "expert_usage"
TB_PR = "pull_requests"


class WizardDB:
    def add_usage(
        self,
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

    def add_pr(
        self,
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
            query = f"INSERT INTO {TB_PR} {fields} VALUES {tuple(f':{f}' for f in fields)};"

            print(query)
            # Insert in table
            conn = st.connection(DB_NAME)
            with conn.session as s:
                s.executemany(
                    query,
                    data_values,
                )
                s.commit()
