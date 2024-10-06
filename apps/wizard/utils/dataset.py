from typing import List

import streamlit as st
from sqlalchemy.orm import Session

from etl.config import OWID_ENV
from etl.grapher_model import Dataset


@st.cache_data
def load_datasets_uri_from_db() -> List[str]:
    with Session(OWID_ENV.engine) as session:
        datasets = Dataset.load_datasets_uri(session)

    return list(datasets["dataset_uri"])
