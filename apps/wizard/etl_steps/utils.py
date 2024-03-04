import streamlit as st

from etl.steps import load_dag


@st.cache_data
def load_datasets(included_str) -> list[str]:
    """Load meadow datasets."""
    dag = load_dag()
    options = list(dag.keys())
    options = [o for o in options if included_str in o]
    options = sorted(options)
    return options
