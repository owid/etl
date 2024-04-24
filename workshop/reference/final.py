"""
FINAL VERSION of the workshop app.

Try to avoid copy-pasting :p
"""
import streamlit as st

from apps.wizard.utils import get_datasets_in_etl
from etl.steps import load_from_uri

# get dataset names, wait for selection
dataset_names = get_datasets_in_etl()
dataset_selected = st.selectbox(
    label="Select a dataset",
    options=dataset_names,
    index=None,
)

# action once user has selected a dataset
if dataset_selected:
    # load dataset, get table names, wait for selection
    ds = load_from_uri(dataset_selected)
    table_selected = st.selectbox(
        "Select a table",
        ds.table_names,
        index=None,
    )

    # action once user has selected a table
    if table_selected:
        # load table, get indicator names, wait for selection
        tb = ds[table_selected]
        indicator_selected = st.selectbox(
            "Select a indicator",
            tb.columns,
            index=None,
        )
        # action once user has selected an indicator
        if indicator_selected:
            tb_ = tb.reset_index()
            countries_selected = st.multiselect("Select countries", tb_["country"].unique())
            st.line_chart(
                tb_[tb_["country"].isin(countries_selected)],
                x="year",
                y=indicator_selected,
                color="country",
            )
