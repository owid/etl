"""Search configuration."""
from typing import Any, Dict

import pandas as pd
import streamlit as st
from pydantic import BaseModel
from structlog import get_logger

log = get_logger()


def build_dataset_form(df: pd.DataFrame, similarity_names: Dict[str, Any]) -> "SearchConfigForm":
    """Form to input dataset 1 and dataset 2."""
    # build dataset display name
    df["display_name"] = "[" + df["id"].astype(str) + "] " + df["name"]
    display_name_to_id_mapping = df.set_index("display_name")["id"].to_dict()
    # Header
    st.header(
        "Dataset update",
        help="Variable mapping will be done from the old to the new dataset. The idea is that the variables in the new dataset will replace those from the old dataset in our charts.",
    )
    col1, col2 = st.columns(2)
    with col1:
        dataset_old = st.selectbox(
            label="Old dataset",
            options=df["display_name"],
            help="Dataset containing variables to be replaced in our charts.",
        )
    with col2:
        dataset_new = st.selectbox(
            label="New dataset",
            options=df["display_name"],
            help="Dataset contianinng the new variables. These will replace the old variables in our charts.",
        )
    col0, _, _ = st.columns(3)
    with col0:
        with st.expander("Parameters"):
            map_identical = st.checkbox(
                "Map identically named variables",
                value=True,
                help="Map variables with the same name in the old and new datasets. \n\n**NOTE:** This is option is DISABLED when working with the same dataset (i.e. old dataset and new dataset are the same) and can't be changed via this checkbox.",
            )
            enable_explore = st.checkbox(
                "Explore variable mappings (Experimental)",
                help="Compare the variable mappings with tables and charts. This might take some time initially, as we need to download data values from S3",
                value=False,
            )
            similarity_name = st.selectbox(
                label="Similarity matching function",
                options=similarity_names,
                help="Select the prefered function for matching variables. Find more details at https://www.analyticsvidhya.com/blog/2021/07/fuzzy-string-matching-a-hands-on-guide/",
            )
    submitted_datasets = st.form_submit_button("Next", type="primary")
    if submitted_datasets:
        st.session_state.submitted_datasets = True
        st.session_state.show_submission_details = False
        st.session_state.submitted_variables = False
        st.session_state.submitted_revisions = False
        log.info(
            f"{st.session_state.submitted_datasets}, {st.session_state.submitted_variables}, {st.session_state.submitted_revisions}"
        )

    # Get IDs of datasets
    dataset_old_id = display_name_to_id_mapping[dataset_old]
    dataset_new_id = display_name_to_id_mapping[dataset_new]

    return SearchConfigForm(
        dataset_old_id=dataset_old_id,
        dataset_new_id=dataset_new_id,
        map_identical_variables=map_identical,
        similarity_function_name=similarity_name,
        enable_explore_mode=enable_explore,
    )


class SearchConfigForm(BaseModel):
    """Form 1."""

    dataset_old_id: str
    dataset_new_id: str
    map_identical_variables: bool
    similarity_function_name: str
    enable_explore_mode: bool

    def __init__(self, **data: Any) -> None:
        """Constructor."""
        super().__init__(**data)
