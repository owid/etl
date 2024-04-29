"""Search configuration."""
from typing import Any, Dict

import pandas as pd
import streamlit as st
from pydantic import BaseModel
from rapidfuzz import fuzz
from structlog import get_logger

from apps.wizard.pages.charts_v2.indicator_config import reset_indicator_form
from apps.wizard.utils import set_states

log = get_logger()

# Set to True to select good initial default dataset selections
DEBUG = True
dataset_old_debug = "Democracy and Human rights - OWID based on Varieties of Democracy (v13) and Regimes of the World"
dataset_new_debug = "Democracy and Human rights - OWID based on Varieties of Democracy (v13) and Regimes of the World"


def build_dataset_form(df: pd.DataFrame, similarity_names: Dict[str, Any]) -> "SearchConfigForm":
    """Form to input dataset 1 and dataset 2."""
    # Build dataset display name
    df["display_name"] = "[" + df["id"].astype(str) + "] " + df["name"]
    display_name_to_id_mapping = df.set_index("display_name")["id"].to_dict()

    # Header
    st.header(
        "Dataset update",
        help="Variable mapping will be done from the old to the new dataset. The idea is that the variables in the new dataset will replace those from the old dataset in our charts.",
    )

    # Sort datasets by updatedAt
    df = df.sort_values("updatedAt", ascending=False).reset_index(drop=True)

    # For debugging
    if DEBUG:
        index_old = list(df["name"]).index(dataset_old_debug)
        index_new = list(df["name"]).index(dataset_new_debug)
    # Pick the newest dataset as a default and the one matching the new dataset as the default
    else:
        index_new = 0
        index_old = int(df["name"].iloc[1:].apply(lambda n: fuzz.ratio(n, df.loc[index_new, "name"])).idxmax())

    # Dataset selectboxes
    col1, col2 = st.columns(2)
    with col1:
        ## Old dataset
        dataset_old = st.selectbox(
            label="Old dataset",
            options=df["display_name"],
            help="Dataset containing variables to be replaced in our charts.",
            index=index_old,
        )
    with col2:
        ## New dataset
        dataset_new = st.selectbox(
            label="New dataset",
            options=df["display_name"],
            help="Dataset contianinng the new variables. These will replace the old variables in our charts.",
            index=index_new,
        )

    # Parameters
    col0, _ = st.columns([1, 2])
    with col0:
        with st.popover("Parameters"):
            map_identical = st.toggle(
                "Map identically named indicators",
                value=True,
                help="Map indicators with the same name in the old and new datasets. \n\n**NOTE:** This is option is DISABLED when working with the same dataset (i.e. old dataset and new dataset are the same) and can't be changed via this checkbox.",
            )
            enable_explore = st.toggle(
                "Explore indicator mappings (Experimental)",
                help="Compare the indicator mappings with tables and charts. This might take some time initially, as we need to download data values from S3",
                value=False,
            )
            similarity_name = st.selectbox(
                label="Similarity matching function",
                options=similarity_names,
                help="Select the prefered function for matching indicators. Find more details at https://www.analyticsvidhya.com/blog/2021/07/fuzzy-string-matching-a-hands-on-guide/",
            )

    # Submit button
    submitted_datasets = st.form_submit_button(
        "Next (1/3)",
        type="primary",
        use_container_width=True,
    )

    # If user clicks on next, proceed
    if submitted_datasets:
        set_states(
            {
                "submitted_datasets": True,  # This means that the user has submitted the datasets
                "submitted_indicators": False,
                "submitted_revisions": False,
            },
            logging=True,
        )
        reset_indicator_form()

    # Get IDs of datasets
    dataset_old_id = display_name_to_id_mapping[dataset_old]
    dataset_new_id = display_name_to_id_mapping[dataset_new]

    return SearchConfigForm(
        dataset_old_id=dataset_old_id,
        dataset_new_id=dataset_new_id,
        map_identical_indicators=map_identical,
        similarity_function_name=similarity_name,
        enable_explore_mode=enable_explore,
    )


class SearchConfigForm(BaseModel):
    """Form 1."""

    dataset_old_id: str
    dataset_new_id: str
    map_identical_indicators: bool
    similarity_function_name: str
    enable_explore_mode: bool

    def __init__(self, **data: Any) -> None:
        """Constructor."""
        super().__init__(**data)
