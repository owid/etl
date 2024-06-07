"""Search configuration."""
from typing import Any, Dict

import pandas as pd
import streamlit as st
from pydantic import BaseModel
from structlog import get_logger

from apps.wizard.app_pages.indicator_upgrade.indicator_mapping import reset_indicator_form
from apps.wizard.utils import set_states

log = get_logger()

# Set to True to select good initial default dataset selections
DEBUG = False
dataset_old_debug = "Democracy and Human rights - OWID based on Varieties of Democracy (v13) and Regimes of the World"
dataset_new_debug = "Democracy and Human rights - OWID based on Varieties of Democracy (v13) and Regimes of the World"


def sort_datasets_old(df: pd.DataFrame) -> pd.DataFrame:
    """Sort selectbox with old datasets based on selected new dataset."""
    if st.session_state.is_any_migration and not st.session_state.show_all_datasets:
        with st.spinner("Updating the old dataset list..."):
            if "new_dataset_selectbox" not in st.session_state:
                num_id = df.loc[df["migration_new"], "id"].iloc[0]
            else:
                num_id = st.session_state.new_dataset_selectbox.split("]")[0].replace("[", "")
            column_sorting = f"score_{num_id}"

            # Account for the case when user chooses "show all datasets" and then unselects the toggle!
            if column_sorting not in df.columns:
                num_id = df.loc[df["migration_new"], "id"].iloc[0]
                column_sorting = f"score_{num_id}"

            df = df.sort_values(column_sorting, ascending=False)
            return df
    return df


def build_dataset_form(df: pd.DataFrame, similarity_names: Dict[str, Any]) -> "SearchConfigForm":
    """Form to input dataset 1 and dataset 2."""
    if not st.session_state.is_any_migration:
        st.warning(
            "No new grapher dataset was detected! Remember that for it to appear it must be in the Database (`etl run step --grapher`). Showing all datasets instead."
        )

    if st.session_state.show_step_name:
        column_display = "step"
    else:
        column_display = "name"

    # st.dataframe(df)
    # Build dataset display name

    df["display_step"] = "[" + df["id"].astype(str) + "] " + df["step"]
    df["display_name"] = "[" + df["id"].astype(str) + "] " + df["name"]
    display_name_to_id_mapping = df.set_index("display_name")["id"].to_dict()
    display_step_to_id_mapping = df.set_index("display_step")["id"].to_dict()

    def display_to_id_mapping(display):
        try:
            return display_name_to_id_mapping[display]
        except KeyError:
            return display_step_to_id_mapping[display]

    # Header
    st.header(
        "Dataset update",
        help="Variable mapping will be done from the old to the new dataset. The idea is that the variables in the new dataset will replace those from the old dataset in our charts.",
    )

    # Sort datasets by updatedAt
    # df = df.sort_values("updatedAt", ascending=False).reset_index(drop=True)
    df = df.reset_index(drop=True)

    # View options
    with st.popover("View options"):
        st.markdown("Change the default dataset view.")
        st.toggle(
            "Show all datasets",
            help="Show all datasets, including those not detected by the grapher.",
            on_change=set_states_if_form_is_modified,
            key="show_all_datasets",
        )
        st.toggle(
            "Show step names",
            help="Show the step names in the dataset list.",
            on_change=set_states_if_form_is_modified,
            key="show_step_name",
        )

    ## New dataset
    if st.session_state.is_any_migration and not st.session_state.show_all_datasets:
        options = df[df["migration_new"]].reset_index(drop=True)
    else:
        options = df.reset_index(drop=True)

    if "new_dataset_selectbox" in st.session_state:
        dataset_new_id = display_to_id_mapping(st.session_state["new_dataset_selectbox"])
        index = options[options["id"] == dataset_new_id].index.item()
    else:
        index = 0

    def _dataset_new_selectbox_on_change():
        # If the new dataset changes, ensure the old dataset changes accordingly.
        set_states(
            {"old_dataset_selectbox": sort_datasets_old(df).reset_index(drop=True)[f"display_{column_display}"].iloc[0]}
        )
        set_states_if_form_is_modified()

    dataset_new = st.selectbox(
        label="**New dataset**",
        options=options[f"display_{column_display}"],
        help="Dataset containing the new variables. These will replace the old variables in our charts.",
        index=index,
        key="new_dataset_selectbox",
        on_change=_dataset_new_selectbox_on_change,
    )

    options = sort_datasets_old(df).reset_index(drop=True)
    if "old_dataset_selectbox" in st.session_state:
        index = options[
            (options["display_name"] == st.session_state["old_dataset_selectbox"])
            | (options["display_step"] == st.session_state["old_dataset_selectbox"])
        ].index.item()
    else:
        index = 0
    # index = 0
    ## Old dataset
    dataset_old = st.selectbox(
        label="**Old dataset**: Select the dataset that you are updating",
        options=options[f"display_{column_display}"],
        help="Dataset containing variables to be replaced in our charts.",
        index=index,
        key="old_dataset_selectbox",
        on_change=set_states_if_form_is_modified,
    )

    # Parameters
    col0, _ = st.columns([1, 2])
    with col0:
        with st.popover("Parameters"):
            map_identical = st.toggle(
                "Map identically named indicators",
                value=True,
                help="Map indicators with the same name in the old and new datasets. \n\n**NOTE:** This is option is DISABLED when working with the same dataset (i.e. old dataset and new dataset are the same) and can't be changed via this checkbox.",
                on_change=set_states_if_form_is_modified,
            )
            enable_bulk_explore = st.toggle(
                "Bulk explore mode",
                help="Compare the indicator mappings with tables and charts. This might take some time initially, as we need to download _all_ data values from S3. Alternatively, you can explore the mappings later on (which will download only the data necessary for a specific comparison).",
                value=False,
                on_change=set_states_if_form_is_modified,
            )
            similarity_name = st.selectbox(
                label="Similarity matching function",
                options=similarity_names,
                help="Select the preferred function for matching indicators. Find more details at https://www.analyticsvidhya.com/blog/2021/07/fuzzy-string-matching-a-hands-on-guide/",
                on_change=set_states_if_form_is_modified,
            )

    # Submit button
    submitted_datasets = st.button(
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
                "submitted_charts": False,
            },
            logging=True,
        )
        reset_indicator_form()

    # Get IDs of datasets
    dataset_old_id = display_to_id_mapping(dataset_old)
    dataset_new_id = display_to_id_mapping(dataset_new)
    set_states({"migration_new_id": dataset_new_id})

    return SearchConfigForm(
        dataset_old_id=str(dataset_old_id),
        dataset_new_id=str(dataset_new_id),
        map_identical_indicators=map_identical,
        similarity_function_name=similarity_name,
        enable_bulk_explore=enable_bulk_explore,
    )


def set_states_if_form_is_modified():
    set_states(
        {
            "submitted_datasets": False,  # This means that the user has submitted the datasets
            "submitted_indicators": False,
            "submitted_charts": False,
        },
        logging=True,
    )


class SearchConfigForm(BaseModel):
    """Form 1."""

    dataset_old_id: str
    dataset_new_id: str
    map_identical_indicators: bool
    similarity_function_name: str
    enable_bulk_explore: bool

    def __init__(self, **data: Any) -> None:
        """Constructor."""
        super().__init__(**data)
