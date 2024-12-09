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

    # Create a column to display the dataset by its dataset id followed by its title.
    df["display_name"] = "[" + df["id"].astype(str) + "] " + df["name"]
    version = df["step"].str.split("/").str[-2]
    is_archived = df["isArchived"].replace({0: "", 1: " (ARCHIVED) "}).fillna("")
    df["display_name"] = is_archived + df["display_name"] + " [" + version.fillna("unknown version") + "]"
    # Create a dictionary mapping from that display to dataset id.
    display_name_to_id_mapping = df.set_index("display_name")["id"].to_dict()
    # Create a column to display the dataset by its dataset id followed by its ETL step.
    df["display_step"] = is_archived + "[" + df["id"].astype(str) + "] " + df["step"]
    # Create a dictionary mapping from that display to dataset id.
    display_step_to_id_mapping = df.set_index("display_step")["id"].to_dict()

    # Header
    st.header(
        "Dataset update",
        help="Variable mapping will be done from the old to the new dataset. The idea is that the variables in the new dataset will replace those from the old dataset in our charts.",
    )

    # View options
    with st.popover("View options"):
        st.markdown("Change the default dataset view.")
        # st.toggle(
        #     "Show archived datasets",
        #     help="By default, archived datasets are not shown. Change this by checking this box.",
        #     on_change=set_states_if_form_is_modified,
        #     key="show_archived_datasets",
        # )
        st.toggle(
            "Show all datasets",
            help="Show all datasets. By default, Indicator Upgrader will try to present only those datasets that are new. You can disable this by ckecking this box. You can also check this box to show archived datasets.",
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
        # If a new dataset mapping has been detected (and if the "Show all datasets" option is not activated)
        # the dropdown of new datasets should only show the detected new datasets.
        options = df[df["migration_new"]].reset_index(drop=True)
    else:
        if not st.session_state.show_all_datasets:
            df = df.loc[df["isArchived"] == 0, :]
        # Otherwise, show all datasets in grapher.
        options = df.reset_index(drop=True)

    if "new_dataset_selectbox" in st.session_state:
        # Find the dataset id of the new dataset chosen from the dropdown.
        dataset_new_id = display_name_to_id_mapping.get(
            st.session_state["new_dataset_selectbox"]
        ) or display_step_to_id_mapping.get(st.session_state["new_dataset_selectbox"])
        # Find the position of that dataset among the options to show in the new datasets dropdown.
        # The dropdown will now start on this position.
        if dataset_new_id in set(options["id"]):
            index = options[options["id"] == dataset_new_id].index.item()
        else:
            index = 0
    else:
        # If no new dataset has been chosen yet, the new datasets dropdown should start from the beginning.
        index = 0

    def _dataset_new_selectbox_on_change():
        # This function will be executed when a new dataset is chosen from the dropdown.
        # If "Show all datasets" is activated, nothing should happen.
        # Otherwise, we want that the choice of new dataset alters the old dataset.
        # (But note that, if the old dataset changes, we don't want the new dataset to change).
        # So, if the new dataset changes, ensure the old dataset changes accordingly.
        if not st.session_state.show_all_datasets:
            set_states(
                {
                    "old_dataset_selectbox": sort_datasets_old(df)
                    .reset_index(drop=True)[f"display_{column_display}"]
                    .iloc[0]
                }
            )
        set_states_if_form_is_modified()

    # New dataset dropdown.
    dataset_new = st.selectbox(
        label="**New dataset**",
        options=options[f"display_{column_display}"],
        help="Dataset containing the new variables. These will replace the old variables in our charts.",
        index=index,
        key="new_dataset_selectbox",
        on_change=_dataset_new_selectbox_on_change,
    )

    ## Old dataset
    # Prepare the list of options to show in the old dataset dropdown.
    options = sort_datasets_old(df).reset_index(drop=True)
    if "old_dataset_selectbox" in st.session_state:
        # If the user chooses a dataset from the dropdown, find the position of that dataset in the list.
        # The dropdown will stay in that dataset.
        index = options[
            (options["display_name"] == st.session_state["old_dataset_selectbox"])
            | (options["display_step"] == st.session_state["old_dataset_selectbox"])
        ].index.item()
    else:
        # If no old dataset has been yet chosen, start the list from the beginning.
        index = 0

    # Old dataset dropdown.
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
            reduced_suggestions = st.toggle(
                "Reduced list of suggestions",
                help="",
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
        st.session_state["not-ignore-all"] = True
        st.session_state["pagination_indicator_mapping"] = 1

    # Get IDs of the chosen old and new datasets.
    dataset_old_id = display_name_to_id_mapping.get(dataset_old) or display_step_to_id_mapping.get(dataset_old)
    dataset_new_id = display_name_to_id_mapping.get(dataset_new) or display_step_to_id_mapping.get(dataset_new)

    return SearchConfigForm(
        dataset_old_id=str(dataset_old_id),
        dataset_new_id=str(dataset_new_id),
        map_identical_indicators=map_identical,
        similarity_function_name=similarity_name,
        enable_bulk_explore=enable_bulk_explore,
        complete_suggestions=not reduced_suggestions,
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
    complete_suggestions: bool

    def __init__(self, **data: Any) -> None:
        """Constructor."""
        super().__init__(**data)
