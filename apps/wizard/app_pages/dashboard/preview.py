from typing import Any, Dict, Optional

import pandas as pd
import streamlit as st
from structlog import get_logger

from apps.wizard.app_pages.dashboard.utils import _add_steps_to_operations

log = get_logger()

# Height of the container for the details list (in pixels).
DETAILS_LIST_CONTAINER_HEIGHT = 300


def render_preview_list(df: Optional[pd.DataFrame], steps_df: pd.DataFrame):
    st.markdown(
        "### Preview datasets",
        help="Preview of the selected steps.\n\nTo actually perform actions on them, please click on **Add steps** button.",
    )

    # Check that there are rows selected in the table.
    if df is None:
        st.warning("No rows selected. Please select at least one dataset from the table above.")
        return

    # Get details of selected steps
    selected_steps_info = _get_selected_steps_info(df, steps_df)

    with st.container(border=True):
        # UI: Display details of selected steps
        # with st.container(border=True, height=DETAILS_LIST_CONTAINER_HEIGHT):
        for selected_step, selected_steps_info in selected_steps_info.items():
            with st.expander(f"Details for step {selected_step}"):
                _render_step_in_list(selected_steps_info)

        # UI: Button to add selected steps to the Operations list.
        _show_button_add_to_operations(df)


def _get_selected_steps_info(df, steps_df) -> Dict[str, Any]:
    """From given list of selected steps, get details for each step.

    df: DataFrame with selected steps. It is usually the grid_response["selected_rows"].
    steps_df: DataFrame with all steps. Output of load_steps_df.
    """
    # Get list of selected steps
    selected_steps = df["step"].tolist()
    # Get details for selected steps
    selected_steps_info = (
        steps_df[steps_df["step"].isin(selected_steps)][
            [
                "step",
                "all_active_dependencies",
                "all_active_usages",
                "updateable_dependencies",
            ]
        ]
        .set_index("step")
        .to_dict(orient="index")
    )
    return selected_steps_info


def _render_step_in_list(selected_step_info: Any):
    """Show the various details of the selected step in a list."""
    # Display each selected row's data.
    for item, value in selected_step_info.items():
        item_name = item.replace("_", " ").capitalize()
        if isinstance(value, list):
            list_html = (
                f"<details><summary> {item_name} ({len(value)}) </summary><ol>"
                + "".join([f"<li>{sub_value}</li>" for sub_value in value])
                + "</ol></details>"
            )
            st.markdown(list_html, unsafe_allow_html=True)
        else:
            st.text(f"{item_name}: {value}")


def _show_button_add_to_operations(df):
    """Display button to add selected steps to the Operations list."""
    # Button to add selected steps to the Operations list.
    if st.button("Add steps", type="primary", help="Add steps to the **Operations list**"):
        if df is None:
            st.error("No rows selected in table. Please select at least one dataset")
            st.stop()
        new_selected_steps = df["step"].tolist()
        st.session_state.selected_steps_table += new_selected_steps
        _add_steps_to_operations(new_selected_steps)
