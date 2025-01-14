from typing import Any, Dict, List

import streamlit as st
from structlog import get_logger

from apps.wizard.app_pages.dashboard.utils import _add_steps_to_selection
from apps.wizard.utils.components import st_horizontal

log = get_logger()

# Height of the container for the details list (in pixels).
DETAILS_LIST_CONTAINER_HEIGHT = 300


def render_preview_list(selected_steps: List[str], steps_info):
    help_text = (
        "Preview of the selected steps.\n\nTo actually perform actions on them, please click on **Add steps** button."
    )

    st.markdown(
        "### Preview",
        help=help_text,
    )

    # Check that there are rows selected in the table.
    if selected_steps == []:
        st.warning("No rows selected. Please select at least one dataset from the table above.")
        return

    with st.container(border=True):
        # UI: Display details of selected steps
        # with st.container(border=True, height=DETAILS_LIST_CONTAINER_HEIGHT):
        # for selected_step, selected_steps_info in selected_steps_info.items():
        with st_horizontal():
            for selected_step in selected_steps:
                selected_step_info = steps_info[selected_step]
                step_alias = selected_step.replace("data://", "")

                # st.write(selected_step)
                with st.popover(step_alias, icon=":material/info:"):
                    #     # _render_step_in_list(selected_step_info)
                    _show_step_details(selected_step_info)

        # UI: Button to add selected steps to the Operations list.
        _show_button_add_to_selection(selected_steps)


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


def _show_step_details(selected_step_info):
    """Show the various details of the selected step in a list."""
    # Display each selected row's data.
    for item, value in selected_step_info.items():
        item_name = item.replace("_", " ").capitalize()
        if isinstance(value, list):
            text = f"**{item_name} ({len(value)})**\n"
            items = "\n".join([f"- {sub_value}" for sub_value in value])
            st.markdown(text + items)
        else:
            st.text(f"{item_name}: {value}")


def _show_button_add_to_selection(selected_steps):
    """Display button to add selected steps to the selection."""
    # Button to add selected steps to the selection.
    num_steps = len(selected_steps)
    if num_steps == 1:
        text = "Add 1 step"
    else:
        text = f"Add {num_steps} steps"
    if st.button(text, type="primary", help="Add steps to the selection."):
        st.session_state.selected_steps_table += selected_steps
        _add_steps_to_selection(selected_steps)
