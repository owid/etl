from typing import Any, Dict

import streamlit as st
from structlog import get_logger

from apps.wizard.utils.components import st_horizontal

log = get_logger()

# Height of the container for the details list (in pixels).
DETAILS_LIST_CONTAINER_HEIGHT = 300


def render_preview_list(steps_info):
    # UI: Display details of selected steps
    with st_horizontal():
        for selected_step in st.session_state.preview_steps:
            preview_step_info = steps_info[selected_step]
            step_alias = selected_step.replace("data://", "")

            # st.write(selected_step)
            with st.popover(step_alias, icon=":material/info:"):
                #     # _render_step_in_list(selected_step_info)
                _show_step_details(preview_step_info)


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
