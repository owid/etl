from typing import List

import pandas as pd
import streamlit as st
from rapidfuzz import fuzz
from structlog import get_logger

from apps.wizard.app_pages.dashboard.utils import NON_UPDATEABLE_IDENTIFIERS, unselect_step
from apps.wizard.utils.components import st_horizontal

log = get_logger()

# Operations allowed for each step
OPERATIONS = [
    (
        ":material/delete_outline:",
        "unselect_step",
        "Unselect step and remove it from the selection list.",
    ),
    (
        ":material/add: direct dependencies",
        "direct_dependencies",
        "Add direct dependencies of this step to the selection. Direct dependencies are steps that are loaded directly by the current step.",
    ),
    (
        ":material/add: _all_ dependencies",
        "all_active_dependencies",
        "Add all dependencies (including indirect dependencies) of this step to the selection. Indirect dependencies are steps that are needed, but not directly loaded, by the current step. In other words: dependencies of dependencies.",
    ),
    (
        ":material/add:  direct usages",
        "direct_usages",
        "Add direct usages of this step to the selection. Direct usages are those steps that load the current step directly.",
    ),
    (
        ":material/add: _all_ usages",
        "all_active_usages",
        "Add all usages (including indirect usages) of this step to the selection. Indirect usages are those steps that need, but do not directly load, the current step. In other words: usages of usages.",
    ),
]
# Display format for operations
OPERATIONS_FORMAT = {op[1]: op[0] for op in OPERATIONS}
OPERATIONS_NAMES = list(OPERATIONS_FORMAT.keys())
# Help text
HELP_TEXT = "List of steps to be operated on. You can add, remove, and update steps here. Accepted operations:\n"
for op in OPERATIONS:
    text = f"- **{op[0]}**: {op[2]}"
    HELP_TEXT += f"\n{text}"

HELP_TEXT += "\n\nSteps with ':material/table_chart:' and in bold come from the main table. This means that they were imported by selecting rows in the main table, and clicking on 'Add steps' from the preview section. Instead, steps added as dependencies or usage come in regular font and without any icon."


@st.fragment
def render_selection_list(steps_df):
    """Render selection list."""
    if st.session_state.selected_steps:
        with st_horizontal():
            st.markdown("""### Selection""", help=HELP_TEXT)
            num_steps = len(st.session_state.selected_steps)
            if num_steps == 1:
                text = ":primary-background[1 step]"
            else:
                text = f":primary-background[{len(st.session_state.selected_steps)} steps]"
            st.markdown(text)

        with st.container(border=True):
            text = st.text_input(
                "Step",
                value=None,
                key="dashboard_step_search",
                placeholder="Search step...",
                label_visibility="collapsed",
            )
            st.session_state.selected_steps_sorted = st.session_state.selected_steps
            if text is not None:
                st.session_state.selected_steps_sorted = sorted(
                    st.session_state.selected_steps,
                    key=lambda step_name: fuzz.ratio(text, step_name),
                    reverse=True,
                )
            for step in st.session_state.selected_steps_sorted:
                # Define the layout of the list.
                _show_row_with_step_details(step, steps_df)

            # Show main buttons in the selection
            _show_main_buttons(steps_df)

    else:
        st.markdown("""### Selection""", help=HELP_TEXT)
        st.warning("No datasets selected. Please add at least one dataset from the preview list.")


def _show_row_with_step_details(step, steps_df):
    """Show row in the selection list with the details of the given step.

    # Define the columns in order (from left to right) as a list of tuples (message, key suffix, function).

    TODO: Consider adding step buttons to:
        * Execute ETL step for only the current step.
        * Edit metadata for the current step.
    """
    if step in st.session_state.preview_steps:
        step_name = f":material/table_chart: **{step.replace('data://', '')}**"
    else:
        step_name = f"{step.replace('data://', '')}"

    # Unique key for the row
    unique_key = f"selection_{step}"

    # with st.container(border=True):
    cols = st.columns([1, 2])
    with cols[0]:
        st.markdown(step_name)
    with cols[1]:
        st.pills(
            step_name,
            options=OPERATIONS_NAMES,
            format_func=lambda x: OPERATIONS_FORMAT.get(x, x),
            on_change=lambda: _on_operations_pills_change(unique_key, step, steps_df),
            key=f"{unique_key}_pills",
            label_visibility="collapsed",
        )


def _on_operations_pills_change(unique_key: str, step: str, steps_df: pd.DataFrame):
    st.session_state[unique_key] = st.session_state[f"{unique_key}_pills"]
    st.session_state[f"{unique_key}_pills"] = None

    # Perform operation
    st.toast(f"`{step}`: You selected **{st.session_state[unique_key]}**")

    # Perform operation
    v = st.session_state[unique_key]
    if v == "unselect_step":
        unselect_step(step)
    else:
        _include_related_steps(steps_df, step, v)


def _show_main_buttons(steps_df):
    """Show main buttons in the selection list:

    - Clear list
    - Remove non-updateable
    - Replace with latest versions
    """
    with st_horizontal():
        # Add button to clear the selection list.
        st.button(
            ":primary[:material/clear_all: Clear list]",
            help="Remove all steps currently in the selection.",
            # type="tertiary",
            key="clear_operations_list",
            on_click=lambda: st.session_state.selected_steps.clear(),
        )

        st.button(
            label=":primary[:material/delete_outline: Remove non-updateable (e.g. population)]",
            help="Remove steps that cannot be updated (i.e. with `update_period_days=0`), and other auxiliary datasets, namely: "
            + "\n- ".join(sorted(NON_UPDATEABLE_IDENTIFIERS)),
            # type="tertiary",
            key="remove_non_updateable",
            on_click=lambda: _remove_non_updateable_steps(steps_df),
        )

        st.button(
            label=":primary[:material/system_update_alt: Replace steps with their latest versions]",
            help="Replace steps in the selection by their latest version available. You may want to use this button after updating steps, to be able to operate on the newly created steps.",
            # type="tertiary",
            key="replace_with_latest",
            on_click=lambda: _upgrade_steps_in_selection(steps_df),
        )


def _include_related_steps(steps_df: pd.DataFrame, step: str, column_related: str):
    """User can add additional steps to the selection on the selected step.

    E.g. adding direct dependencies, all usages, etc.
    """
    steps_related = steps_df[steps_df["step"] == step]
    if len(steps_related) == 0:
        log.error(f"Step {step} not found in the steps table.")
    elif len(steps_related) == 1:
        # Add steps to selection
        _add_steps_to_selection(steps_related[column_related].item())
    else:
        st.error(f"More than one step found with the same URI {step}!")
        st.stop()


def _remove_non_updateable_steps(steps_df):
    # Remove steps that cannot be updated (because update_period_days is set to 0).
    # For convenience, also remove steps that a user most likely doesn't want to update.
    non_updateable_steps = steps_df[
        (steps_df["update_period_days"] == 0) | (steps_df["identifier"].isin(NON_UPDATEABLE_IDENTIFIERS))
    ]["step"].tolist()
    st.session_state.selected_steps = [
        step for step in st.session_state.selected_steps if step not in non_updateable_steps
    ]


def _upgrade_steps_in_selection(steps_df):
    new_list = []
    for step in st.session_state.selected_steps:
        step_info = steps_df[steps_df["step"] == step].iloc[0].to_dict()
        step_identifier = step_info["identifier"]
        latest_version = step_info["latest_version"]
        step_latest = steps_df[(steps_df["identifier"] == step_identifier) & (steps_df["version"] == latest_version)][
            "step"
        ]
        if not step_latest.empty:
            new_list.append(step_latest.item())
        else:
            new_list.append(step)

    st.session_state.selected_steps = new_list


def import_steps_from_preview():
    """Display button to add selected steps to the selection."""
    # Button to add selected steps to the selection.
    num_steps = len(st.session_state.preview_steps)
    if num_steps == 1:
        text = "Select step"
    else:
        text = f"Select {num_steps} steps"

    # Only if there are steps in the preview list and they are not in the selection list
    missing_steps = {step for step in st.session_state.preview_steps if step not in st.session_state.selected_steps}
    num_steps_missing = len(missing_steps)
    if num_steps_missing == 0:
        kwargs = {
            "label": "Already selected",
            "type": "primary",
            "help": "Nothing to import to the selection list. All steps in preview have already been selected!",
            "disabled": True,
        }
    else:
        if num_steps_missing != num_steps:
            text = f"{text} ({num_steps_missing} missing)"
        kwargs = {
            "label": text,
            "type": "primary",
            "help": "Import steps to the selection list.",
            "disabled": False,
        }
    if st.button(**kwargs):
        _add_steps_to_selection(st.session_state.preview_steps)
        st.rerun()


def _add_steps_to_selection(steps_related: List[str]):
    """Add steps to the selection."""
    # Remove those already in selection
    new_selected_steps = [step for step in steps_related if step not in st.session_state.selected_steps]
    # Add new steps to the selection
    st.session_state.selected_steps += new_selected_steps
