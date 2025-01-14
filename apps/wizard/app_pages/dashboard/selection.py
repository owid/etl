import pandas as pd
import streamlit as st
from structlog import get_logger

from apps.wizard.app_pages.dashboard.utils import NON_UPDATEABLE_IDENTIFIERS, _add_steps_to_selection, remove_step
from apps.wizard.utils.components import st_horizontal

log = get_logger()


def render_selection_list(steps_df):
    """Render selection list."""
    help_text = "List of steps to be operated on. You can add, remove, and update steps here."
    if st.session_state.selected_steps:
        with st_horizontal():
            st.markdown("""### Selection""", help=help_text)
            num_steps = len(st.session_state.selected_steps)
            if num_steps == 1:
                text = ":primary-background[1 step]"
            else:
                text = f":primary-background[{len(st.session_state.selected_steps)} steps]"
            st.markdown(text)

        with st.container(border=True):
            for index, step in enumerate(st.session_state.selected_steps):
                # Define the layout of the list.
                _show_row_with_step_details(steps_df, step, index)

            # Show main buttons in the selection
            _show_main_buttons(steps_df)

    else:
        st.markdown("""### Selection""", help=help_text)
        st.warning("No datasets selected. Please add at least one dataset from the preview list.")


def _show_row_with_step_details(steps_df, step, index):
    """Show row in the selection list with the details of the given step.

    # Define the columns in order (from left to right) as a list of tuples (message, key suffix, function).

    TODO: Consider adding step buttons to:
        * Execute ETL step for only the current step.
        * Edit metadata for the current step.
    """
    col1, col2 = st.columns([2.5, 4])
    with col1:
        with st.container(height=40, border=False):
            # with col1:
            if step in st.session_state.selected_steps_table:
                st.markdown(f"**{step.replace('data://', '')}**")
            else:
                st.markdown(f"{step.replace('data://', '')}")
    with col2:
        with st_horizontal(justify_content="space-between"):
            actions = [
                (
                    "Add direct dependencies",
                    "direct_dependencies",
                    "Add direct dependencies of this step to the selection. Direct dependencies are steps that are loaded directly by the current step.",
                ),
                (
                    "Add all dependencies",
                    "all_active_dependencies",
                    "Add all dependencies (including indirect dependencies) of this step to the selection. Indirect dependencies are steps that are needed, but not directly loaded, by the current step. In other words: dependencies of dependencies.",
                ),
                (
                    "Add direct usages",
                    "direct_usages",
                    "Add direct usages of this step to the selection. Direct usages are those steps that load the current step directly.",
                ),
                (
                    "Add all usages",
                    "all_active_usages",
                    "Add all usages (including indirect usages) of this step to the selection. Indirect usages are those steps that need, but do not directly load, the current step. In other words: usages of usages.",
                ),
            ]
            unique_key = f"remove_{step}_{index}"
            st.button(
                label="🗑️",
                key=unique_key,
                on_click=lambda step=step: remove_step(step),
                help="Remove this step from the selection.",
                type="secondary",
            )
            # Display the selection list.
            for action_name, key_suffix, help_text in actions:
                # Create a unique key for the button (if any button is to be created).
                unique_key = f"{key_suffix}_{step}_{index}"
                # Add related steps
                st.button(
                    label=f":blue[{action_name}]",
                    key=unique_key,
                    on_click=lambda step=step, key_suffix=key_suffix: _include_related_steps(
                        steps_df, step, key_suffix
                    ),
                    help=help_text,
                    type="tertiary",
                )


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
        steps_related = steps_df[steps_df["step"] == step][column_related].item()
        # Add steps to selection
        _add_steps_to_selection(steps_related)
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
