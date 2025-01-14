import pandas as pd
import streamlit as st
from structlog import get_logger

from apps.wizard.app_pages.dashboard.agrid import clear_aggrid_selections
from apps.wizard.app_pages.dashboard.utils import NON_UPDATEABLE_IDENTIFIERS, _add_steps_to_operations, remove_step
from apps.wizard.utils.components import st_horizontal

log = get_logger()


def render_operations_list(steps_df: pd.DataFrame, steps_info):
    """Render operations list."""

    # Title
    with st_horizontal():
        st.markdown("""### Operations list""")

        # Show info on number of selected steps (if any)
        if len(st.session_state.selected_steps) > 0:
            st.markdown(f":primary-background[{len(st.session_state.selected_steps)} steps]")

    # Show warning if no step is selected
    if st.session_state.selected_steps == []:
        st.warning("No rows selected. Please select at least one dataset from the table above.")
        return

    # Get details of selected steps
    # st.session_state.selected_steps = list(selected_steps_info.keys())
    # st.write(selected_steps_info)
    # st.write(st.session_state.selected_steps)
    # st.write(selected_steps)

    # Show the actual "operations list"
    with st.container(border=True):
        # Create an operations list, that contains the steps (selected from the main steps table) we will operate upon.
        # Note: Selected steps might contain steps other those selected in the main steps table, based on user selections (e.g. dependencies).
        if st.session_state.selected_steps:
            for index, step in enumerate(st.session_state.selected_steps):
                # Define the layout of the list.
                selected_step_info = steps_info[step]
                _show_row_with_step_details(steps_df, step, index, selected_step_info)

            # Show main buttons in the operations list
            _show_main_operations_buttons(steps_df)

        else:
            st.markdown(":grey[_No rows selected for operation..._]")


def _show_row_with_step_details(steps_df, step, index, step_info):
    """Show row in the operations list with the details of the given step.

    # Define the columns in order (from left to right) as a list of tuples (message, key suffix, function).

    TODO: Consider adding step buttons to:
        * Execute ETL step for only the current step.
        * Edit metadata for the current step.
    """
    col1, col2 = st.columns([2.5, 4], vertical_alignment="center")
    with col1:
        with st.container(height=None, border=False):
            # with col1:
            step_alias = step.replace("data://", "")
            if step in st.session_state.selected_steps_table:
                text = f"**{step_alias}**"
                with st.popover(text, use_container_width=False, icon=":material/table_chart:"):
                    _show_step_details(step_info)
            else:
                text = step_alias
                with st.popover(text, use_container_width=False):
                    _show_step_details(step_info)

    with col2:
        with st_horizontal(justify_content="space-between"):
            actions = [
                (
                    "Add direct dependencies",
                    "direct_dependencies",
                    "Add direct dependencies of this step to the **Operations list**. Direct dependencies are steps that are loaded directly by the current step.",
                ),
                (
                    "Add all dependencies",
                    "all_active_dependencies",
                    "Add all dependencies (including indirect dependencies) of this step to the **Operations list**. Indirect dependencies are steps that are needed, but not directly loaded, by the current step. In other words: dependencies of dependencies.",
                ),
                (
                    "Add direct usages",
                    "direct_usages",
                    "Add direct usages of this step to the **Operations list**. Direct usages are those steps that load the current step directly.",
                ),
                (
                    "Add all usages",
                    "all_active_usages",
                    "Add all usages (including indirect usages) of this step to the **Operations list**. Indirect usages are those steps that need, but do not directly load, the current step. In other words: usages of usages.",
                ),
            ]
            unique_key = f"remove_{step}_{index}"
            if step in st.session_state.selected_steps_table:
                st.button(
                    label="🗑️",
                    key=unique_key,
                    on_click=lambda step=step: remove_step(step),
                    help="To remove this step, please unselect it from the table above.",
                    type="secondary",
                    disabled=True,
                )
            else:
                st.button(
                    label="🗑️",
                    key=unique_key,
                    on_click=lambda step=step: remove_step(step),
                    help="Remove this step from the **Operations list**.",
                    type="secondary",
                )
            # Display the operations list.
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


def _show_main_operations_buttons(steps_df):
    """Show main buttons in the operations list:

    - Clear list
    - Remove non-updateable
    - Replace with latest versions
    """

    def _clear_list():
        st.session_state.selected_steps_table.clear()
        st.session_state.selected_steps_extra.clear()

        # Clear the selection in the ag-grid table
        clear_aggrid_selections()

    with st_horizontal():
        # Add button to clear the operations list.
        if st.button(
            ":primary[:material/clear_all: Clear list]",
            help="Remove all steps currently in the **Operations list**.",
            # type="tertiary",
            key="clear_operations_list",
            # on_click=lambda: _clear_list(),
        ):
            _clear_list()

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
            help="Replace steps in the **Operations list** by their latest version available. You may want to use this button after updating steps, to be able to operate on the newly created steps.",
            # type="tertiary",
            key="replace_with_latest",
            on_click=lambda: _upgrade_steps_in_operations_list(steps_df),
        )


def _include_related_steps(steps_df: pd.DataFrame, step: str, column_related: str):
    """User can add additional steps to the operations list based on the selected step.

    E.g. adding direct dependencies, all usages, etc.
    """
    steps_related = steps_df[steps_df["step"] == step]
    if len(steps_related) == 0:
        log.error(f"Step {step} not found in the steps table.")
    elif len(steps_related) == 1:
        steps_related = steps_df[steps_df["step"] == step][column_related].item()
        # Add steps to operations list
        _add_steps_to_operations(steps_related)
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


def _upgrade_steps_in_operations_list(steps_df):
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
