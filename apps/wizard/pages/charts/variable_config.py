"""Concerns the second stage of wizard charts, when the variable mapping is constructed."""
from typing import Any, Dict, List

import pandas as pd
import streamlit as st
from pydantic import BaseModel
from streamlit_extras.grid import grid
from structlog import get_logger

from apps.backport.datasync.data_metadata import variable_data_df_from_s3
from apps.wizard.pages.charts.utils import get_variables_from_datasets
from apps.wizard.utils import set_states
from apps.wizard.utils.env import OWID_ENV
from etl.db import get_engine
from etl.match_variables import find_mapping_suggestions, preliminary_mapping

# Logger
log = get_logger()
# App can't hanle too many variables to map. We set an upper limit
LIMIT_VARS_TO_MAP = 100


@st.cache_data(show_spinner=False)
def find_mapping_suggestions_cached(missing_old, missing_new, similarity_name):
    return find_mapping_suggestions(
        missing_old=missing_old,
        missing_new=missing_new,
        similarity_name=similarity_name,
    )  # type: ignore


def ask_and_get_variable_mapping(search_form) -> "VariableConfig":
    """Ask and get variable mapping."""
    variable_config = VariableConfig()

    ###########################################################################
    # 1/ PROCESSING: Get variables, find similarities and suggestions, etc.
    ###########################################################################

    # 1.1/ INTERNAL PROCESSING
    # Get variables from old and new datasets
    old_variables, new_variables = get_variables_from_datasets(
        search_form.dataset_old_id,
        search_form.dataset_new_id,
        show_new_not_in_old=False,
    )

    # 1.2/ Build display mappings: id -> display_name
    ## This is to display the variables in the selectboxes with format "[id] name"
    df = pd.concat([old_variables, new_variables], ignore_index=True)  # .drop_duplicates()
    df["display_name"] = "[" + df["id"].astype(str) + "] " + df["name"]
    variable_id_to_display = df.set_index("id")["display_name"].to_dict()

    # 1.3/ Get auto variable mapping (if mapping by identical name is enabled)
    ## Note that when the old and new datasets are the same, this option is disabled. Otherwise all variables are mapped (which probably does not make sense?)
    if search_form.dataset_old_id == search_form.dataset_new_id:
        match_identical_vars = False
        match_identical_vars = search_form.map_identical_variables
    else:
        match_identical_vars = search_form.map_identical_variables
    mapping, missing_old, missing_new = preliminary_mapping(
        old_variables=old_variables,
        new_variables=new_variables,
        match_identical=match_identical_vars,
    )
    if not mapping.empty:
        variable_mapping_auto = mapping.set_index("id_old")["id_new"].to_dict()
    else:
        variable_mapping_auto = {}

    # 1.4/ Get remaining mapping suggestions
    # This is for those variables which couldn't be automatically mapped
    import time

    print("start")
    t0 = time.time()
    with st.spinner():
        suggestions = find_mapping_suggestions_cached(
            missing_old=missing_old,
            missing_new=missing_new,
            similarity_name=search_form.similarity_function_name,
        )  # type: ignore
    print(time.time() - t0)
    # Sort by max similarity: First suggestion is that one that has the highest similarity score with any of its suggested new vars.
    suggestions = sorted(suggestions, key=lambda x: x["new"]["similarity"].max(), reverse=True)

    # [OPTIONAL] EXPLORE MODE
    # Get data points
    if search_form.enable_explore_mode:
        with st.spinner(
            "Retrieving data values from S3. This might take some time... If you don't need this, disable the 'Explore' option from the 'parameters' section."
        ):
            df_data = get_variable_data_cached(list(set(old_variables["id"]) | set(new_variables["id"])))

    ###########################################################################
    #
    # 2/ DISPLAY: Show the variable mapping form
    #
    ###########################################################################
    if not variable_mapping_auto and not suggestions:
        st.warning(
            f"It looks as the dataset [{search_form.dataset_old_id}]({OWID_ENV.dataset_admin_site(search_form.dataset_old_id)}) has no variable in use in any chart! Therefore, no mapping is needed."
        )
    else:
        with st.container(border=True):
            # 2.1/ DISPLAY MAPPING SECTION
            ## Header
            st.header("Map variables")
            st.markdown(
                "Map variables from the old to the new dataset. The idea is that the variables in the new dataset will replace those from the old dataset in our charts. You can choose to ignore some variables if you want to.",
            )
            # Column proportions per row (out of 1)
            cols = [0.43, 0.07, 0.38, 0.06, 0.06] if search_form.enable_explore_mode else [0.43, 0.07, 0.43, 0.07]

            #################################
            # 2.2/ Header: Titles, links, general checkboxes
            #################################
            grid_variables_header = grid(2, cols, [6, 1, 7])
            # Row 1
            grid_variables_header.subheader("Old dataset")
            grid_variables_header.subheader("New dataset")
            # Row 2
            grid_variables_header.link_button(
                "Explore dataset", OWID_ENV.dataset_admin_site(search_form.dataset_old_id)
            )
            grid_variables_header.caption("Ignore", help="Check to ignore this variable in the mapping.")
            grid_variables_header.link_button(
                "Explore dataset",
                OWID_ENV.dataset_admin_site(search_form.dataset_new_id),
            )
            grid_variables_header.caption(
                "Score",
                help="Similarity score between the old variable and the 'closest' new variable (from 0 to 100%). Variables with low scores are likely not to have a good match.",
            )
            if search_form.enable_explore_mode:
                grid_variables_header.caption(
                    "🔎",
                    help="Explore the distribution of the currently compared variables.",
                )
            # Row 3
            grid_variables_header.empty()
            grid_variables_header.checkbox(
                "All",
                help="Check to ignore all mappings.",
                on_change=lambda: set_states({"submitted_variables": False}),
                key="ignore-all",
            )

            #################################
            # 2.3/ Automatically mapped variables
            #################################
            old_var_selectbox = []
            ignore_selectbox = []
            new_var_selectbox = []

            # Automatically mapped variables (non-editable)
            # st.write(variable_mapping_auto)
            if search_form.enable_explore_mode:
                row_cols = len(variable_mapping_auto) * [cols, 1]
            else:
                row_cols = len(variable_mapping_auto) * [cols]
            grid_variables_auto = grid(*(row_cols))

            # Loop over automatically mapped variables
            for i, (variable_old, variable_new) in enumerate(variable_mapping_auto.items()):
                # Old variable selectbox
                element = grid_variables_auto.selectbox(
                    label=f"auto-{i}-left",
                    options=[variable_old],
                    disabled=True,
                    label_visibility="collapsed",
                    format_func=variable_id_to_display.get,
                )
                old_var_selectbox.append(element)
                # Ignore checkbox
                check = st.session_state.get("ignore-all")
                check = check if check else st.session_state.get(f"auto-ignore-{i}", False)
                element = grid_variables_auto.checkbox(
                    "Ignore",
                    key=f"auto-ignore-{i}",
                    label_visibility="collapsed",
                    value=st.session_state.get("ignore-all", st.session_state.get(f"auto-ignore-{i}", False)),
                    on_change=lambda: set_states({"submitted_variables": False}),
                )
                ignore_selectbox.append(element)
                # New variable selectbox
                element = grid_variables_auto.selectbox(
                    label=f"auto-{i}-right",
                    options=[variable_new],
                    disabled=True,
                    label_visibility="collapsed",
                    format_func=variable_id_to_display.get,
                )
                new_var_selectbox.append(element)
                # Score
                grid_variables_auto.markdown(":violet[**100%**]")
                # (Optional) Explore mode
                if search_form.enable_explore_mode:
                    ## Explore mode checkbox
                    element_check = grid_variables_auto.toggle(
                        "Explore", key=f"auto-explore-{i}", label_visibility="collapsed"
                    )
                    ## Explore mode plot
                    with grid_variables_auto.container():
                        show_explore_df(
                            df_data,  # type: ignore
                            variable_old,  # type: ignore
                            variable_new,  # type: ignore
                            variable_id_to_display,
                            element_check,
                        )  # type: ignore

            #################################
            # 2.4/ Manually mapped variables
            #################################
            # Show only first 100 variables to map (otherwise app crashes)
            if len(suggestions) > LIMIT_VARS_TO_MAP:
                st.warning(
                    f"Too many variables to map! Showing only the first {LIMIT_VARS_TO_MAP}. If you want to map more variables, do it in batches. That is, first map this batch and approve the generated chart revisions in admin. Once you are done, run this app again. Make sure you have approved the previously generated revisions!"
                )
                suggestions = suggestions[:LIMIT_VARS_TO_MAP]

            if search_form.enable_explore_mode:
                row_cols = len(suggestions) * [cols, 1]
            else:
                row_cols = len(suggestions) * [cols]
            grid_variables_manual = grid(*(row_cols))
            # grid_variables_manual = grid(1, 2)
            for i, suggestion in enumerate(suggestions):
                variable_old = suggestion["old"]["id_old"]
                similarity_max = int(suggestion["new"]["similarity"].max().round(0))
                variable_old_manual = grid_variables_manual.selectbox(
                    label=f"manual-{i}-left",
                    options=[variable_old],
                    disabled=True,
                    label_visibility="collapsed",
                    format_func=variable_id_to_display.get,
                )

                old_var_selectbox.append(variable_old_manual)
                # Ignore checkbox
                ## If ignore-all is checked, then inherit. Otherwise preserve value.
                check = st.session_state.get("ignore-all")
                check = check if check else st.session_state.get(f"manual-ignore-{i}", False)
                element_ignore = grid_variables_manual.checkbox(
                    "Ignore",
                    key=f"manual-ignore-{i}",
                    label_visibility="collapsed",
                    value=check,
                    on_change=lambda: set_states({"submitted_variables": False}),
                )
                ignore_selectbox.append(element_ignore)
                # New variable selectbox
                variable_new_manual = grid_variables_manual.selectbox(
                    label=f"manual-{i}-right",
                    options=suggestion["new"]["id_new"],
                    disabled=False,
                    label_visibility="collapsed",
                    format_func=variable_id_to_display.get,
                    on_change=lambda: set_states({"submitted_variables": False}),
                )
                new_var_selectbox.append(variable_new_manual)
                # Score
                if similarity_max > 80:
                    color = "blue"
                elif similarity_max > 60:
                    color = "green"
                elif similarity_max > 40:
                    color = "orange"
                else:
                    color = "red"
                grid_variables_manual.markdown(f":{color}[**{similarity_max}%**]")
                # (Optional) Explore mode
                if search_form.enable_explore_mode:
                    ## Explore mode checkbox
                    element_check = grid_variables_manual.toggle(
                        "Explore", key=f"auto-explore-{i}", label_visibility="collapsed"
                    )
                    ## Explore mode plot
                    with grid_variables_manual.container():
                        show_explore_df(
                            df_data,  # type: ignore
                            variable_old_manual,
                            variable_new_manual,
                            variable_id_to_display,
                            element_check,
                        )  # type: ignore

            #################################
            # 2.5/ Submit button
            #################################
            # Form button
            st.button(
                label="Next (2/3)",
                type="primary",
                use_container_width=True,
                on_click=set_states_after_submitting,
            )

            if st.session_state.submitted_variables:
                # BUILD MAPPING
                variable_mapping = _build_variable_mapping(
                    old_var_selectbox,
                    new_var_selectbox,
                    ignore_selectbox,
                )
                variable_config = VariableConfig(variable_mapping=variable_mapping)
    return variable_config


def show_explore_df(df_data, variable_old, variable_new, variable_id_to_display, element_check) -> None:
    if element_check:  # type: ignore
        with st.container(border=True):
            try:
                plot_comparison_two_variables(df_data, variable_old, variable_new, variable_id_to_display)  # type: ignore
            except Exception:
                st.error(
                    "Something went wrong! This can be due to several reasons: One (or both) of the variables are not numeric, `values` for one of the variables does not have the columns `entityName` and `year`. Please check the error message below. Report the error #002001"
                )
    else:
        st.empty()


class VariableConfig(BaseModel):
    is_valid: bool = False
    variable_mapping: Dict[int, int] = {}

    def __init__(self, **data: Any) -> None:
        """Construct variable config object."""
        if "variable_mapping" in data:
            data["is_valid"] = True
        super().__init__(**data)


@st.cache_data(show_spinner=False)
def _build_variable_mapping(old_var_selectbox, new_var_selectbox, ignore_selectbox) -> Dict[int, int]:
    if len(old_var_selectbox) != len(new_var_selectbox):
        raise ValueError("Something went wrong! The number of old and new variables is different.")
    if len(old_var_selectbox) != len(ignore_selectbox):
        raise ValueError("Something went wrong! The number of old variables and ignore checkboxes is different.")
    if len(new_var_selectbox) != len(ignore_selectbox):
        raise ValueError("Something went wrong! The number of new variables and ignore checkboxes is different.")
    variable_mapping = {
        int(old): int(new)
        for old, new, ignore in zip(old_var_selectbox, new_var_selectbox, ignore_selectbox)
        if not ignore
    }
    return variable_mapping


@st.cache_data(show_spinner=False)
def get_variable_data_cached(variables_ids: List[int]):
    df = variable_data_df_from_s3(get_engine(), variable_ids=[int(v) for v in variables_ids], workers=10)
    return df


@st.cache_data(show_spinner=False)
def build_df_comparison_two_variables_cached(df, variable_old, variable_new, var_id_to_display):
    # Get df with only the two variables, cast to appropriate type
    df_variables = df[df["variableId"].isin([variable_old, variable_new])]
    df_variables.loc[:, "value"] = df_variables.value.astype(float)
    # Reshape dataframe
    df_variables = df_variables.pivot(index=["entityName", "year"], columns="variableId", values="value").reset_index()
    df_variables["Relative difference (abs, %)"] = (
        (100 * (df_variables[variable_old] - df_variables[variable_new]) / df_variables[variable_old]).round(2)
    ).abs()
    df_variables = df_variables.rename(columns=var_id_to_display).sort_values(
        "Relative difference (abs, %)", ascending=False
    )
    # df_variables = df_variables.style.bar(subset=['Relative difference (%)'], color='#d65f5f')
    return df_variables


# @st.cache_data(show_spinner=False)
def plot_comparison_two_variables(df, variable_old, variable_new, var_id_to_display) -> None:
    log.info("table: comparison of two variables")
    df_variables = build_df_comparison_two_variables_cached(df, variable_old, variable_new, var_id_to_display)
    # Show country filters
    # countries = st.multiselect(
    #     "Select locations",
    #     sorted(set(df_variables["entityName"])),
    #     key=f"multi-{variable_old}-{variable_new}-{uuid.uuid4().hex[:10]}",
    # )
    # st.write(countries)
    # if countries:
    #     df_variables = df_variables[df_variables["entityName"].isin(countries)]
    ## Keep only rows with relative difference != 0
    df_variables = df_variables[df_variables["Relative difference (abs, %)"] != 0]

    # Row sanity check
    ## (Streamlit has a limit on the number of rows it can show)
    cell_limit = 262144
    num_cells = df_variables.shape[0] * df_variables.shape[1]
    if num_cells > cell_limit:
        num_rows_new = cell_limit // df_variables.shape[1]
        df_variables = df_variables.head(num_rows_new)
        st.warning(f"Cell limit reached. Only showing first {num_rows_new} rows.")

    ## Replace inf values
    # df_variables = df_variables.replace([np.inf, -np.inf], np.nan)

    # Add bg cell colouring
    # df_variables = df_variables.style.background_gradient(
    #     cmap="OrRd", subset=["Relative difference (abs, %)"], vmin=0, vmax=20
    # )

    # Show table
    st.dataframe(df_variables)


def reset_variable_form() -> None:
    """ "Reset variable form."""
    # Create dictionary with checkboxes set to False
    checks = {
        str(k): False
        for k in st.session_state.keys()
        if str(k).startswith("auto-ignore-") or str(k).startswith("manual-ignore-")
    }
    # Create dictionary with toggles set to False
    toggles = {
        str(k): False
        for k in st.session_state.keys()
        if str(k).startswith("auto-explore-") or str(k).startswith("manual-explore-")
    }
    set_states(
        {
            "ignore-all": False,
            **checks,
            **toggles,
        }
    )


def set_states_after_submitting():
    set_states(
        {
            "submitted_variables": True,
            "submitted_revisions": False,
        },
        logging=True,
    )
    reset_gpt_form()


def reset_gpt_form() -> None:
    """Reset variable form.

    Whenever we change the variable form, we want to disable showing the gpt forms in the next steps.
    """
    # Create dictionary to set gpt forms to False (i.e. not visible)
    settings = {str(k): False for k in st.session_state.keys() if str(k).startswith("chart-experimental-")}
    settings = {
        **settings,
        "gpt_tweaks": {},
    }
    # Set states
    set_states(settings)
