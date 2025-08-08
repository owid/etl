import streamlit as st
from rapidfuzz import fuzz

from apps.wizard import utils
from apps.wizard.etl_steps.forms import CollectionForm
from apps.wizard.etl_steps.utils import dag_files, dag_not_add_option
from apps.wizard.utils.components import preview_file

st.set_page_config(
    page_title="Wizard: Collection Step",
    page_icon="🪄",
)


# SESSION STATE
st.session_state.submit_form = st.session_state.get("submit_form", False)


# FIELDS FROM OTHER STEPS
st.session_state["step_name"] = "collection"
APP_STATE = utils.AppState()
APP_STATE._previous_step = "data"

# Namespaces
OPTIONS_NAMESPACES = utils.get_namespaces("all")


def render_form():
    """Render main part of the form."""
    col1, col2, col3 = st.columns([2, 2, 1])
    #
    # Namespace
    #
    with col1:
        APP_STATE.st_widget(
            st_widget=st.selectbox,
            key="namespace",
            label="Namespace",
            help="Institution or topic name",
            options=OPTIONS_NAMESPACES,
            default_last=OPTIONS_NAMESPACES[0],
            on_change=edit_field,
            accept_new_options=True,
        )

    #
    # Short name
    #
    with col2:
        APP_STATE.st_widget(
            st_widget=st.text_input,
            key="short_name",
            label="short name",
            help="Dataset short name using [snake case](https://en.wikipedia.org/wiki/Snake_case). Example: natural_disasters",
            placeholder="Example: 'cherry_blossom'",
            value=None,
            on_change=edit_field,
        )

    #
    # Version
    #
    if (default_version := APP_STATE.default_value("version", previous_step="snapshot")) == "":
        default_version = APP_STATE.default_value("snapshot_version", previous_step="snapshot")
    with col3:
        APP_STATE.st_widget(
            st_widget=st.text_input,
            label="version",
            help="Version of the dataset (by default, the current date, or exceptionally the publication date).",
            key="version",
            default_last=default_version,
            value=default_version,
            on_change=edit_field,
            placeholder="latest",
        )

    #
    # Add to DAG
    #
    sorted_dag = sorted(
        dag_files,
        key=lambda file_name: fuzz.ratio(file_name.replace(".yml", ""), APP_STATE.vars["namespace"]),
        reverse=True,
    )
    sorted_dag = [
        dag_not_add_option,
        *sorted_dag,
    ]
    if sorted_dag[1].replace(".yml", "") == APP_STATE.vars["namespace"]:
        default_value = sorted_dag[1]
    else:
        default_value = ""

    APP_STATE.st_widget(
        st.selectbox,
        label="Add to DAG",
        options=sorted_dag,
        key="dag_file",
        help="Add ETL step to a DAG file. This will allow it to be tracked and executed by the `etl` command.",
        default_value=default_value,
        on_change=edit_field,
    )


def edit_field() -> None:
    """Submit form."""
    utils.set_states({"submit_form": False})


def submit_form() -> None:
    """Submit form."""
    # Create form
    form = CollectionForm.from_state()
    # Update states with values from form
    APP_STATE.update_from_form(form)

    # Submit
    utils.set_states({"submit_form": True})


# TITLE
st.title(":material/collections: Collection **:gray[Create step]**")

st.markdown(":small[:orange-badge[:material/warning: This app is in development. Please review the generated files.]]")
st.markdown("Use this app to create a collection (previously referred as MDIMs).")
with st.container(border=True):
    render_form()

    st.button(
        label="Submit",
        type="primary",
        width="stretch",
        on_click=submit_form,
    )

#########################################################
# SUBMISSION ############################################
#########################################################
if st.session_state.submit_form:
    # Create form
    form = CollectionForm.from_state()

    # Create files
    generated_files = form.create_files()

    # Add lines to DAG
    dag_content = form.add_steps_to_dag()

    ########################
    # PREVIEW & NEXT STEPS #
    ########################
    utils.preview_dag_additions(dag_content, form.dag_path, prefix="**DAG**", expanded=True)

    # Preview generated
    for f in generated_files:
        preview_file(f["path"], "File", f["language"])

    # Prompt user
    st.toast("Templates generated. Read the next steps.", icon="✅")

    # Update config
    utils.update_wizard_defaults_from_form(form=form)
