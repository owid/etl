"""Meadow phase."""
import os
from pathlib import Path
from typing import cast

import streamlit as st
from owid.catalog import Dataset
from st_pages import add_indentation
from typing_extensions import Self

from apps.utils.files import add_to_dag, generate_step_to_channel
from apps.wizard import utils
from apps.wizard.etl_steps.utils import load_datasets
from etl.paths import BASE_DIR, DAG_DIR, MEADOW_DIR
from etl.steps import load_from_uri

#########################################################
# CONSTANTS #############################################
#########################################################

# Page config
st.set_page_config(page_title="Wizard: Create a Meadow step", page_icon="🪄")
add_indentation()

# Available namespaces
OPTIONS_NAMESPACES = sorted(os.listdir(MEADOW_DIR))

# Get current directory
CURRENT_DIR = Path(__file__).parent
# State management
st.session_state["step_name"] = "meadow"
APP_STATE = utils.AppState()
# Config style
utils.config_style_html()
# DUMMY defaults
dummy_values = {
    "namespace": "dummy",
    "version": utils.DATE_TODAY,
    "short_name": "dummy",
    "snapshot_version": utils.DATE_TODAY,
    "file_extension": "csv",
}


#########################################################
# FUNCTIONS & CLASSES ###################################
#########################################################
@st.cache_data
def load_instructions() -> str:
    """Load snapshot step instruction text."""
    with open(file=utils.MD_MEADOW, mode="r") as f:
        return f.read()


class MeadowForm(utils.StepForm):
    """Meadow step form."""

    step_name: str = "meadow"

    short_name: str
    namespace: str
    version: str
    snapshot_version: str
    file_extension: str
    add_to_dag: bool
    dag_file: str
    generate_notebook: bool
    is_private: bool

    def __init__(self: Self, **data: str | bool) -> None:
        """Construct class."""
        data["add_to_dag"] = data["dag_file"] != utils.ADD_DAG_OPTIONS[0]

        # Handle custom namespace
        if "namespace_custom" in data:
            data["namespace"] = str(data["namespace_custom"])

        super().__init__(**data)

    def validate(self: "MeadowForm") -> None:
        """Check that fields in form are valid.

        - Add error message for each field (to be displayed in the form).
        - Return True if all fields are valid, False otherwise.
        """
        # Check other fields (non meta)
        fields_required = ["namespace", "version", "short_name", "snapshot_version", "file_extension"]
        fields_snake = ["namespace", "short_name"]
        fields_version = ["version", "snapshot_version"]

        self.check_required(fields_required)
        self.check_snake(fields_snake)
        self.check_is_version(fields_version)


def update_state() -> None:
    """Submit form."""
    # Create form
    form = MeadowForm.from_state()
    # Update states with values from form
    APP_STATE.update_from_form(form)


#########################################################
# MAIN ##################################################
#########################################################
# PRE-LOAD METADATA
st.selectbox(
    label="Edit metadata from existing dataset",
    options=load_datasets("://meadow/"),
    placeholder="(Experimental) Edit metadata from existing dataset",
    index=None,
    help="You can fill the metadata fields with the metadata from an existing dataset or snapshot. This is useful when updating a step",
    label_visibility="collapsed",
    key="meadow.edit_dataset",
)
ds_edit = None
if st.session_state["meadow.edit_dataset"]:
    try:
        ds_edit = cast(Dataset, load_from_uri(uri=st.session_state["meadow.edit_dataset"]))
        APP_STATE.set_dataset_to_edit(ds_edit)
    except Exception:
        st.error(
            f"Error loading metadata for {st.session_state['meadow.edit_dataset']}. Remember to run `etl run {st.session_state['meadow.edit_dataset']}` first."
        )
        st.stop()
else:
    APP_STATE.reset_dataset_to_edit()

# TITLE
if st.session_state["meadow.edit_dataset"]:
    st.title("Edit step  **:gray[Meadow]**")
else:
    st.title("Create step  **:gray[Meadow]**")

# SIDEBAR
with st.sidebar:
    # utils.warning_metadata_unstable()
    with st.expander("**Instructions**", expanded=True):
        text = load_instructions()
        st.markdown(text)


# FORM
form_widget = st.empty()
with form_widget.form("meadow"):
    # Namespace
    namespace_field = [st.empty(), st.container()]
    # Meadow version
    if (default_version := APP_STATE.default_value("version")) == "":
        default_version = APP_STATE.default_value("snapshot_version")
    APP_STATE.st_widget(
        st_widget=st.text_input,
        label="Meadow dataset version",
        help="Version of the meadow dataset (by default, the current date, or exceptionally the publication date).",
        key="version",
        value=dummy_values["version"] if APP_STATE.args.dummy_data else default_version,
    )
    # Meadow short name
    APP_STATE.st_widget(
        st_widget=st.text_input,
        label="Meadow dataset short name",
        help="Dataset short name using [snake case](https://en.wikipedia.org/wiki/Snake_case). Example: natural_disasters",
        placeholder="Example: 'cherry_blossom'",
        key="short_name",
        value=dummy_values["short_name"] if APP_STATE.args.dummy_data else None,
    )

    st.markdown("#### Dependencies")
    # Snapshot version
    APP_STATE.st_widget(
        st.text_input,
        label="Snapshot version",
        help="Version of the snapshot dataset (by default, the current date, or exceptionally the publication date).",
        # placeholder=f"Example: {DATE_TODAY}",
        key="snapshot_version",
        value=dummy_values["snapshot_version"] if APP_STATE.args.dummy_data else None,
    )
    # File extension
    APP_STATE.st_widget(
        st.text_input,
        label="File extension",
        help="File extension (without the '.') of the file to be downloaded.",
        placeholder="Example: 'csv', 'xls', 'zip'",
        key="file_extension",
        value=dummy_values["file_extension"] if APP_STATE.args.dummy_data else None,
    )

    st.markdown("#### Others")
    # Add to DAG
    APP_STATE.st_widget(
        st.selectbox,
        label="Add to DAG",
        options=utils.ADD_DAG_OPTIONS,
        key="dag_file",
        help="Add ETL step to a DAG file. This will allow it to be tracked and executed by the `etl` command.",
    )
    # Notebook
    APP_STATE.st_widget(
        st.toggle,
        label="Generate playground notebook",
        key="generate_notebook",
        default_last=False,
    )
    # Private?
    APP_STATE.st_widget(
        st.toggle,
        label="Make dataset private",
        key="is_private",
        default_last=False,
    )

    # SUBMIT
    submitted = st.form_submit_button(
        "Submit",
        type="primary",
        use_container_width=True,
        on_click=update_state,
    )


# Render responsive namespace field
utils.render_responsive_field_in_form(
    key="namespace",
    display_name="Namespace",
    field_1=namespace_field[0],
    field_2=namespace_field[1],
    options=OPTIONS_NAMESPACES,
    custom_label="Custom namespace...",
    help_text="Institution or topic name",
    app_state=APP_STATE,
    default_value=dummy_values["namespace"] if APP_STATE.args.dummy_data else OPTIONS_NAMESPACES[0],
)

#########################################################
# SUBMISSION ############################################
#########################################################
if submitted:
    # Create form
    form = cast(MeadowForm, MeadowForm.from_state())

    if not form.errors:
        # Remove form from UI
        form_widget.empty()

        # User asked for private mode?
        private_suffix = "-private" if form.is_private else ""

        # Handle addition to the DAG
        dag_path = DAG_DIR / form.dag_file
        if form.add_to_dag:
            dag_content = add_to_dag(
                dag={
                    f"data{private_suffix}://meadow/{form.namespace}/{form.version}/{form.short_name}": [
                        f"snapshot{private_suffix}://{form.namespace}/{form.snapshot_version}/{form.short_name}.{form.file_extension}",
                    ]
                },
                dag_path=dag_path,
            )
        else:
            dag_content = ""

        # Create necessary files
        DATASET_DIR = generate_step_to_channel(
            cookiecutter_path=utils.COOKIE_MEADOW, data=dict(**form.dict(), channel="meadow")
        )

        step_path = DATASET_DIR / (form.short_name + ".py")
        notebook_path = DATASET_DIR / "playground.ipynb"

        if (not form.generate_notebook) and (notebook_path.is_file()):
            os.remove(notebook_path)

        # Preview generated
        st.subheader("Generated files")
        utils.preview_file(step_path, "python")
        if form.generate_notebook:
            with st.expander(f"File: `{notebook_path}`", expanded=False):
                st.markdown("Open the file to see the generated notebook.")
        utils.preview_dag_additions(dag_content=dag_content, dag_path=dag_path)

        # Display next steps
        with st.expander("⏭️ **Next steps**", expanded=True):
            # 1/ Run ETL step
            st.markdown("#### 1. Run ETL step")
            st.code(
                f"poetry run etl run data{private_suffix}://meadow/{form.namespace}/{form.version}/{form.short_name} {'--private' if form.is_private else ''}",
                language="shellSession",
            )

            container = st.container(border=True)
            if form.generate_notebook or dag_content:
                with st.container(border=True):
                    if form.generate_notebook:
                        # 2/ Open notebook
                        st.markdown("**(Optional)**")
                        # A/ Playground notebook
                        st.markdown("#### Playground notebook")
                        st.markdown(
                            f"Use the generated notebook `{notebook_path.relative_to(BASE_DIR)}` to examine the dataset output interactively."
                        )
                    if dag_content:
                        st.markdown("#### Organize the DAG")
                        st.markdown(f"Check the DAG `{dag_path}`.")

            st.markdown("#### 2. Proceed to next step")
            utils.st_page_link("garden", use_container_width=True, border=True)

        # User message
        st.toast("Templates generated. Read the next steps.", icon="✅")

        # Update config
        utils.update_wizard_config(form=form)
    else:
        st.write(form.errors)
        st.error("Form not submitted! Check errors!")
