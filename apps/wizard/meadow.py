import os
from pathlib import Path
from typing import Any, Dict

import streamlit as st
from st_pages import add_indentation

from apps.wizard import utils
from etl.paths import DAG_DIR, ETL_DIR

#########################################################
# CONSTANTS #############################################
#########################################################

# Page config
st.set_page_config(page_title="Wizard (meadow)", page_icon="🪄")
add_indentation()

# Get current directory
CURRENT_DIR = Path(__file__).parent
# State management
st.session_state["step_name"] = "meadow"
APP_STATE = utils.AppState()
# Config style
utils.config_style_html()

#########################################################
# FUNCTIONS & CLASSES ###################################
#########################################################
@st.cache_data
def load_instructions() -> str:
    """Load snapshot step instruction text."""
    with open(CURRENT_DIR / "meadow.md", "r") as f:
        return f.read()


class MeadowForm(utils.StepForm):
    """Meadow step form."""

    short_name: str
    namespace: str
    version: str
    snapshot_version: str
    file_extension: str
    add_to_dag: bool
    dag_file: str
    generate_notebook: bool
    is_private: bool

    def __init__(self: "MeadowForm", **data: Dict[str, Any]) -> None:
        """Construct class."""
        data["add_to_dag"] = data["dag_file"] != utils.ADD_DAG_OPTIONS[0]
        super().__init__(**data, step_name="meadow")

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
# TITLE
st.title("Wizard  **:gray[Meadow]**")

# SIDEBAR
with st.sidebar:
    with st.expander("**Instructions**", expanded=True):
        text = load_instructions()
        st.markdown(text)

# FORM
form_widget = st.empty()
with form_widget.form("meadow"):
    # Namespace
    APP_STATE.st_widget(
        st.text_input,
        label="Namespace",
        help="Institution or topic name",
        placeholder="Example: 'emdat', 'health'",
        key="namespace",
    )
    # Meadow version
    if (default_version := APP_STATE.default_value("version")) == "":
        default_version = APP_STATE.default_value("snapshot_version")
    APP_STATE.st_widget(
        st.text_input,
        label="Meadow dataset version",
        help="Version of the meadow dataset (by default, the current date, or exceptionally the publication date).",
        key="version",
        value=default_version,
    )
    # Meadow short name
    APP_STATE.st_widget(
        st.text_input,
        label="Meadow dataset short name",
        help="Dataset short name using [snake case](https://en.wikipedia.org/wiki/Snake_case). Example: natural_disasters",
        placeholder="Example: 'cherry_blossom'",
        key="short_name",
    )

    st.markdown("#### Dependencies")
    # Snapshot version
    APP_STATE.st_widget(
        st.text_input,
        label="Snapshot version",
        help="Version of the snapshot dataset (by default, the current date, or exceptionally the publication date).",
        # placeholder=f"Example: {DATE_TODAY}",
        key="snapshot_version",
    )
    # File extension
    APP_STATE.st_widget(
        st.text_input,
        label="File extension",
        help="File extension (without the '.') of the file to be downloaded.",
        placeholder="Example: 'csv', 'xls', 'zip'",
        key="file_extension",
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
        default_last=True,
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


#########################################################
# SUBMISSION ############################################
#########################################################
if submitted:
    # Create form
    form = MeadowForm.from_state()

    if not form.errors:
        # Remove form from UI
        form_widget.empty()

        # User asked for private mode?
        private_suffix = "-private" if form.is_private else ""

        # handle DAG-addition
        dag_path = DAG_DIR / form.dag_file
        if form.add_to_dag:
            dag_content = utils.add_to_dag(
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
        DATASET_DIR = utils.generate_step_to_channel(
            CURRENT_DIR / "meadow_cookiecutter/", dict(**form.dict(), channel="meadow")
        )

        step_path = DATASET_DIR / (form.short_name + ".py")
        notebook_path = DATASET_DIR / "playground.ipynb"

        if not form.generate_notebook:
            os.remove(notebook_path)

        # Display next steps
        st.subheader("Next steps")
        with st.expander("", expanded=True):
            st.markdown(
                f"""
        1. Run `etl` to generate the dataset

            ```
            poetry run etl data{private_suffix}://meadow/{form.namespace}/{form.version}/{form.short_name} {"--private" if form.is_private else ""}
            ```

        2. (Optional) Generated notebook `{notebook_path.relative_to(ETL_DIR)}` can be used to examine the dataset output interactively.

        3. Continue to the garden step
        """
            )

        # Preview generated
        st.subheader("Generated files")
        utils.preview_file(step_path, "python")
        utils.preview_dag_additions(dag_content, dag_path)

        # User message
        st.toast("Templates generated. Read the next steps.", icon="✅")

    else:
        st.write(form.errors)
        st.error("Form not submitted! Check errors!")
