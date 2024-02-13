"""Grapher phase."""
import os
from pathlib import Path
from typing import cast

import streamlit as st
from st_pages import add_indentation
from typing_extensions import Self

from apps.wizard import utils
from etl.paths import DAG_DIR, GRAPHER_DIR

#########################################################
# CONSTANTS #############################################
#########################################################
# Page config
st.set_page_config(page_title="Wizard: Create a Grapher step", page_icon="🪄")
add_indentation()

# Available namespaces
OPTIONS_NAMESPACES = sorted(os.listdir(GRAPHER_DIR))


# Get current directory
CURRENT_DIR = Path(__file__).parent
# FIELDS FROM OTHER STEPS
st.session_state["step_name"] = "grapher"
APP_STATE = utils.AppState()
# Config style
utils.config_style_html()
# DUMMY defaults
dummy_values = {
    "namespace": "dummy",
    "version": utils.DATE_TODAY,
    "short_name": "dummy",
    "garden_version": utils.DATE_TODAY,
}


#########################################################
# FUNCTIONS & CLASSES ###################################
#########################################################
@st.cache_data
def load_instructions() -> str:
    """Load snapshot step instruction text."""
    with open(file=utils.MD_GRAPHER, mode="r") as f:
        return f.read()


class GrapherForm(utils.StepForm):
    """Grapher form."""

    step_name: str = "grapher"

    short_name: str
    namespace: str
    version: str
    garden_version: str
    add_to_dag: bool
    dag_file: str
    is_private: bool

    def __init__(self: Self, **data: str | bool) -> None:
        """Construct class."""
        data["add_to_dag"] = data["dag_file"] != utils.ADD_DAG_OPTIONS[0]

        # Handle custom namespace
        if "namespace_custom" in data:
            data["namespace"] = str(data["namespace_custom"])

        super().__init__(**data)

    def validate(self: Self) -> None:
        """Check that fields in form are valid.

        - Add error message for each field (to be displayed in the form).
        - Return True if all fields are valid, False otherwise.
        """
        # Check other fields (non meta)
        fields_required = ["namespace", "version", "short_name", "garden_version"]
        fields_snake = ["namespace", "short_name"]
        fields_version = ["version", "garden_version"]

        self.check_required(fields_required)
        self.check_snake(fields_snake)
        self.check_is_version(fields_version)


def update_state() -> None:
    """Submit form."""
    # Create form
    form = GrapherForm.from_state()
    # Update states with values from form
    APP_STATE.update_from_form(form)


#########################################################
# MAIN ##################################################
#########################################################
# TITLE
st.title("Create step  **:gray[Grapher]**")

# SIDEBAR
with st.sidebar:
    # utils.warning_metadata_unstable()
    # CONNECT AND
    if APP_STATE.args.run_checks:
        with st.expander("**Environment checks**", expanded=True):
            utils._check_env()
            utils._show_environment()

    # INSTRUCTIONS
    with st.expander("**Instructions**", expanded=True):
        text = load_instructions()
        st.markdown(text)


# FORM
form_widget = st.empty()
with form_widget.form("grapher"):
    # Namespace
    namespace_field = [st.empty(), st.container()]
    # Grapher version
    version_grapher = APP_STATE.st_widget(
        st_widget=st.text_input,
        label="Grapher dataset version",
        help="Version of the grapher dataset (by default, the current date, or exceptionally the publication date).",
        key="version",
        default_last=utils.DATE_TODAY,
        value=dummy_values["version"] if APP_STATE.args.dummy_data else None,
    )
    # Grapher short name
    short_name_grapher = APP_STATE.st_widget(
        st_widget=st.text_input,
        label="Grapher dataset short name",
        help="Dataset short name using [snake case](https://en.wikipedia.org/wiki/Snake_case). Example: natural_disasters",
        placeholder="Example: 'cherry_blossom'",
        key="short_name",
        value=dummy_values["short_name"] if APP_STATE.args.dummy_data else None,
    )

    st.markdown("#### Dependencies")
    # Garden version
    if (default_version := APP_STATE.default_value("garden_version")) == "":
        default_version = APP_STATE.default_value("version", default_last=utils.DATE_TODAY)
    version_snap = APP_STATE.st_widget(
        st_widget=st.text_input,
        label="Garden dataset version",
        help="Version of the garden dataset (by default, the current date, or exceptionally the publication date).",
        key="garden_version",
        value=dummy_values["garden_version"] if APP_STATE.args.dummy_data else default_version,
    )

    st.markdown("#### Others")
    # Add to DAG
    dag_file = APP_STATE.st_widget(
        st_widget=st.selectbox,
        label="Add to DAG",
        options=utils.ADD_DAG_OPTIONS,
        key="dag_file",
        help="Add ETL step to a DAG file. This will allow it to be tracked and executed by the `etl` command.",
    )
    # Private
    private = APP_STATE.st_widget(
        st_widget=st.toggle,
        label="Make dataset private",
        key="is_private",
        default_last=False,
    )

    # Submit
    submitted = st.form_submit_button(
        label="Submit",
        type="primary",
        use_container_width=True,
        on_click=APP_STATE.update,
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
    default_value=dummy_values["namespace"] if APP_STATE.args.dummy_data else "Custom namespace...",
)


#########################################################
# SUBMISSION ############################################
#########################################################
if submitted:
    # Create form
    form = cast(GrapherForm, GrapherForm.from_state())

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
                    f"data{private_suffix}://grapher/{form.namespace}/{form.version}/{form.short_name}": [
                        f"data{private_suffix}://garden/{form.namespace}/{form.garden_version}/{form.short_name}"
                    ]
                },
                dag_path=dag_path,
            )
        else:
            dag_content = ""

        # Create necessary files
        DATASET_DIR = utils.generate_step_to_channel(
            cookiecutter_path=utils.COOKIE_GRAPHER, data=dict(**form.dict(), channel="grapher")
        )

        step_path = DATASET_DIR / (form.short_name + ".py")

        # Display next steps
        with st.expander("⏭️ **Next steps**", expanded=True):
            st.markdown(
                f"""
        1. Test your step against your local database. If you have your grapher DB configured locally, your `.env` file should look similar to this:

            ```bash
            GRAPHER_USER_ID=your_user_id
            DB_USER=root
            DB_NAME=owid
            DB_HOST=127.0.0.1
            DB_PORT=3306
            DB_PASS=
            ```

            Get `your_user_id` by running SQL query `select id from users where email = 'your_name@ourworldindata.org'`.

            Then run the grapher step:
            ```
            poetry run etl grapher/{form.namespace}/{form.version}/{form.short_name} --grapher {"--private" if form.is_private else ""}
            ```
            Your new dataset should then appear in your local set-up: http://localhost:3030/admin/datasets. Follow the instructions [here](https://github.com/owid/owid-grapher/blob/master/docs/docker-compose-mysql.md) to create your local Grapher development set-up.

        2. When you feel confident, use `.env.staging` for staging which looks something like this:

            ```
            GRAPHER_USER_ID=your_user_id
            DB_USER=staging_grapher
            DB_NAME=staging_grapher
            DB_PASS=***
            DB_PORT=3306
            DB_HOST=owid-staging
            ```

            Make sure you have [Tailscale](https://tailscale.com/download) installed and running.

            After you run

            ```
            ENV=.env.staging poetry run etl grapher/{form.namespace}/{form.version}/{form.short_name} --grapher {"--private" if form.is_private else ""}
            ```

            you should see it [in staging admin](https://staging.owid.cloud/admin/datasets).

        3. To get your dataset into production DB, you can merge your PR into master and it'll be deployed automatically. Alternatively, you can push it to production manually by using `.env.prod` file

            ```
            GRAPHER_USER_ID=your_user_id
            DB_USER=live_grapher
            DB_NAME=etl_grapher
            DB_PASS=***
            DB_PORT=3306
            DB_HOST=owid-live-db
            ```

            and running

            ```
            ENV=.env.prod poetry run etl grapher/{form.namespace}/{form.version}/{form.short_name} --grapher {"--private" if form.is_private else ""}
            ```

        4. Check your dataset in [admin](https://owid.cloud/admin/datasets).

        5. If you are an internal OWID member and, because of this dataset update, you want to update charts in our Grapher DB, continue with charts
        """
            )
            utils.st_page_link("charts", use_container_width=True, border=True)

        # Preview generated files
        st.subheader("Generated files")
        utils.preview_file(step_path, "python")
        utils.preview_dag_additions(dag_content, dag_path)

        # User message
        st.toast("Templates generated. Read the next steps.", icon="✅")

        # Update config
        utils.update_wizard_config(form=form)
    else:
        st.write(form.errors)
        st.error("Form not submitted! Check errors!")
