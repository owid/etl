"""Garden phase."""
from pathlib import Path
from typing import Any, Dict, List, cast

import streamlit as st
from sqlalchemy.exc import OperationalError
from typing_extensions import Self

import etl.grapher_model as gm
from apps.utils.files import add_to_dag, generate_step_to_channel
from apps.wizard import utils
from apps.wizard.etl_steps.utils import TAGS_DEFAULT, remove_playground_notebook
from etl.config import DB_HOST, DB_NAME
from etl.db import get_session
from etl.paths import DAG_DIR

#########################################################
# CONSTANTS #############################################
#########################################################
st.set_page_config(
    page_title="Wizard: Express",
    page_icon="🪄",
)
# Available namespaces
OPTIONS_NAMESPACES = utils.get_namespaces("all")


# Get current directory
CURRENT_DIR = Path(__file__).parent
# FIELDS FROM OTHER STEPS
st.session_state["step_name"] = "express"
APP_STATE = utils.AppState()
APP_STATE._previous_step = "snapshot"
# Config style
utils.config_style_html()
# DUMMY defaults
dummy_values = {
    "namespace": "dummy",
    "version": utils.DATE_TODAY,
    "short_name": "dummy",
    "snapshot_version": "2020-01-01",
    "topic_tags": ["Uncategorized"],
}

# Get list of available tags from DB (only those used as topic pages)
# If can't connect to DB, use TAGS_DEFAULT instead
USING_TAGS_DEFAULT = False
try:
    with get_session() as session:
        tag_list = gm.Tag.load_tags(session)
        tag_list = ["Uncategorized"] + sorted([tag.name for tag in tag_list])
except OperationalError:
    USING_TAGS_DEFAULT = True
    tag_list = TAGS_DEFAULT


#########################################################
# FUNCTIONS & CLASSES ###################################
#########################################################
@st.cache_data
def load_instructions() -> str:
    """Load snapshot step instruction text."""
    with open(file=utils.MD_EXPRESS, mode="r") as f:
        return f.read()


class ExpressForm(utils.StepForm):
    """express step form."""

    step_name: str = "express"

    namespace: str
    short_name: str
    version: str
    update_period_days: int
    topic_tags: List[str]
    add_to_dag: bool
    dag_file: str
    # Others
    is_private: bool
    # Snapshot
    snapshot_version: str
    file_extension: str

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
        fields_required = ["namespace", "short_name", "version", "topic_tags", "snapshot_version", "file_extension"]
        fields_snake = ["namespace", "short_name"]
        fields_version = ["version", "snapshot_version"]

        self.check_required(fields_required)
        self.check_snake(fields_snake)
        self.check_is_version(fields_version)

        # Check tags
        if (len(self.topic_tags) > 1) and ("Uncategorized" in self.topic_tags):
            self.errors["topic_tags"] = "If you choose multiple tags, you cannot choose `Uncategorized`."

    @property
    def base_step_name(self) -> str:
        """namespace/version/short_name"""
        return f"{form.namespace}/{form.version}/{form.short_name}"

    @property
    def snapshot_step_uri(self) -> str:
        """Get snapshot step URI."""
        return f"snapshot{self.private_suffix}://{self.namespace}/{self.snapshot_version}/{self.short_name}.{self.file_extension}"

    @property
    def meadow_step_uri(self) -> str:
        """Get garden step name."""
        return f"data{self.private_suffix}://meadow/{self.base_step_name}"

    @property
    def garden_step_uri(self) -> str:
        """Get garden step name."""
        return f"data{self.private_suffix}://garden/{self.base_step_name}"

    @property
    def grapher_step_uri(self) -> str:
        """Get garden step name."""
        return f"data{self.private_suffix}://grapher/{self.base_step_name}"

    @property
    def dag_path(self) -> Path:
        """Get DAG path."""
        return DAG_DIR / self.dag_file

    @property
    def private_suffix(self) -> str:
        return "-private" if self.is_private else ""

    @property
    def meadow_dict(self):
        """Get meadow dictionary."""
        return {
            "namespace": self.namespace,
            "short_name": self.short_name,
            "version": self.version,
            "add_to_dag": self.add_to_dag,
            "dag_file": self.dag_file,
            "is_private": self.is_private,
            "snapshot_version": self.snapshot_version,
            "file_extension": self.file_extension,
            "channel": "meadow",
        }

    @property
    def garden_dict(self):
        """Get meadow dictionary."""
        ## HOTFIX 1: filter topic_tags if empty
        if self.topic_tags is None or self.topic_tags == []:
            topic_tags = ""
        ## HOTFIX 2: For some reason, when using cookiecutter only the first element in the list is taken?
        ## Hence we need to convert the list to an actual string
        else:
            topic_tags = "- " + "\n- ".join(self.topic_tags)
        return {
            "namespace": self.namespace,
            "short_name": self.short_name,
            "version": self.version,
            "meadow_version": self.version,
            "add_to_dag": self.add_to_dag,
            "dag_file": self.dag_file,
            "is_private": self.is_private,
            "update_period_days": self.update_period_days,
            "topic_tags": topic_tags,
            "channel": "garden",
        }

    @property
    def grapher_dict(self):
        """Get meadow dictionary."""
        return {
            "namespace": self.namespace,
            "short_name": self.short_name,
            "version": self.version,
            "garden_version": self.version,
            "add_to_dag": self.add_to_dag,
            "dag_file": self.dag_file,
            "is_private": self.is_private,
            "channel": "grapher",
        }

    def to_dict(self, channel: str):
        match channel:
            case "meadow":
                return self.meadow_dict
            case "garden":
                return self.garden_dict
            case "grapher":
                return self.grapher_dict
            case _:
                raise ValueError(f"Channel `{channel}` not recognized.")

    def create_files(self, channel: str) -> List[Dict[str, Any]]:
        # Generate files
        DATASET_DIR = generate_step_to_channel(
            cookiecutter_path=utils.COOKIE_STEPS[channel], data=self.to_dict(channel)
        )
        # Remove playground (by default it is created, we no longer want it)
        remove_playground_notebook(DATASET_DIR)
        # Add to generated files
        generated_files = [
            {
                "path": DATASET_DIR / (self.short_name + ".py"),
                "language": "python",
                "channel": "meadow",
            }
        ]
        if channel == "garden":
            generated_files.append(
                {
                    "path": DATASET_DIR / (self.short_name + ".meta.yml"),
                    "language": "yaml",
                    "channel": "garden",
                }
            )
        return generated_files

    def add_steps_to_dag(self) -> str:
        if form.add_to_dag:
            dag_content = add_to_dag(
                dag={
                    self.meadow_step_uri: [
                        self.snapshot_step_uri,
                    ],
                    self.garden_step_uri: [
                        self.meadow_step_uri,
                    ],
                    self.grapher_step_uri: [
                        self.garden_step_uri,
                    ],
                },
                dag_path=self.dag_path,
            )
        else:
            dag_content = ""

        return dag_content


def update_state() -> None:
    """Submit form."""
    # Create form
    form = ExpressForm.from_state()
    # Update states with values from form
    APP_STATE.update_from_form(form)


def export_metadata() -> None:
    dataset_path = st.session_state["garden.dataset_path"]
    try:
        output_path = utils.metadata_export_basic(dataset_path=dataset_path)
    except Exception as e:
        st.exception(e)
        st.stop()
    else:
        st.success(f"Metadata exported to `{output_path}`.")


#########################################################
# MAIN ##################################################
#########################################################
# TITLE
st.title("Create step 🐆 **:gray[Express]**")

st.info("Use this step to create Meadow, Garden and Grapher step for a _single dataset_!")

# SIDEBAR
with st.sidebar:
    # utils.warning_metadata_unstable()
    with st.expander("**Instructions**", expanded=True):
        text = load_instructions()  # TODO: which instructions?
        st.markdown(text)

# FORM
form_widget = st.empty()
with form_widget.form("express"):
    if (default_version := APP_STATE.default_value("version", previous_step="snapshot")) == "":
        default_version = APP_STATE.default_value("snapshot_version", previous_step="snapshot")

    # Namespace
    namespace_field = [st.empty(), st.container()]

    # Short name (meadow, garden, grapher)
    APP_STATE.st_widget(
        st_widget=st.text_input,
        label="short name",
        help="Dataset short name using [snake case](https://en.wikipedia.org/wiki/Snake_case). Example: natural_disasters",
        placeholder="Example: 'cherry_blossom'",
        key="short_name",
        value=dummy_values["short_name"] if APP_STATE.args.dummy_data else None,
    )

    # Version (meadow, garden, grapher)
    APP_STATE.st_widget(
        st_widget=st.text_input,
        label="version",
        help="Version of the dataset (by default, the current date, or exceptionally the publication date).",
        key="version",
        default_last=default_version,
        value=dummy_values["version"] if APP_STATE.args.dummy_data else default_version,
    )

    # Indicator tags
    label = "Indicators tag"
    if USING_TAGS_DEFAULT:
        label += f"\n\n:red[Using a 2024 March snapshot of the tags. Couldn't connect to database `{DB_NAME}` in host `{DB_HOST}`.]"
    APP_STATE.st_widget(
        st_widget=st.multiselect,
        label=label,
        help=(
            """
            This tag will be propagated to all dataset's indicators (it will not be assigned to the dataset).

            If you want to use a different tag for a specific indicator you can do it by editing its metadata field `variable.presentation.topic_tags`.

            Exceptionally, and if unsure what to choose, choose tag `Uncategorized`.
            """
        ),
        key="topic_tags",
        options=tag_list,
        placeholder="Choose a tag (or multiple)",
        default=dummy_values["topic_tags"] if APP_STATE.args.dummy_data else None,
    )

    # Add to DAG
    APP_STATE.st_widget(
        st.selectbox,
        label="Add to DAG",
        options=utils.ADD_DAG_OPTIONS,
        key="dag_file",
        help="Add ETL step to a DAG file. This will allow it to be tracked and executed by the `etl` command.",
    )

    # Update frequency
    APP_STATE.st_widget(
        st_widget=st.number_input,
        label="Dataset update frequency (days)",
        help="Expected number of days between consecutive updates of this dataset by OWID, typically `30`, `90` or `365`.",
        key="update_period_days",
        step=1,
        min_value=0,
        default_last=365,
    )

    APP_STATE.st_widget(
        st.toggle,
        label="Make dataset private",
        key="is_private",
        default_last=False,
    )

    st.markdown("#### Snapshot")
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

    # Submit
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
    form = cast(ExpressForm, ExpressForm.from_state())

    if not form.errors:
        # Remove form from UI
        form_widget.empty()

        # Create files for all steps
        generated_files = []
        for channel in ["meadow", "garden", "grapher"]:
            generated_files_ = form.create_files(channel)
            generated_files.extend(generated_files_)

        # Add lines to DAG
        dag_content = form.add_steps_to_dag()

        #######################
        # PREVIEW #############
        #######################
        st.subheader("Generated files")
        utils.preview_dag_additions(dag_content, form.dag_path, expanded=True)

        tab_meadow, tab_garden, tab_grapher = st.tabs(["Meadow", "Garden", "Grapher"])
        for f in generated_files:
            if f["channel"] == "meadow":
                with tab_meadow:
                    utils.preview_file(f["path"], f["language"])
            elif f["channel"] == "garden":
                with tab_garden:
                    utils.preview_file(f["path"], f["language"])
            elif f["channel"] == "grapher":
                with tab_grapher:
                    utils.preview_file(f["path"], f["language"])

        #######################
        # NEXT STEPS ##########
        #######################
        with tab_meadow:
            st.markdown("#### ⏭️ Next")
            ## Run step
            st.markdown("##### Run ETL meadow step")
            st.code(
                f"poetry run etl run {form.meadow_step_uri} {'--private' if form.is_private else ''}",
                language="shellSession",
            )

        with tab_garden:
            st.markdown("#### ⏭️ Next")
            ## 1/ Harmonize country names
            st.markdown("##### Harmonize country names")
            st.markdown("Run it in your terminal:")
            st.code(
                f"poetry run etl harmonize data/meadow/{form.base_step_name}/{form.short_name}.feather country etl/steps/data/garden/{form.base_step_name}.countries.json",
                "shellSession",
            )
            st.markdown("Or run it on Wizard")
            utils.st_page_link(
                "harmonizer",
                use_container_width=True,
                help="You will leave this page, and the guideline text will be hidden.",
            )

            # 2/ Run etl step
            st.markdown("##### Run ETL step")
            st.markdown("After editing the code of your Garden step, run the following command:")
            st.code(
                f"poetry run etl run {form.garden_step_uri} {'--private' if form.is_private else ''}",
                "shellSession",
            )

        with tab_grapher:
            st.markdown("#### ⏭️ Next")
            # 1/ PR
            st.markdown("##### Pull request")
            st.markdown("Create a pull request in [ETL](https://github.com/owid/etl), get it reviewed and merged.")

        # Prompt user
        st.toast("Templates generated. Read the next steps.", icon="✅")

        # Update config
        utils.update_wizard_defaults_from_form(form=form)
    else:
        st.write(form.errors)
        st.error("Form not submitted! Check errors!")
