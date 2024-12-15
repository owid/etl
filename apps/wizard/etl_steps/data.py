"""Garden phase."""

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import streamlit as st
from rapidfuzz import fuzz
from typing_extensions import Self

from apps.utils.files import add_to_dag, generate_step_to_channel
from apps.wizard import utils
from apps.wizard.app_pages.harmonizer.utils import render
from apps.wizard.etl_steps.utils import TAGS_DEFAULT, remove_playground_notebook
from apps.wizard.utils.components import st_horizontal
from etl.config import DB_HOST, DB_NAME
from etl.paths import DAG_DIR

#########################################################
# CONSTANTS #############################################
#########################################################
st.set_page_config(
    page_title="Wizard: Data Step",
    page_icon="🪄",
)
st.session_state.submit_form = st.session_state.get("submit_form", False)
st.session_state["data.steps_to_create"] = st.session_state.get("data.steps_to_create", [])
st.session_state["data_steps_to_create"] = st.session_state.get("data_steps_to_create")
st.session_state.update_steps_selection = st.session_state.get("update_steps_selection", False)

# Available namespaces
OPTIONS_NAMESPACES = utils.get_namespaces("all")


# Get current directory
CURRENT_DIR = Path(__file__).parent
# FIELDS FROM OTHER STEPS
st.session_state["step_name"] = "data"
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
# USING_TAGS_DEFAULT = False
# try:
#     with get_session() as session:
#         tag_list = gm.Tag.load_tags(session)
#         tag_list = ["Uncategorized"] + sorted([tag.name for tag in tag_list])
# except OperationalError:
#     USING_TAGS_DEFAULT = True
#     tag_list = TAGS_DEFAULT
# TODO: Remove following lines, uncomment lines above
USING_TAGS_DEFAULT = True
tag_list = TAGS_DEFAULT
# Step names
STEP_NAME_PRESENT = {
    "meadow": ":material/nature: Meadow",
    "garden": ":material/deceased: Garden",
    "grapher": ":material/database: Grapher",
}


#########################################################
# FUNCTIONS & CLASSES ###################################
#########################################################
class DataForm(utils.StepForm):
    """express step form."""

    step_name: str = "data"

    # List of steps
    steps_to_create: List[str]
    # Common
    namespace: str
    namespace_custom: Optional[str] = None  # Custom
    short_name: str
    version: str
    add_to_dag: bool
    dag_file: str
    is_private: bool
    # Only in Garden
    snapshot_version: Optional[str] = None
    file_extension: Optional[str] = None
    notebook: Optional[bool] = None
    # Only in Garden
    update_period_days: Optional[int] = None
    topic_tags: Optional[List[str]] = None
    update_period_date: Optional[date] = None  # Custom

    def __init__(self: Self, **data: Any) -> None:  # type: ignore[reportInvalidTypeVarUse]
        """Construct class."""
        data["add_to_dag"] = data["dag_file"] != utils.ADD_DAG_OPTIONS[0]

        # Handle custom namespace
        if ("namespace_custom" in data) and data["namespace_custom"] is not None:
            data["namespace"] = str(data["namespace_custom"])

        # Handle update_period_days. Obtain from date.
        if "update_period_date" in data:
            assert isinstance(data["update_period_date"], date)
            update_period_days = (data["update_period_date"] - date.today()).days

            data["update_period_days"] = update_period_days

        # Extra options
        data["is_private"] = True if "private" in data["extra_options"] else False
        data["notebook"] = True if "notebook" in data["extra_options"] else False

        # st.write(data)
        super().__init__(**data)  # type: ignore

    def validate(self: Self) -> None:
        """Check that fields in form are valid.

        - Add error message for each field (to be displayed in the form).
        - Return True if all fields are valid, False otherwise.
        """
        # Default common checks
        fields_required = ["namespace", "short_name", "version"]
        fields_snake = ["namespace", "short_name"]
        fields_version = ["version"]

        # Extra checks for particular steps
        if "meadow" in self.steps_to_create:
            fields_required += ["snapshot_version", "file_extension"]
            fields_version += ["snapshot_version"]
        if "garden" in self.steps_to_create:
            fields_required += ["topic_tags"]

        self.check_required(fields_required)
        self.check_snake(fields_snake)
        self.check_is_version(fields_version)

        # Check tags
        if "garden" in self.steps_to_create:
            assert isinstance(self.topic_tags, list), "topic_tags must be a list! Should have been ensured actually!"
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
        # Remove playground notebook if not needed
        if channel != "garden" or not self.notebook:
            remove_playground_notebook(DATASET_DIR)

        # Add to generated files
        generated_files = [
            {
                "path": DATASET_DIR / (self.short_name + ".py"),
                "language": "python",
                "channel": channel,
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
                dag=self.dag(),
                dag_path=self.dag_path,
            )
        else:
            dag_content = ""

        return dag_content

    def dag(self) -> Dict[str, Any]:
        dag = {}
        if "meadow" in self.steps_to_create:
            dag[self.meadow_step_uri] = [self.snapshot_step_uri]
        if "garden" in self.steps_to_create:
            dag[self.garden_step_uri] = [self.meadow_step_uri]
        if "grapher" in self.steps_to_create:
            dag[self.grapher_step_uri] = [self.garden_step_uri]
        return dag


def submit_form() -> None:
    """Submit form."""
    # Create form
    form = DataForm.from_state()
    # Update states with values from form
    APP_STATE.update_from_form(form)

    # Submit
    utils.set_states({"submit_form": True})


def edit_field() -> None:
    """Submit form."""
    utils.set_states({"submit_form": False})


def render_step_selection():
    """Render step selection."""
    # st.write(st.session_state)
    with st_horizontal("center"):
        # Multi-select (data steps)
        with st.container():
            if (st.session_state.update_steps_selection) and (st.session_state["data_steps_to_create"] is not None):
                st.session_state["data.steps_to_create"] = ["meadow", "garden", "grapher"]
                st.session_state.update_steps_selection = False

            st.segmented_control(
                label="Select the steps you want to create",
                options=["meadow", "garden", "grapher"],
                format_func=lambda option: STEP_NAME_PRESENT[option],
                selection_mode="multi",
                help="You can select multiple steps to create.",
                key="data.steps_to_create",
                on_change=edit_field,
            )
        # Express mode
        with st.container():
            if (st.session_state["data_steps_to_create"] is not None) and (
                len(st.session_state["data.steps_to_create"]) != 3
            ):
                st.session_state["data_steps_to_create"] = None

            # Set to false if: data_steps_to_create is "express" & len(st.session_state["data.steps_to_create"]) is not 3
            st.segmented_control(
                "Express?",
                ["express"],
                format_func=lambda _: ":material/bolt: Use express mode",
                label_visibility="hidden",
                help="Express mode will create all steps at once.",
                # default=default,
                key="data_steps_to_create",
                on_change=lambda: set_states({"update_steps_selection": True, "submit_form": False}),
            )

        if len(st.session_state["data.steps_to_create"]) > 0:
            return True
        return False


def render_form():
    """Render form."""
    with st.expander("**Form**", expanded=True):
        if (default_version := APP_STATE.default_value("version", previous_step="snapshot")) == "":
            default_version = APP_STATE.default_value("snapshot_version", previous_step="snapshot")

        # Namespace
        custom_label = "Custom namespace..."
        APP_STATE.st_selectbox_responsive(
            st_widget=st.selectbox,
            custom_label=custom_label,
            key="namespace",
            label="Namespace",
            help="Institution or topic name",
            options=OPTIONS_NAMESPACES,
            default_last=dummy_values["namespace"] if APP_STATE.args.dummy_data else OPTIONS_NAMESPACES[0],
            on_change=edit_field,
        )
        if APP_STATE.vars.get("namespace") == custom_label:
            namespace_key = "namespace_custom"
        else:
            namespace_key = "namespace"

        # Short name (meadow, garden, grapher)
        APP_STATE.st_widget(
            st_widget=st.text_input,
            key="short_name",
            label="short name",
            help="Dataset short name using [snake case](https://en.wikipedia.org/wiki/Snake_case). Example: natural_disasters",
            placeholder="Example: 'cherry_blossom'",
            value=dummy_values["short_name"] if APP_STATE.args.dummy_data else None,
            on_change=edit_field,
        )

        # Version (meadow, garden, grapher)
        APP_STATE.st_widget(
            st_widget=st.text_input,
            label="version",
            help="Version of the dataset (by default, the current date, or exceptionally the publication date).",
            key="version",
            default_last=default_version,
            value=dummy_values["version"] if APP_STATE.args.dummy_data else default_version,
            on_change=edit_field,
        )

        # Add to DAG
        sorted_dag = sorted(
            utils.dag_files,
            key=lambda file_name: fuzz.ratio(file_name.replace(".yml", ""), APP_STATE.vars[namespace_key]),
            reverse=True,
        )
        sorted_dag = [
            utils.dag_not_add_option,
            *sorted_dag,
        ]
        if sorted_dag[1].replace(".yml", "") == APP_STATE.vars[namespace_key]:
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

        if "garden" in st.session_state["data.steps_to_create"]:
            # Indicator tags
            label = "Indicators tag"
            if USING_TAGS_DEFAULT:
                label += f"\n\n:red[Using a 2024 March snapshot of the tags. Couldn't connect to database `{DB_NAME}` in host `{DB_HOST}`.]"

            namespace = APP_STATE.vars[namespace_key].replace("_", " ")
            default_last = None
            for tag in tag_list:
                if namespace.lower() == tag.lower():
                    default_last = tag
                    break
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
                default=dummy_values["topic_tags"] if APP_STATE.args.dummy_data else default_last,
                on_change=edit_field,
            )

            # Update frequency
            today = datetime.today()
            APP_STATE.st_widget(
                st_widget=st.date_input,
                label="When is the next update expected?",
                help="Expected date of the next update of this dataset by OWID (typically in a year).",
                key="update_period_date",
                min_value=today + timedelta(days=1),
                default_last=today.replace(year=today.year + 1),
                on_change=edit_field,
            )

        if "meadow" in st.session_state["data.steps_to_create"]:
            st.markdown("#### Snapshot")
            # Snapshot version
            APP_STATE.st_widget(
                st.text_input,
                label="Snapshot version",
                help="Version of the snapshot dataset (by default, the current date, or exceptionally the publication date).",
                # placeholder=f"Example: {DATE_TODAY}",
                key="snapshot_version",
                value=dummy_values["snapshot_version"] if APP_STATE.args.dummy_data else None,
                on_change=edit_field,
            )
            # File extension
            APP_STATE.st_widget(
                st.text_input,
                label="File extension",
                help="File extension (without the '.') of the file to be downloaded.",
                placeholder="Example: 'csv', 'xls', 'zip'",
                key="file_extension",
                value=dummy_values["file_extension"] if APP_STATE.args.dummy_data else None,
                on_change=edit_field,
            )

        # Others
        st.markdown("#### Other options")
        name_mapping = {
            "private": ":material/lock:  Make dataset private",
            "notebook": ":material/skateboarding:  Create playground notebook",
            # "grapher": ":material/database: Grapher",
        }

        st.pills(
            label="Select the steps you want to create",
            help="Extra options.",
            options=["private", "notebook"],
            format_func=lambda option: name_mapping[option],
            selection_mode="multi",
            label_visibility="collapsed",
            key="data.extra_options",
            on_change=edit_field,
        )

        # Submit
        st.button(
            label="Submit",
            type="primary",
            use_container_width=True,
            on_click=submit_form,
        )


@st.dialog("Harmonize country names", width="large")
def render_harm():
    render()


def render_instructions(form):
    for channel in ["meadow", "garden", "grapher"]:
        if channel in form.steps_to_create:
            with st.container(border=True):
                st.markdown(f"##### **{STEP_NAME_PRESENT.get(channel, channel)}**")
                render_instructions_step(channel, form)


def render_instructions_step(channel, form=None):
    if channel == "meadow":
        render_instructions_meadow(form)
    elif channel == "garden":
        render_instructions_garden(form)
    elif channel == "grapher":
        render_instructions_grapher(form)


def render_instructions_meadow(form=None):
    ## Run step
    st.markdown("**1) Run Meadow step**")
    if form is None:
        st.code(
            "uv run etl run data://meadow/namespace/version/short_name",
            language="shellSession",
            wrap_lines=True,
            line_numbers=True,
        )
        st.markdown("Use `--private` if the dataset is private.")
    else:
        st.code(
            f"uv run etl run {form.meadow_step_uri} {'--private' if form.is_private else ''}",
            language="shellSession",
            wrap_lines=True,
            line_numbers=True,
        )

    st.markdown("**2) Harmonize country names**")
    st.button("Harmonize", on_click=render_harm)
    st.markdown("You can also run it in your terminal:")
    if form is None:
        st.code(
            "uv run etl harmonize data/meadow/version/short_name/table_name.feather country etl/steps/data/garden/version/short_name.countries.json",
            "shellSession",
            wrap_lines=True,
            line_numbers=True,
        )
    else:
        st.code(
            f"uv run etl harmonize data/meadow/{form.base_step_name}/{form.short_name}.feather country etl/steps/data/garden/{form.base_step_name}.countries.json",
            "shellSession",
            wrap_lines=True,
            line_numbers=True,
        )


def render_instructions_garden(form=None):
    ## 1/ Run etl step
    st.markdown("**1) Run Garden step**")
    st.markdown("After editing the code of your Garden step, run the following command:")
    if form is None:
        st.code(
            "uv run etl run data://garden/namespace/version/short_name",
            "shellSession",
            wrap_lines=True,
            line_numbers=True,
        )
        st.markdown("Use `--private` if the dataset is private.")
    else:
        st.code(
            f"uv run etl run {form.garden_step_uri} {'--private' if form.is_private else ''}",
            "shellSession",
            wrap_lines=True,
            line_numbers=True,
        )


def render_instructions_grapher(form=None):
    st.markdown("**1) Run Grapher step**")
    if form is None:
        st.code(
            "uv run etl run data://meadow/namespace/version/short_name",
            language="shellSession",
            wrap_lines=True,
            line_numbers=True,
        )
        st.markdown("Use `--private` if the dataset is private.")
    else:
        st.code(
            f"uv run etl run {form.grapher_step_uri} {'--private' if form.is_private else ''}",
            language="shellSession",
            wrap_lines=True,
            line_numbers=True,
        )
    st.markdown("**2) Pull request**")
    st.markdown("Create a pull request in [ETL](https://github.com/owid/etl), get it reviewed and merged.")


#########################################################
# MAIN ##################################################
#########################################################
# TITLE
st.title(":material/bolt: Data **:gray[Create steps]**")

# SELECT MODE
step_selected = render_step_selection()

# FORM
if step_selected:
    # form_widget = st.empty()
    render_form()
else:
    st.warning("Select at least one step to create.")

#########################################################
# SUBMISSION ############################################
#########################################################
if st.session_state.submit_form:
    # Create form
    form = DataForm.from_state()

    # st.write(form.model_dump())
    if not form.errors:
        # Remove form from UI
        # form_widget.empty()

        # Create files for all steps
        generated_files = {}
        for channel in form.steps_to_create:
            generated_files_ = form.create_files(channel)
            generated_files[channel] = generated_files_

        # Add lines to DAG
        dag_content = form.add_steps_to_dag()

        ########################
        # PREVIEW & NEXT STEPS #
        ########################
        utils.preview_dag_additions(dag_content, form.dag_path, prefix="**DAG**", expanded=True)

        # st.write(generated_files)
        tab_instructions, tab_files = st.tabs(["Instructions", "Files"])

        with tab_files:
            for channel in ["meadow", "garden", "grapher"]:
                if channel in generated_files:
                    # st.markdown(f"**{channel.title()}**")
                    for f in generated_files[channel]:
                        utils.preview_file(f["path"], f"**{STEP_NAME_PRESENT.get(channel)}**", f["language"])

        with tab_instructions:
            render_instructions(form)

        # Prompt user
        st.toast("Templates generated. Read the next steps.", icon="✅")

        # Update config
        utils.update_wizard_defaults_from_form(form=form)
    else:
        st.write(form.errors)
        st.error("Form not submitted! Check errors!")


# st.divider()
# st.subheader("Legacy")
# st_page_link("meadow")
# st_page_link("garden")
# st_page_link("grapher")
