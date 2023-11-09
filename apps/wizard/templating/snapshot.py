"""Snapshot phase."""
import subprocess
import traceback
from datetime import datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Optional, cast

import streamlit as st
from st_pages import add_indentation
from typing_extensions import Self

from apps.wizard import utils
from etl.docs import examples_to_markdown, faqs_to_markdown, guidelines_to_markdown
from etl.helpers import read_json_schema
from etl.paths import BASE_DIR, SCHEMAS_DIR, SNAPSHOTS_DIR

#########################################################
# CONSTANTS #############################################
#########################################################
st.set_page_config(page_title="Wizard (snapshot)", page_icon="🪄")
add_indentation()

# Page config
# st.set_page_config(page_title="Wizard (snapshot)", page_icon="🪄")
# Read schema
SNAPSHOT_SCHEMA = read_json_schema(path=SCHEMAS_DIR / "snapshot-schema.json")
# Get properties for origin in schema
schema_origin = SNAPSHOT_SCHEMA["properties"]["meta"]["properties"]["origin"]["properties"]
# Lists with fields of special types. By default, fields are text inputs.
FIELD_TYPES_TEXTAREA = [
    "origin.description_snapshot",
    "origin.description",
    "origin.citation_full",
]
FIELD_TYPES_SELECT = ["origin.license.name"]
# Get current directory
CURRENT_DIR = Path(__file__).parent
# Accepted schema categories
ACCEPTED_CATEGORIES = ["dataset", "citation", "files", "license"]
# FIELDS FROM OTHER STEPS
st.session_state["step_name"] = "snapshot"
APP_STATE = utils.AppState()
# DUMMY defaults
dummy_values = {
    "namespace": "dummy",
    "snapshot_version": utils.DATE_TODAY,
    "short_name": "dummy",
    "file_extension": "csv",
    # Programatically generated based on schemas/
    "origin.title": "Dummy data product",
    "origin.date_published": utils.DATE_TODAY,
    "origin.producer": "Non-dummy producer",
    "origin.citation_full": "Dummy description for a dummy snapshot.",
    "origin.url_main": "https://dummy.dummy",
    "origin.date_accessed": utils.DATE_TODAY,
}
# Other state vars
if "show_form" not in st.session_state:
    st.session_state["show_form"] = True
if "run_step" not in st.session_state:
    st.session_state["run_step"] = False
if "snapshot_file" not in st.session_state:
    st.session_state["snapshot_file"] = None


#########################################################
# FUNCTIONS & CLASSES ###################################
#########################################################
class SnapshotForm(utils.StepForm):
    """Interface for snapshot form."""

    step_name: str = "snapshot"

    # config
    namespace: str
    snapshot_version: str
    short_name: str
    file_extension: str
    is_private: bool
    dataset_manual_import: bool

    # origin
    title: str
    description: str
    title_snapshot: str
    description_snapshot: str
    origin_version: str
    date_published: str
    producer: str
    citation_full: str
    attribution: Optional[str]
    attribution_short: str
    url_main: str
    url_download: str
    date_accessed: str

    # license
    license_url: str
    license_name: str

    def __init__(self: Self, **data: str | int) -> None:
        """Construct form."""
        # Change name for certain fields (and remove old ones)
        data["license_url"] = data["origin.license.url"]
        data["origin_version"] = data["origin.version_producer"]
        data["dataset_manual_import"] = data["local_import"]

        # Handle custom license
        if "origin.license.name_custom" in data:
            data["license_name"] = data["origin.license.name_custom"]
        else:
            data["license_name"] = data["origin.license.name"]

        # Remove unused fields
        data = {k: v for k, v in data.items() if k not in ["origin.license.url", "origin.license.name"]}
        # Remove 'origin.' prefix from keys
        data = {k.replace("origin.", ""): v for k, v in data.items()}

        # Init object (includes schema validation)
        super().__init__(**data)

        # Handle custom attribution
        if not self.errors:
            if "attribution_custom" in data:
                self.attribution = str(data["attribution_custom"])
            else:
                self.attribution = self.parse_attribution(data)

    def parse_attribution(self: Self, data: Dict[str, str | int]) -> str | None:
        """Parse the field attribution.

        By default, the field attribution contains the format of the attribution, not the actual attribution. This function
        renders the actual attribution.
        """
        attribution_template = cast(str, data["attribution"])
        if attribution_template == "{producer} ({year})":
            return None
        data_extra = {
            "year": dt.strptime(str(data["date_published"]), "%Y-%m-%d").year,
            # "version_producer": data["origin_version"],
        }
        attribution = attribution_template.format(**data, **data_extra).replace("  ", " ")
        return attribution

    def validate(self: "SnapshotForm") -> None:
        """Check that fields in form are valid.

        - Add error message for each field (to be displayed in the form).
        - Return True if all fields are valid, False otherwise.
        """
        # 1) Validate using schema
        # This only applies to SnapshotMeta fields
        self.validate_schema(SNAPSHOT_SCHEMA, ["meta"])

        # 2) Check other fields (non meta)
        fields_required = ["namespace", "snapshot_version", "short_name", "file_extension"]
        fields_snake = ["namespace", "short_name"]
        fields_version = ["snapshot_version"]

        self.check_required(fields_required)
        self.check_snake(fields_snake)
        self.check_is_version(fields_version)

        # License
        if self.license_name == "":
            self.errors["origin.license.name_custom"] = "Please introduce the name of the custom license!"

        # Attribution
        if self.attribution == "":
            self.errors["origin.attribution_custom"] = "Please introduce the name of the custom attribute!"

    @property
    def metadata(self: Self) -> Dict[str, Any]:
        """Define metadata for easy YAML-export."""
        license_field = {
            "name": self.license_name,
            "url": self.license_url,
        }
        meta = {
            "meta": {
                "origin": {
                    "title": self.title,
                    "description": self.description.replace("\n", "\n      "),
                    "title_snapshot": self.title_snapshot,
                    "description_snapshot": self.description_snapshot.replace("\n", "\n      "),
                    "producer": self.producer,
                    "citation_full": self.citation_full,
                    "attribution": self.attribution,
                    "attribution_short": self.attribution_short,
                    "version_producer": self.origin_version,
                    "url_main": self.url_main,
                    "url_download": self.url_download,
                    "date_published": self.date_published,
                    "date_accessed": self.date_accessed,
                    "license": license_field,
                },
                "is_public": not self.is_private,
            }
        }
        meta = cast(Dict[str, Any], utils.clean_empty_dict(meta))
        return meta


def _color_req_level(req_level: str) -> str:
    if req_level == "required":
        return f"**:red[{req_level}]**"
    elif "required" in req_level:
        color = "red"
    elif "recommended" in req_level:
        color = "orange"
    elif "optional" in req_level:
        return req_level
    else:
        raise ValueError(f"Unknown requirement level: {req_level}")
    req_level = f":{color}[{req_level}]"
    return req_level


def create_display_name_init_section(name: str) -> str:
    """Create display name for a field."""
    # Get requirement level colored
    req_level = _color_req_level("required")
    # Create display name
    display_name = f"{name} ┃ {req_level}"
    return display_name


def create_display_name_snap_section(
    props: Dict[str, Any], name: str, property_name: str, title: Optional[str] = None
) -> str:
    """Create display name for a field."""
    # Get requirement level colored
    req_level = _color_req_level(props["requirement_level"])
    # Create display name
    if not title:
        title = props["title"]
    display_name = f"`{property_name}.{name}` ┃ {title} ┃ {req_level}"
    return display_name


@st.cache_data
def load_instructions() -> str:
    """Load snapshot step instruction text."""
    with open(file=utils.MD_SNAPSHOT, mode="r") as f:
        return f.read()


def create_description(field: Dict[str, Any]) -> str:
    """Create description for field, using values `description` and `guidelines`."""
    # Main description
    toc = "[Description](#description) "
    description = f"## Description\n\n {field['description']}"
    # Guidelines
    if field.get("guidelines"):
        description += "\n## Guidelines\n\n" + guidelines_to_markdown(guidelines=field["guidelines"], extra_tab=0)
        toc += "| [Guidelines](#guidelines) "
    # Examples (good vs bad)
    if field.get("examples"):
        if "examples_bad" in field:
            description += "\n## Examples\n\n" + examples_to_markdown(
                examples=field["examples"], examples_bad=field["examples_bad"], extra_tab=0, do_sign="✅", dont_sign="❌"
            )
        else:
            description += "\n## Examples\n\n" + examples_to_markdown(
                examples=field["examples"], examples_bad=[], extra_tab=0, do_sign="✅", dont_sign="❌"
            )
        toc += "| [Examples](#examples) "
    # FAQs
    if field.get("faqs"):
        description += "\n## FAQs\n\n" + faqs_to_markdown(faqs=field["faqs"], extra_tab=0)
        toc += "| [FAQs](#faqs) "
    # Insert TOC at the beginnining of description
    description = toc.strip() + "\n\n" + description
    return description


def render_fields_init() -> None:
    """Render fields to create directories and all."""
    # Text inputs
    fields = [
        {
            "title": "Namespace",
            "description": "## Description\n\nInstitution or topic name",
            "placeholder": "'emdat', 'health'",
        },
        {
            "title": "Snapshot version",
            "description": "## Description\n\nVersion of the snapshot dataset (by default, the current date, or exceptionally the publication date).",
            "placeholder": f"'{utils.DATE_TODAY}'",
            "value": utils.DATE_TODAY,
        },
        {
            "title": "Short name",
            "description": "## Description\n\nDataset short name using [snake case](https://en.wikipedia.org/wiki/Snake_case). Example: natural_disasters",
            "placeholder": "'cherry_blossom'",
        },
        {
            "title": "File extension",
            "description": "## Description\n\nFile extension (without the '.') of the file to be downloaded.",
            "placeholder": "'csv', 'xls', 'zip'",
        },
    ]
    for field in fields:
        key = field["title"].replace(" ", "_").lower()
        args = {
            "st_widget": st.text_input,
            "label": create_display_name_init_section(field["title"]),
            "help": field["description"],
            "placeholder": f"Example: {field['placeholder']}",
            "key": key,
            "default_last": field.get("value", ""),
        }
        if APP_STATE.args.dummy_data:
            args["value"] = dummy_values[key]

        APP_STATE.st_widget(**args)

    # Private dataset?
    APP_STATE.st_widget(
        st.toggle,
        label="Make dataset private",
        help="Check if you want to make the dataset private.",
        key="is_private",
        default_last=False,
    )
    # Import local file?
    APP_STATE.st_widget(
        st.toggle,
        label="Import dataset from local file",
        help="Check if you want to import the snapshot from a local file.",
        key="local_import",
        default_last=False,
    )


def render_fields_from_schema(
    schema: Dict[str, Any],
    property_name: str,
    form_fields: List[str],
    categories: Optional[List[Any]] = None,
    container: Optional[Any] = None,
) -> List[str]:
    """Render fields from schema.

    This function is quite complex, and includes some recursion. Could probably be improved, but it is OK for now.
    """
    # Define containers (if applicable)
    containers = {}
    if categories:
        for cat in categories:
            containers[cat] = st.container()
            containers[cat].markdown(f"#### {cat.capitalize()}")
    # Iterate over schema
    for name, props in schema.items():
        prop_uri = f"{property_name}.{name}"
        if "properties" in props:
            if categories:
                form_fields = render_fields_from_schema(
                    props["properties"], prop_uri, form_fields, container=containers[cat]  # type: ignore
                )
            else:
                form_fields = render_fields_from_schema(props["properties"], prop_uri, form_fields)
        else:
            # Define display, help and placeholder texts
            display_name = create_display_name_snap_section(props, name, property_name)
            # Render field
            ## Text areas
            if prop_uri in FIELD_TYPES_TEXTAREA:
                # Use text area for these fields
                kwargs = {
                    "label": display_name,
                    "help": create_description(field=props),
                    "placeholder": props["examples"][:3] if isinstance(props["examples"], str) else "",
                    "key": prop_uri,
                }
                if APP_STATE.args.dummy_data:
                    kwargs["value"] = dummy_values.get(prop_uri, "")

                if categories:
                    with containers[props["category"]]:
                        field = APP_STATE.st_widget(st_widget=st.text_area, **kwargs)  # type: ignore
                elif container:
                    with container:
                        field = APP_STATE.st_widget(st_widget=st.text_area, **kwargs)
                else:
                    field = APP_STATE.st_widget(st_widget=st.text_area, **kwargs)
            ## Special case: license name (select box)
            elif prop_uri in ["origin.license.name", "origin.attribution"]:
                # Special one, need to have responsive behaviour inside form (work around)
                if categories:
                    with containers[props["category"]]:
                        field = [prop_uri, st.empty(), st.container()]  # type: ignore
                elif container:
                    with container:
                        field = [prop_uri, st.empty(), st.container()]
                else:
                    field = [prop_uri, st.empty(), st.container()]
            ## Text input
            else:
                # default_value = DEFAULT_VALUES.get(prop_uri, "")
                # Simple text input for the rest
                kwargs = {
                    "label": display_name,
                    "help": create_description(field=props),
                    "placeholder": "Examples: " + ", ".join([f"'{ex}'" for ex in props["examples"]])
                    if props["examples"]
                    else "",
                    "key": prop_uri,
                }
                if APP_STATE.args.dummy_data:
                    kwargs["value"] = dummy_values.get(prop_uri, "")

                if categories:
                    with containers[props["category"]]:
                        field = APP_STATE.st_widget(st.text_input, **kwargs)  # type: ignore
                elif container:
                    with container:
                        field = APP_STATE.st_widget(st.text_input, **kwargs)
                else:
                    field = APP_STATE.st_widget(st.text_input, **kwargs)
            # Add field to list
            form_fields.append(cast(str, field))
    return form_fields


def render_license_field(form: List[Any]) -> List[str]:
    """Render the license field within the form.

    We want the license field to be a selectbox, but with the option to add a custom license.

    This is a workaround to have repsonsive behaviour within a form.

    Source: https://discuss.streamlit.io/t/can-i-add-to-a-selectbox-an-other-option-where-the-user-can-add-his-own-answer/28525/5
    """
    # Assert there is only one element of type list
    assert (
        len([field for field in form if isinstance(field, list)]) == 2
    ), "More than one element in the form is of type list!"

    # Get relevant values from schema
    property_name = "origin.license"
    name = "name"
    prop_uri = f"{property_name}.{name}"
    props_license = schema_origin["license"]
    props = props_license["properties"][name]
    display_name = create_display_name_snap_section(props, name, property_name)

    # Main decription
    toc = "[Description](#description) "
    help_text = (
        "## Description\n\n" + props["description"] + "\n\n" + props_license["description"].replace("\n", "\n\n\n")
    )
    # Guidelines
    if props.get("guidelines"):
        help_text += "\n## Guidelines" + guidelines_to_markdown(guidelines=props["guidelines"])
        toc += "| [Guidelines](#guidelines) "
    # Examples (good vs bad)
    if props.get("examples"):
        if "examples_bad" in props:
            help_text += "\n## Examples" + examples_to_markdown(
                examples=props["examples"], examples_bad=props["examples_bad"], extra_tab=0, do_sign="✅", dont_sign="❌"
            )
        else:
            help_text += "\n## Examples" + examples_to_markdown(
                examples=props["examples"], examples_bad=[], extra_tab=0, do_sign="✅", dont_sign="❌"
            )
        toc += "| [Examples](#examples) "
    help_text = toc.strip() + "\n\n" + help_text
    options = sorted(props["options"])

    # Default option in select box for custom license
    CUSTOM_OPTION = "Custom license..."
    # Render and get element depending on selection in selectbox
    for field in form:
        if isinstance(field, list) and field[0] == "origin.license.name":
            with field[1]:
                license_field = APP_STATE.st_widget(
                    st.selectbox,
                    label=display_name,
                    options=["Custom license..."] + options,
                    help=help_text,
                    key=prop_uri,
                    default_last=options[0],
                )
            with field[2]:
                if license_field == CUSTOM_OPTION:
                    license_field = APP_STATE.st_widget(
                        st.text_input,
                        label="↳ *Use custom license*",
                        placeholder="© GISAID 2023",
                        help="Enter custom license",
                        key=f"{prop_uri}_custom",
                    )

    # Remove list from form (former license st.empty tuple)
    form = [f for f in form if not isinstance(f, list)]

    # Add license field
    form.append(license_field)  # type: ignore

    return form


def render_attribution_field(form: List[Any]) -> List[str]:
    """Render the attribution field within the form.

    We want the attribution field to be a selectbox, but with the option to add a custom license.

    This is a workaround to have repsonsive behaviour within a form.

    Source: https://discuss.streamlit.io/t/can-i-add-to-a-selectbox-an-other-option-where-the-user-can-add-his-own-answer/28525/5
    """
    # Assert there is only one element of type list
    assert (
        len([field for field in form if isinstance(field, list)]) == 2
    ), "More than one element in the form is of type list!"

    # Get relevant values from schema
    parent = "origin"
    name = "attribution"
    prop_uri = f"{parent}.{name}"
    props = schema_origin[name]
    display_name = create_display_name_snap_section(props, name, parent, title="Attribution format")

    # Main decription
    toc = "[Description](#description) "
    description_add = """Use this dropdown to select the desired `attribution` format.

The default option should be used in most cases. Alternatively other common format options are available.

Only in rare occasions you will need to define a custom attribution.
    """
    help_text = "## Description\n\n" + description_add
    # Guidelines
    if props.get("guidelines"):
        help_text += "\n## Guidelines" + guidelines_to_markdown(guidelines=props["guidelines"])
        toc += "| [Guidelines](#guidelines) "
    # Examples (good vs bad)
    if props.get("examples"):
        if "examples_bad" in props:
            help_text += "\n## Examples" + examples_to_markdown(
                examples=props["examples"], examples_bad=props["examples_bad"], extra_tab=0, do_sign="✅", dont_sign="❌"
            )
        else:
            help_text += "\n## Examples" + examples_to_markdown(
                examples=props["examples"], examples_bad=[], extra_tab=0, do_sign="✅", dont_sign="❌"
            )
        toc += "| [Examples](#examples) "
    help_text = toc.strip() + "\n\n" + help_text

    # Options
    DEFAULT_OPTION = "{producer} ({year})"
    options = [
        DEFAULT_OPTION,
        "{producer} - {title} {version_producer} ({year})",
        # "{title} {version_producer} - {producer} ({year})",
    ]

    # Default option in select box for custom license
    CUSTOM_OPTION = "Custom attribution..."
    # Render and get element depending on selection in selectbox
    for field in form:
        if isinstance(field, list) and field[0] == "origin.attribution":
            with field[1]:
                attribution_field = APP_STATE.st_widget(
                    st.selectbox,
                    label=display_name,
                    options=["Custom attribution..."] + options,
                    help=help_text,
                    key=prop_uri,
                    default_last=options[0],
                )
            with field[2]:
                if attribution_field == CUSTOM_OPTION:
                    attribution_field = APP_STATE.st_widget(
                        st.text_input,
                        label="↳ *Use custom attribution*",
                        placeholder="",
                        help="Enter custom license. Make sure to add the explicit attribution and not its format (as in the dropdown options)!",
                        key=f"{prop_uri}_custom",
                    )

    # Remove list from form (former license st.empty tuple)
    form = [f for f in form if not isinstance(f, list)]

    # Add license field
    form.append(attribution_field)  # type: ignore

    return form


def update_state() -> None:
    """Submit form."""
    # Create form
    form = SnapshotForm.from_state()
    # Update states with values from form
    APP_STATE.update_from_form(form)


def run_snap_step() -> None:
    """Update state variables to correctly render the UI."""
    st.session_state["show_form"] = False
    st.session_state["run_step"] = True


#########################################################
# MAIN ##################################################
#########################################################

# TITLE
st.title("Wizard **:gray[Snapshot]**")

# SIDEBAR
with st.sidebar:
    utils.warning_metadata_unstable()

    # INSTRUCTIONS
    with st.expander("**Instructions**", expanded=True):
        text = load_instructions()
        st.markdown(text)
        st.markdown("3. **Only supports `origin`**. `source` is not supported anymore.")

# FORM
if st.session_state["show_form"]:
    form_widget = st.empty()
    with form_widget.form("form"):
        # 1) Show fields for initial configuration (create directories, etc.)
        # st.header("Config")
        st.markdown("Note that sometimes some fields might not be available (even if they are labelled as required)")
        render_fields_init()

        # 2) Show fields for metadata fields
        # st.header("Metadata")
        # st.markdown(
        #     "Fill the following fields to help us fill all the created files for you! Note that sometimes some fields might not be available (even if they are labelled as required)."
        # )
        # Get categories
        for k, v in schema_origin.items():
            if "category" not in v:
                print(k)
                print(v)
                # raise ValueError(f"Category not found for {k}")
        categories_in_schema = {v["category"] for k, v in schema_origin.items()}
        assert categories_in_schema == set(
            ACCEPTED_CATEGORIES
        ), f"Unknown categories in schema: {categories_in_schema - set(ACCEPTED_CATEGORIES)}"

        form_metadata = render_fields_from_schema(schema_origin, "origin", [], categories=ACCEPTED_CATEGORIES)

        # 3) Submit
        submitted = st.form_submit_button("Submit", type="primary", use_container_width=True, on_click=update_state)

    # 2.1) Create fields for attribution (responsive within form)
    form = render_attribution_field(form_metadata)

    # 2.1) Create fields for License (responsive within form)
    form = render_license_field(form_metadata)
else:
    submitted = False
    form_widget = st.empty()

#########################################################
# SUBMISSION ############################################
#########################################################
if submitted:
    # Create form
    form = cast(SnapshotForm, SnapshotForm.from_state())

    if not form.errors:
        form_widget.empty()

        # Create files
        utils.generate_step(
            cookiecutter_path=utils.COOKIE_SNAPSHOT,
            data=dict(**form.dict(), channel="snapshots"),
            target_dir=SNAPSHOTS_DIR,
        )
        ingest_path = SNAPSHOTS_DIR / form.namespace / form.snapshot_version / (form.short_name + ".py")
        meta_path = (
            SNAPSHOTS_DIR / form.namespace / form.snapshot_version / f"{form.short_name}.{form.file_extension}.dvc"
        )

        # Preview generated
        st.subheader("Generated files")
        utils.preview_file(ingest_path, "python")
        utils.preview_file(meta_path, "yaml")

        # Display next steps
        if form.dataset_manual_import:
            manual_import_instructions = "--path-to-file **relative path of file**"
        else:
            manual_import_instructions = ""
        st.subheader("Next steps")
        with st.expander("", expanded=True):
            st.markdown(
                """
            1. Verify that generated files are correct and update them if necessary.

            2. Run the snapshot step to upload files to S3
            """
            )
            args = []
            if form.dataset_manual_import:
                s_file = st.text_input(
                    label="Select local file to import", placeholder="path/to/file.csv", key="snapshot_file"
                )
            st.button("Run snapshot step", key="run_snapshot_step", on_click=run_snap_step)  # type: ignore
            st.markdown(
                f"""
            You can also run the step from the command line:
            ```bash
            python {ingest_path.relative_to(BASE_DIR)} {manual_import_instructions}
            ```

            3. Continue to the meadow step.
            """
            )

        # User message
        st.toast("Templates generated. Read the next steps.", icon="✅")

        # Update config
        utils.update_wizard_config(form=form)

        # st.write(st.session_state)
    else:
        st.write(form.errors)
        st.error("Form not submitted! Check errors!")


#########################################################
# EXECUTION #############################################
#########################################################
if st.session_state["run_step"]:
    # Get form
    form = cast(SnapshotForm, SnapshotForm.from_state())
    # Get snapshot script path
    script_path = f"{SNAPSHOTS_DIR}/{form.namespace}/{form.snapshot_version}/{form.short_name}.py"

    # Build command
    command = f"poetry run python {script_path}"
    commands = ["poetry", "run", "python", script_path]
    if form.dataset_manual_import:
        # Get snapshot local file
        commands.extend(["--path-to-file", st.session_state["snapshot_file"]])
    command_str = f"`{' '.join(commands)}`"

    # Run step
    with st.spinner(f"Running snapshot step... {command_str}"):
        try:
            output = subprocess.check_output(args=commands)
        except Exception as e:
            st.write("The snapshot was NOT uploaded! Please check the terminal for the complete error message.")
            tb_str = "".join(traceback.format_exception(e))
            st.error(tb_str)
        else:
            st.write("Snapshot should be uploaded!")
