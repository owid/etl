"""Snapshot phase."""
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, cast

import streamlit as st
from botocore.exceptions import ClientError
from owid.catalog import s3_utils
from st_pages import add_indentation
from typing_extensions import Self

from apps.wizard import utils
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
SNAPSHOT_SCHEMA = read_json_schema(SCHEMAS_DIR / "snapshot-schema.json")
# Get properties for origin in schema
schema_origin = SNAPSHOT_SCHEMA["properties"]["meta"]["properties"]["origin"]["properties"]
# Lists with fields of special types. By default, fields are text inputs.
FIELD_TYPES_TEXTAREA = [
    "origin.dataset_description_owid",
    "origin.dataset_description_producer",
    "origin.citation_producer",
]
FIELD_TYPES_SELECT = ["origin.license.name"]
# Get current directory
CURRENT_DIR = Path(__file__).parent
# Accepted schema categories
ACCEPTED_CATEGORIES = ["dataset", "citation", "files", "license"]
# FIELDS FROM OTHER STEPS
st.session_state["step_name"] = "snapshot"
APP_STATE = utils.AppState()

# Session state variables initialitzatio


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
    dataset_title_owid: str
    dataset_description_owid: str
    dataset_title_producer: str
    origin_version: str
    date_published: str
    dataset_description_producer: str
    producer: str
    citation_producer: str
    attribution: str
    attribution_short: str
    dataset_url_main: str
    dataset_url_download: str
    date_accessed: str

    # license
    license_url: str
    license_name: str

    def __init__(self: Self, **data: str | int) -> None:
        """Construct form."""
        # Change name for certain fields (and remove old ones)
        data["license_url"] = data["origin.license.url"]
        if "origin.license.name_custom" in data:
            data["license_name"] = data["origin.license.name_custom"]
        else:
            data["license_name"] = data["origin.license.name"]
        data["origin_version"] = data["origin.version_producer"]
        data["dataset_manual_import"] = data["local_import"]
        data = {
            k: v for k, v in data.items() if k not in ["origin.license.url", "origin.license.name", "origin.version"]
        }
        # Remove 'origin.' prefix from keys
        data = {k.replace("origin.", ""): v for k, v in data.items()}
        super().__init__(**data)

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
                    "dataset_title_owid": self.dataset_title_owid,
                    "dataset_title_producer": self.dataset_title_producer,
                    "dataset_description_owid": self.dataset_description_owid.replace("\n", "\n      "),
                    "dataset_description_producer": self.dataset_description_producer.replace("\n", "\n      "),
                    "producer": self.producer,
                    "citation_producer": self.citation_producer,
                    "attribution": self.attribution,
                    "attribution_short": self.attribution_short,
                    "version": self.origin_version,
                    "dataset_url_main": self.dataset_url_main,
                    "dataset_url_download": self.dataset_url_download,
                    "date_published": self.date_published,
                    "date_accessed": self.date_accessed,
                    "license": license_field,
                },
                "license": license_field,
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


def create_display_name_snap_section(props: Dict[str, Any], name: str, property_name: str) -> str:
    """Create display name for a field."""
    # Get requirement level colored
    req_level = _color_req_level(props["requirement_level"])
    # Create display name
    display_name = f"{props['title']} (`{property_name}.{name}`) ┃ {req_level}"
    return display_name


@st.cache_data
def load_instructions() -> str:
    """Load snapshot step instruction text."""
    with open(file=utils.MD_SNAPSHOT, mode="r") as f:
        return f.read()


def create_description(field: Dict[str, Any]) -> str:
    """Create description for field, using values `description` and `guidelines`."""
    description = field["description"]
    if field.get("guidelines"):
        description += "\n\n" + guidelines_to_markdown(guidelines=field["guidelines"])
    return description


def guidelines_to_markdown(guidelines: List[Any]) -> str:
    """Render guidelines to markdown from given list in schema."""
    text = "**Guidelines**\n"
    for guideline in guidelines:
        # Main guideline
        if isinstance(guideline[0], str):
            # Add main guideline
            text += f"\n- {guideline[0]}"
        else:
            raise TypeError("The first element of an element in `guidelines` must be a string!")

        # Additions to the guideline (nested bullet points, exceptions, etc.)
        if len(guideline) == 2:
            if isinstance(guideline[1], dict):
                # Sanity checks
                if "type" not in guideline[1]:
                    raise ValueError("The second element of an element in `guidelines` must have a `type` key!")
                if "value" not in guideline[1]:
                    raise ValueError("The second element of an element in `guidelines` must have a `value` key!")

                # Render exceptions
                if guideline[1]["type"] == "exceptions":
                    text += " Exceptions:"
                    for exception in guideline[1]["value"]:
                        text += f"\n\t- {exception}"
                # Render nested list
                elif guideline[1]["type"] == "list":
                    for subitem in guideline[1]["value"]:
                        text += f"\n\t- {subitem}"
                # Exception
                else:
                    raise ValueError(f"Unknown guideline type: {guideline[1]['type']}!")
            else:
                raise TypeError("The second element of an element in `guidelines` must be a dictionary!")

        # Element in guideliens is more than 2 items long
        if len(guideline) > 2:
            raise ValueError("Each element in `guidelines` must have at most 2 elements!")
    return text


def render_fields_init() -> None:
    """Render fields to create directories and all."""
    # Text inputs
    fields = [
        {
            "title": "Namespace",
            "description": "Institution or topic name",
            "placeholder": "'emdat', 'health'",
        },
        {
            "title": "Snapshot Version",
            "description": "Version of the snapshot dataset (by default, the current date, or exceptionally the publication date).",
            "placeholder": f"'{utils.DATE_TODAY}'",
            "value": utils.DATE_TODAY,
        },
        {
            "title": "Short name",
            "description": "Dataset short name using [snake case](https://en.wikipedia.org/wiki/Snake_case). Example: natural_disasters",
            "placeholder": "'cherry_blossom'",
        },
        {
            "title": "File extension",
            "description": "File extension (without the '.') of the file to be downloaded.",
            "placeholder": "'csv', 'xls', 'zip'",
        },
    ]
    for field in fields:
        key = field["title"].replace(" ", "_").lower()
        APP_STATE.st_widget(
            st.text_input,
            label=create_display_name_init_section(field["title"]),
            help=field["description"],
            placeholder=f"Example: {field['placeholder']}",
            key=key,
            default_last=field.get("value", ""),
        )

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
                    "placeholder": props["examples"] if props["examples"] else "",
                    "key": prop_uri,
                }
                if categories:
                    with containers[props["category"]]:
                        field = APP_STATE.st_widget(st.text_area, **kwargs)  # type: ignore
                elif container:
                    with container:
                        field = APP_STATE.st_widget(st.text_area, **kwargs)
                else:
                    field = APP_STATE.st_widget(st.text_area, **kwargs)
            ## Special case: license name (select box)
            elif prop_uri == "origin.license.name":
                # Special one, need to have responsive behaviour inside form (work around)
                if categories:
                    with containers[props["category"]]:
                        field = [st.empty(), st.container()]  # type: ignore
                elif container:
                    with container:
                        field = [st.empty(), st.container()]
                else:
                    field = [st.empty(), st.container()]
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
        len([field for field in form if isinstance(field, list)]) == 1
    ), "More than one element in the form is of type list!"

    # Get relevant values from schema
    property_name = "origin.license"
    name = "name"
    prop_uri = f"{property_name}.{name}"
    props_license = schema_origin["license"]
    props = props_license["properties"][name]
    display_name = create_display_name_snap_section(props, name, property_name)

    help_text = props["description"] + "\n\n" + props_license["description"].replace("\n", "\n\n\n")
    options = sorted(props["options"])

    # Default option in select box for custom license
    CUSTOM_OPTION = "Custom license..."
    # Render and get element depending on selection in selectbox
    for field in form:
        if isinstance(field, list):
            with field[0]:
                license_field = APP_STATE.st_widget(
                    st.selectbox,
                    label=display_name,
                    options=["Custom license..."] + options,
                    help=help_text,
                    key=prop_uri,
                    default_last=options[0],
                )
            with field[1]:
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


@st.cache_resource
def _aws_is_ok() -> None | s3_utils.MissingCredentialsError:
    try:
        s3_utils.check_for_default_profile()
    except s3_utils.MissingCredentialsError as e:
        return e


@st.cache_resource
def _get_s3_buckets() -> Tuple[bool, Any]:
    s3 = s3_utils.connect()
    try:
        buckets = s3.list_buckets()["Buckets"]
    except ClientError as e:
        return (True, e)
    return (False, buckets)


def run_checks() -> None:
    """Environment checks."""
    text_reference_expander = "\n\nExpand the **Environment checks** section for more details!"
    # AWS config
    aws_error = _aws_is_ok()
    if aws_error:
        text = "Invalid AWS profile:\n{}".format(aws_error)
        st.error(text)
        st.toast(text + text_reference_expander, icon="❌")
        raise aws_error
    else:
        text = "AWS profile is valid"
        # st.toast(text, icon="✅")
        st.success(text)

    # S3 conncetion
    error, buckets_or_error = _get_s3_buckets()
    if error:
        text = "Error connecting to S3:\n{}".format(buckets_or_error)
        st.error(text)
        st.toast(text + text_reference_expander, icon="❌")
        raise buckets_or_error
    else:
        text = "S3 connection successful"
        # st.toast(text, icon="✅")
        st.success(text)

        bucket_names = [b["Name"] for b in buckets_or_error]  # type: ignore
        if "owid-catalog" not in bucket_names:
            text = "`owid-catalog` bucket not found"
            st.error(text)
            st.toast(text + text_reference_expander, icon="❌")
            raise Exception()


def update_state() -> None:
    """Submit form."""
    # Create form
    form = SnapshotForm.from_state()
    # Update states with values from form
    APP_STATE.update_from_form(form)


#########################################################
# MAIN ##################################################
#########################################################

# TITLE
st.title("Wizard **:gray[Snapshot]**")

# SIDEBAR
with st.sidebar:
    utils.warning_notion_latest()
    if APP_STATE.args.run_checks:
        # CONNECT AND CHECK
        with st.expander("**Environment checks**", expanded=True):
            run_checks()

    # INSTRUCTIONS
    with st.expander("**Instructions**", expanded=True):
        text = load_instructions()
        st.markdown(text)
        st.markdown("3. **Only supports `origin`**. To work with `source` instead, use `walkthrough`.")

# FORM
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

# 2.1) Create fields for License (responsive within form)
form = render_license_field(form_metadata)


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
            data=dict(**form.dict(), channel="snapshots", walkthrough_origins=utils.WALKTHROUGH_ORIGINS),
            target_dir=SNAPSHOTS_DIR,
        )
        ingest_path = SNAPSHOTS_DIR / form.namespace / form.snapshot_version / (form.short_name + ".py")
        meta_path = (
            SNAPSHOTS_DIR / form.namespace / form.snapshot_version / f"{form.short_name}.{form.file_extension}.dvc"
        )
        form.to_yaml(meta_path)

        # Display next steps
        if form.dataset_manual_import:
            manual_import_instructions = "--path-to-file **relative path of file**"
        else:
            manual_import_instructions = ""
        with st.expander("## Next steps", expanded=True):
            st.markdown(
                f"""
            1. Verify that generated files are correct and update them if necessary

            2. Run the snapshot step to upload files to S3
            ```bash
            python {ingest_path.relative_to(BASE_DIR)} {manual_import_instructions}
            ```

            3. Continue to the meadow step
            """
            )

        # Preview generated
        st.subheader("Generated files")
        utils.preview_file(ingest_path, "python")
        utils.preview_file(meta_path, "yaml")

        # User message
        st.toast("Templates generated. Read the next steps.", icon="✅")

        # Update config
        utils.update_wizard_config(form=form)
    else:
        st.write(form.errors)
        st.error("Form not submitted! Check errors!")
