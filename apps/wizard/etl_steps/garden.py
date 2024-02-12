"""Garden phase."""
import os
from pathlib import Path
from typing import List, cast

import streamlit as st
from owid.catalog import Dataset
from st_pages import add_indentation
from typing_extensions import Self

import etl.grapher_model as gm
from apps.wizard import utils
from etl.db import get_session
from etl.files import ruamel_dump, ruamel_load
from etl.paths import BASE_DIR, DAG_DIR, DATA_DIR, GARDEN_DIR

#########################################################
# CONSTANTS #############################################
#########################################################
# Page config
st.set_page_config(page_title="Wizard: Create a Garden step", page_icon="🪄")
add_indentation()

# Available namespaces
OPTIONS_NAMESPACES = sorted(os.listdir(GARDEN_DIR))


# Get current directory
CURRENT_DIR = Path(__file__).parent
# FIELDS FROM OTHER STEPS
st.session_state["step_name"] = "garden"
APP_STATE = utils.AppState()
# Config style
utils.config_style_html()
# DUMMY defaults
dummy_values = {
    "namespace": "dummy",
    "version": utils.DATE_TODAY,
    "short_name": "dummy",
    "meadow_version": "2020-01-01",
    "topic_tags": ["Uncategorized"],
}
# Get list of available tags from DB (only those used as topic pages)
with get_session() as session:
    tag_list = gm.Tag.load_tags(session)
tag_list = ["Uncategorized"] + sorted([tag.name for tag in tag_list])


#########################################################
# FUNCTIONS & CLASSES ###################################
#########################################################
@st.cache_data
def load_instructions() -> str:
    """Load snapshot step instruction text."""
    with open(file=utils.MD_GARDEN, mode="r") as f:
        return f.read()


class GardenForm(utils.StepForm):
    """Garden step form."""

    step_name: str = "garden"

    short_name: str
    namespace: str
    version: str
    meadow_version: str
    add_to_dag: bool
    dag_file: str
    include_metadata_yaml: bool
    generate_notebook: bool
    is_private: bool
    update_period_days: int
    topic_tags: List[str]

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
        fields_required = ["namespace", "version", "short_name", "meadow_version", "topic_tags"]
        fields_snake = ["namespace", "short_name"]
        fields_version = ["version", "meadow_version"]

        self.check_required(fields_required)
        self.check_snake(fields_snake)
        self.check_is_version(fields_version)

        # Check tags
        if (len(self.topic_tags) > 1) and ("Uncategorized" in self.topic_tags):
            self.errors["topic_tags"] = "If you choose multiple tags, you cannot choose `Uncategorized`."


def update_state() -> None:
    """Submit form."""
    # Create form
    form = GardenForm.from_state()
    # Update states with values from form
    APP_STATE.update_from_form(form)


def _check_dataset_in_meadow(form: GardenForm) -> None:
    private_suffix = "-private" if form.is_private else ""

    dataset_uri = f"data{private_suffix}://meadow/{form.namespace}/{form.meadow_version}/{form.short_name}"

    try:
        ds = Dataset(DATA_DIR / "meadow" / form.namespace / form.meadow_version / form.short_name)
        if form.short_name not in ds.table_names:
            st.warning(f"Table `{form.short_name}` not found in Meadow dataset. Available tables are {ds.table_names}.")
        else:
            st.success(f"Dataset ```{dataset_uri}``` found in Meadow!")
    except FileNotFoundError:
        # raise a warning, but continue
        st.warning(f"Dataset not found in Meadow! Have you run ```etl {dataset_uri}```?")


def _fill_dummy_metadata_yaml(metadata_path: Path) -> None:
    """Fill dummy metadata yaml file with some dummy values.

    Only useful when `--dummy-data` is used. We need this to avoid errors in `etl-wizard grapher --dummy-data`.
    """
    with open(metadata_path, "r") as f:
        doc = ruamel_load(f)

    # add all available metadata fields to dummy variable
    variable_meta = {
        "title": "Dummy",
        "description": "This is a dummy indicator with full metadata.",
        "unit": "Dummy unit",
        "short_unit": "Du",
        "display": {
            "isProjection": True,
            "conversionFactor": 1000,
            "numDecimalPlaces": 1,
            "tolerance": 5,
            "yearIsDay": False,
            "zeroDay": "1900-01-01",
            "entityAnnotationsMap": "Germany: dummy annotation",
            "includeInTable": True,
        },
        "description_processing": "This is some description of the dummy indicator processing.",
        "description_key": [
            "Key information 1",
            "Key information 2",
        ],
        "description_short": "Short description of the dummy indicator.",
        "description_from_producer": "The description of the dummy indicator by the producer, shown separately on a data page.",
        "processing_level": "major",
        "license": {"name": "CC-BY 4.0", "url": ""},
        "presentation": {
            "grapher_config": {
                "title": "The dummy indicator - chart title",
                "subtitle": "You'll never guess where the line will go",
                "hasMapTab": True,
                "selectedEntityNames": ["Germany", "Italy", "France"],
            },
            "title_public": "The dummy indicator - data page title",
            "title_variant": "historical data",
            "attribution_short": "ACME",
            "attribution": "ACME project",
            "topic_tags": ["Internet"],
            "key_info_text": [
                "First bullet point info about the data. [Detail on demand link](#dod:primaryenergy)",
                "Second bullet point with **bold** text and a [normal link](https://ourworldindata.org)",
            ],
            "faqs": [{"fragment_id": "cherries", "gdoc_id": "16uGVylqtS-Ipc3OCxqapJ3BEVGjWf648wvZpzio1QFE"}],
        },
    }

    doc["tables"]["dummy"]["variables"] = {"dummy_variable": variable_meta}

    with open(metadata_path, "w") as f:
        f.write(ruamel_dump(doc))


def export_metadata() -> None:
    dataset_path = st.session_state["garden.dataset_path"]
    try:
        output_path = utils.meta_export(dataset_path=dataset_path)
    except Exception as e:
        st.exception(e)
        st.stop()
    finally:
        st.success(f"Metadata exported to `{output_path}`.")


#########################################################
# MAIN ##################################################
#########################################################
# TITLE
st.title("Create step  **:gray[Garden]**")

# SIDEBAR
with st.sidebar:
    # utils.warning_metadata_unstable()
    with st.expander("**Instructions**", expanded=True):
        text = load_instructions()
        st.markdown(text)

# FORM
form_widget = st.empty()
with form_widget.form("garden"):
    # Get default version (used in multiple fields)
    if (default_version := APP_STATE.default_value("meadow_version")) == "":
        default_version = APP_STATE.default_value("version", default_last=utils.DATE_TODAY)

    # Namespace
    # APP_STATE.st_widget(
    #     st_widget=st.text_input,
    #     label="Namespace",
    #     help="Institution or topic name",
    #     placeholder="Example: 'emdat', 'health'",
    #     key="namespace",
    #     value=dummy_values["namespace"] if APP_STATE.args.dummy_data else None,
    # )
    # Namespace
    namespace_field = [st.empty(), st.container()]

    # Garden version
    APP_STATE.st_widget(
        st_widget=st.text_input,
        label="Garden dataset version",
        help="Version of the garden dataset (by default, the current date, or exceptionally the publication date).",
        key="version",
        default_last=default_version,
        value=dummy_values["version"] if APP_STATE.args.dummy_data else default_version,
    )
    # Garden short name
    APP_STATE.st_widget(
        st_widget=st.text_input,
        label="Garden dataset short name",
        help="Dataset short name using [snake case](https://en.wikipedia.org/wiki/Snake_case). Example: natural_disasters",
        placeholder="Example: 'cherry_blossom'",
        key="short_name",
        value=dummy_values["short_name"] if APP_STATE.args.dummy_data else None,
    )

    st.markdown("#### Dataset")
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
        st_widget=st.multiselect,
        label="Indicators tag",
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

    st.markdown("#### Dependencies")
    # Meadow version
    APP_STATE.st_widget(
        st_widget=st.text_input,
        label="Meadow version",
        help="Version of the meadow dataset (by default, the current date, or exceptionally the publication date).",
        key="meadow_version",
        default_last=default_version,
        value=dummy_values["meadow_version"] if APP_STATE.args.dummy_data else None,
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
    APP_STATE.st_widget(
        st.toggle,
        label="Include *.meta.yaml file with metadata",
        key="include_metadata_yaml",
        default_last=True,
    )
    APP_STATE.st_widget(
        st.toggle,
        label="Generate playground notebook",
        key="generate_notebook",
        default_last=False,
    )
    APP_STATE.st_widget(
        st.toggle,
        label="Make dataset private",
        key="is_private",
        default_last=False,
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
    form = cast(GardenForm, GardenForm.from_state())

    if not form.errors:
        # Remove form from UI
        form_widget.empty()

        # Private dataset?
        private_suffix = "-private" if form.is_private else ""

        # Run checks on dependency
        if APP_STATE.args.run_checks:
            _check_dataset_in_meadow(form)

        # Add to dag
        dag_path = DAG_DIR / form.dag_file
        if form.add_to_dag:
            deps = [f"data{private_suffix}://meadow/{form.namespace}/{form.meadow_version}/{form.short_name}"]
            dag_content = utils.add_to_dag(
                dag={f"data{private_suffix}://garden/{form.namespace}/{form.version}/{form.short_name}": deps},
                dag_path=dag_path,
            )
        else:
            dag_content = ""

        # Create necessary files
        ## HOTFIX 1: filter topic_tags if empty
        form_dict = form.dict()
        if form_dict.get("topic_tags") is None or form_dict.get("topic_tags") == []:
            form_dict["topic_tags"] = ""
        ## HOTFIX 2: For some reason, when using cookiecutter only the first element in the list is taken?
        ## Hence we need to convert the list to an actual string
        else:
            form_dict["topic_tags"] = "- " + "\n- ".join(form_dict["topic_tags"])

        DATASET_DIR = utils.generate_step_to_channel(
            cookiecutter_path=utils.COOKIE_GARDEN, data=dict(**form_dict, channel="garden")
        )

        step_path = DATASET_DIR / (form.short_name + ".py")
        notebook_path = DATASET_DIR / "playground.ipynb"
        metadata_path = DATASET_DIR / (form.short_name + ".meta.yml")

        if (not form.generate_notebook) and (notebook_path.is_file()):
            os.remove(notebook_path)

        if (not form.include_metadata_yaml) and (metadata_path.is_file()):
            os.remove(metadata_path)

        # Fill with dummy metadata
        if form.namespace == "dummy":
            _fill_dummy_metadata_yaml(metadata_path)

        # Preview generated files
        st.subheader("Generated files")
        if form.include_metadata_yaml:
            utils.preview_file(metadata_path, "yaml")
        utils.preview_file(step_path, "python")
        if form.generate_notebook:
            with st.expander(f"File: `{notebook_path}`", expanded=False):
                st.markdown("Open the file to see the generated notebook.")
        utils.preview_dag_additions(dag_content, dag_path)

        # Display next steps
        with st.expander("## Next steps", expanded=True):
            # 1/ Harmonize
            st.markdown("####  1. Harmonize Country names")
            st.markdown("Run it in your terminal:")
            st.code(
                f"poetry run etl-harmonize data/meadow/{form.namespace}/{form.meadow_version}/{form.short_name}/{form.short_name}.feather country etl/steps/data/garden/{form.namespace}/{form.version}/{form.short_name}.countries.json",
                "bash",
            )
            st.markdown("Or run it on Wizard")
            utils.st_page_link(
                "harmonizer",
                use_container_width=True,
                help="You will leave this page, and the guideline text will be hidden.",
            )

            # 2/ Run etl step
            st.markdown("####  2. Run ETL step")
            st.markdown("After editing the code of your Garden step, run the following command:")
            st.code(
                f"poetry run etl data{private_suffix}://garden/{form.namespace}/{form.version}/{form.short_name} {'--private' if form.is_private else ''}",
                "bash",
            )
            # 3/ Optional shit
            with st.container(border=True):
                st.markdown("**(Optional)**")
                # A/ Playground notebook
                st.markdown("#### Playground notebook")
                st.markdown(
                    f"Use the generated notebook `{notebook_path.relative_to(BASE_DIR)}` to examine the dataset output interactively."
                )
                # B/ Generate metadata
                st.session_state[
                    "garden.dataset_path"
                ] = f"data/garden/{form.namespace}/{form.version}/{form.short_name}"
                st.markdown("#### Generate metadata")
                st.markdown(f"Generate metadata file `{form.short_name}.meta.yml` from your dataset with:")
                st.button(
                    "Metadata export",
                    help="Once clicked, the whole guideline will disappear.",
                    on_click=export_metadata,
                )

                with st.container(border=True):
                    st.markdown("Alternitavely you can generate the metadata with the following command:")
                    st.code(
                        f"poetry run etl-metadata-export {st.session_state['garden.dataset_path']}",
                        "bash",
                    )

                st.markdown("then manual edit it and rerun the step again with")
                st.code(
                    f"poetry run etl data{private_suffix}://garden/{form.namespace}/{form.version}/{form.short_name} {'--private' if form.is_private else ''}",
                    "bash",
                )

                # C/ Organize DAG
                st.markdown("#### Organize the DAG")
                st.markdown(f"Check the DAG `{dag_path}`.")

            # 4/ Final steps
            st.markdown("####  3. Pull request")
            st.markdown("Create a pull request in [ETL](https://github.com/owid/etl), get it reviewed and merged.")

            # D/ Load dataset from catalog
            with st.container(border=True):
                st.markdown("**(Optional)**")
                st.markdown("##### Load dataset from catalog")
                st.markdown(
                    "Once your changes are merged, your steps will be run automatically by our server and published to the OWID catalog. Then it can be loaded by anyone using:"
                )
                st.code(
                    f"""
                from owid.catalog import find_one
                tab = find_one(table="{form.short_name}", namespace="{form.namespace}", dataset="{form.short_name}")
                print(tab.metadata)
                print(tab.head())
                """,
                    "python",
                )

            # 5/ Push to Grapher
            st.markdown("####  4. Push to Grapher")
            st.markdown(
                "If you are an internal OWID member and want to push data to our Grapher DB, continue to the grapher step or to explorers step."
            )

        # User message
        st.toast("Templates generated. Read the next steps.", icon="✅")

        # Update config
        utils.update_wizard_config(form=form)
    else:
        st.write(form.errors)
        st.error("Form not submitted! Check errors!")
