"""Garden phase."""
import os
from pathlib import Path
from typing import List, cast

import streamlit as st
from owid.catalog import Dataset
from sqlalchemy.exc import OperationalError
from st_pages import add_indentation
from typing_extensions import Self

import etl.grapher_model as gm
from apps.utils.files import add_to_dag, generate_step_to_channel
from apps.wizard import utils
from etl.config import DB_HOST, DB_NAME
from etl.db import get_session
from etl.files import ruamel_dump, ruamel_load
from etl.paths import BASE_DIR, DAG_DIR, DATA_DIR

#########################################################
# CONSTANTS #############################################
#########################################################
# Page config
st.set_page_config(page_title="Wizard: Create a all steps", page_icon="🪄")
add_indentation()

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
TAGS_DEFAULT = [
    "Uncategorized",
    "Access to Energy",
    "Age Structure",
    "Agricultural Production",
    "Air Pollution",
    "Alcohol Consumption",
    "Animal Welfare",
    "Artificial Intelligence",
    "Biodiversity",
    "Biological & Chemical Weapons",
    "Books",
    "Burden of Disease",
    "CO2 & Greenhouse Gas Emissions",
    "COVID-19",
    "Cancer",
    "Cardiovascular Diseases",
    "Causes of Death",
    "Child & Infant Mortality",
    "Child Labor",
    "Clean Water",
    "Clean Water & Sanitation",
    "Climate Change",
    "Corruption",
    "Crop Yields",
    "Democracy",
    "Diarrheal Diseases",
    "Diet Compositions",
    "Economic Growth",
    "Economic Inequality",
    "Economic Inequality by Gender",
    "Education Spending",
    "Electricity Mix",
    "Employment in Agriculture",
    "Energy",
    "Energy Mix",
    "Environmental Impacts of Food Production",
    "Eradication of Diseases",
    "Famines",
    "Farm Size",
    "Fertility Rate",
    "Fertilizers",
    "Financing Healthcare",
    "Fish & Overfishing",
    "Food Prices",
    "Food Supply",
    "Forests & Deforestation",
    "Fossil Fuels",
    "Gender Ratio",
    "Global Education",
    "Global Health",
    "Government Spending",
    "HIV/AIDS",
    "Happiness & Life Satisfaction",
    "Homelessness",
    "Homicides",
    "Human Development Index (HDI)",
    "Human Height",
    "Human Rights",
    "Hunger & Undernourishment",
    "Illicit Drug Use",
    "Indoor Air Pollution",
    "Influenza",
    "Internet",
    "LGBT+ Rights",
    "Land Use",
    "Lead Pollution",
    "Life Expectancy",
    "Light at Night",
    "Literacy",
    "Loneliness & Social Connections",
    "Malaria",
    "Marriages & Divorces",
    "Maternal Mortality",
    "Meat & Dairy Production",
    "Mental Health",
    "Micronutrient Deficiency",
    "Migration",
    "Military Personnel & Spending",
    "Mpox (monkeypox)",
    "Natural Disasters",
    "Neurodevelopmental Disorders",
    "Nuclear Energy",
    "Nuclear Weapons",
    "Obesity",
    "Oil Spills",
    "Outdoor Air Pollution",
    "Ozone Layer",
    "Pandemics",
    "Pesticides",
    "Plastic Pollution",
    "Pneumonia",
    "Polio",
    "Population Growth",
    "Poverty",
    "Pre-Primary Education",
    "Primary & Secondary Education",
    "Quality of Education",
    "Renewable Energy",
    "Research & Development",
    "Sanitation",
    "Smallpox",
    "Smoking",
    "Space Exploration & Satellites",
    "State Capacity",
    "Suicides",
    "Taxation",
    "Technological Change",
    "Terrorism",
    "Tertiary Education",
    "Tetanus",
    "Time Use",
    "Tourism",
    "Trade & Globalization",
    "Transport",
    "Trust",
    "Tuberculosis",
    "Urbanization",
    "Vaccination",
    "Violence Against Children & Children's Rights",
    "War & Peace",
    "Waste Management",
    "Water Use & Stress",
    "Women's Employment",
    "Women's Rights",
    "Working Hours",
]
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
    include_metadata_yaml: bool
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
        }

    def garden_dict(self):
        """Get meadow dictionary."""
        return {
            "namespace": self.namespace,
            "short_name": self.short_name,
            "version": self.version,
            "meadow_version": self.version,
            "add_to_dag": self.add_to_dag,
            "dag_file": self.dag_file,
            "is_private": self.is_private,
            "update_period_days": self.update_period_days,
            "topic_tags": self.topic_tags,
            "include_metadata_yaml": self.include_metadata_yaml,
        }

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
        }


def update_state() -> None:
    """Submit form."""
    # Create form
    form = ExpressForm.from_state()
    # Update states with values from form
    APP_STATE.update_from_form(form)


def _fill_dummy_metadata_yaml(metadata_path: Path) -> None:
    """Fill dummy metadata yaml file with some dummy values.

    Only useful when `--dummy-data` is used. We need this to avoid errors in `etlwiz grapher --dummy-data`.
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
st.title("Create step 🔥 **:gray[Express]**")

# SIDEBAR
with st.sidebar:
    # utils.warning_metadata_unstable()
    with st.expander("**Instructions**", expanded=True):
        text = load_instructions()  # TODO: which instructions?
        st.markdown(text)

# FORM
form_widget = st.empty()
with form_widget.form("express"):
    # Get default version (used in multiple fields)
    # if (default_version := APP_STATE.default_value("snapshot_version", previous_step="snapshot")) == "":
    #     default_version = APP_STATE.default_value("version", previous_step="snapshot", default_last=utils.DATE_TODAY)
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

    with st.popover("Other parameters"):
        APP_STATE.st_widget(
            st.toggle,
            label="Include *.meta.yaml file with metadata",
            key="include_metadata_yaml",
            default_last=True,
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

        # Private dataset?
        private_suffix = "-private" if form.is_private else ""

        generated_files = []

        #######################
        # MEADOW ##############
        #######################
        DATASET_DIR = generate_step_to_channel(
            cookiecutter_path=utils.COOKIE_MEADOW, data=dict(**form.meadow_dict(), channel="meadow")
        )
        generated_files.append(
            {
                "path": DATASET_DIR / (form.short_name + ".py"),
                "language": "python",
                "channel": "meadow",
            }
        )

        #######################
        # GARDEN ##############
        #######################
        ## HOTFIX 1: filter topic_tags if empty
        garden_dict = form.garden_dict()
        if garden_dict.get("topic_tags") is None or garden_dict.get("topic_tags") == []:
            garden_dict["topic_tags"] = ""
        ## HOTFIX 2: For some reason, when using cookiecutter only the first element in the list is taken?
        ## Hence we need to convert the list to an actual string
        else:
            garden_dict["topic_tags"] = "- " + "\n- ".join(garden_dict["topic_tags"])
        ## Create py
        DATASET_DIR = generate_step_to_channel(
            cookiecutter_path=utils.COOKIE_GARDEN, data=dict(**garden_dict, channel="garden")
        )
        generated_files.append(
            {
                "path": DATASET_DIR / (form.short_name + ".py"),
                "language": "python",
                "channel": "garden",
            }
        )
        ## Create metadata
        metadata_path = DATASET_DIR / (form.short_name + ".meta.yml")
        if (not form.include_metadata_yaml) and (metadata_path.is_file()):
            os.remove(metadata_path)
        generated_files.append(
            {
                "path": metadata_path,
                "language": "yaml",
                "channel": "garden",
            }
        )

        #######################
        # GRAPHER #############
        #######################
        DATASET_DIR = generate_step_to_channel(
            cookiecutter_path=utils.COOKIE_GRAPHER, data=dict(**form.grapher_dict(), channel="grapher")
        )
        generated_files.append(
            {
                "path": DATASET_DIR / (form.short_name + ".py"),
                "language": "python",
                "channel": "grapher",
            }
        )

        # Add to DAG
        dag_path = DAG_DIR / form.dag_file
        if form.add_to_dag:
            dag_content = add_to_dag(
                dag={
                    f"data{private_suffix}://meadow/{form.namespace}/{form.version}/{form.short_name}": [
                        f"snapshot{private_suffix}://{form.namespace}/{form.snapshot_version}/{form.short_name}.{form.file_extension}",
                    ],
                    f"data{private_suffix}://garden/{form.namespace}/{form.version}/{form.short_name}": [
                        f"data{private_suffix}://meadow/{form.namespace}/{form.version}/{form.short_name}",
                    ],
                    f"data{private_suffix}://grapher/{form.namespace}/{form.version}/{form.short_name}": [
                        f"data{private_suffix}://garden/{form.namespace}/{form.version}/{form.short_name}",
                    ],
                },
                dag_path=dag_path,
            )
        else:
            dag_content = ""

        #######################
        # PREVIEW #############
        #######################
        st.subheader("Generated files")
        utils.preview_dag_additions(dag_content, dag_path, expanded=True)

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
                f"poetry run etl run data{private_suffix}://meadow/{form.namespace}/{form.version}/{form.short_name} {'--private' if form.is_private else ''}",
                language="shellSession",
            )

        with tab_garden:
            st.markdown("#### ⏭️ Next")
            ## 1/ Harmonize country names
            st.markdown("##### Harmonize country names")
            st.markdown("Run it in your terminal:")
            st.code(
                f"poetry run etl harmonize data/meadow/{form.namespace}/{form.version}/{form.short_name}/{form.short_name}.feather country etl/steps/data/garden/{form.namespace}/{form.version}/{form.short_name}.countries.json",
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
                f"poetry run etl run data{private_suffix}://garden/{form.namespace}/{form.version}/{form.short_name} {'--private' if form.is_private else ''}",
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
