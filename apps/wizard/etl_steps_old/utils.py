import os
from typing import Any, List

import streamlit as st

from apps.wizard.utils import WIZARD_DIR
from etl.dag_helpers import load_dag
from etl.paths import DAG_DIR


@st.cache_data
def load_datasets(included_str) -> list[str]:
    """Load meadow datasets."""
    dag = load_dag()
    options = list(dag.keys())
    options = [o for o in options if included_str in o]
    options = sorted(options)
    return options


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
    "Neglected Tropical Diseases",
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
    "Wildfires",
    "Women's Employment",
    "Women's Rights",
    "Working Hours",
]


def remove_playground_notebook(dataset_dir, notebook_name: str = "playground.ipynb"):
    notebook_path = dataset_dir / notebook_name
    if notebook_path.is_file():
        os.remove(notebook_path)


# Paths to cookiecutter files
COOKIE_SNAPSHOT = WIZARD_DIR / "etl_steps_old" / "cookiecutter" / "snapshot"
COOKIE_MEADOW = WIZARD_DIR / "etl_steps_old" / "cookiecutter" / "meadow"
COOKIE_GARDEN = WIZARD_DIR / "etl_steps_old" / "cookiecutter" / "garden"
COOKIE_GRAPHER = WIZARD_DIR / "etl_steps_old" / "cookiecutter" / "grapher"
COOKIE_STEPS = {
    "snapshot": COOKIE_SNAPSHOT,
    "meadow": COOKIE_MEADOW,
    "garden": COOKIE_GARDEN,
    "grapher": COOKIE_GRAPHER,
}
# Paths to markdown templates
MD_SNAPSHOT = WIZARD_DIR / "etl_steps_old" / "markdown" / "snapshot.md"
MD_MEADOW = WIZARD_DIR / "etl_steps_old" / "markdown" / "meadow.md"
MD_GARDEN = WIZARD_DIR / "etl_steps_old" / "markdown" / "garden.md"
MD_GRAPHER = WIZARD_DIR / "etl_steps_old" / "markdown" / "grapher.md"
MD_EXPRESS = WIZARD_DIR / "etl_steps_old" / "markdown" / "express.md"
MD_STEPS = {
    "snapshot": MD_SNAPSHOT,
    "meadow": MD_MEADOW,
    "garden": MD_GARDEN,
    "grapher": MD_GRAPHER,
    "express": MD_EXPRESS,
}


# DAG dropdown options
dag_files = sorted([f for f in os.listdir(DAG_DIR) if f.endswith(".yml")])
dag_not_add_option = "(do not add to DAG)"
ADD_DAG_OPTIONS = [dag_not_add_option] + dag_files


def render_responsive_field_in_form(
    key: str,
    display_name: str,
    field_1: Any,
    field_2: Any,
    options: List[str],
    custom_label: str,
    help_text: str,
    app_state: Any,
    default_value: str,
) -> None:
    """Render the namespace field within the form.

    We want the namespace field to be a selectbox, but with the option to add a custom namespace.

    This is a workaround to have repsonsive behaviour within a form.

    Source: https://discuss.streamlit.io/t/can-i-add-to-a-selectbox-an-other-option-where-the-user-can-add-his-own-answer/28525/5
    """
    # Main decription
    help_text = "## Institution or topic name"

    # Render and get element depending on selection in selectbox
    with field_1:
        field = app_state.st_widget(
            st.selectbox,
            label=display_name,
            options=[custom_label] + options,
            help=help_text,
            key=key,
            default_last=default_value,  # dummy_values[prop_uri],
        )
    with field_2:
        if field == custom_label:
            default_value = app_state.default_value(key)
            field = app_state.st_widget(
                st.text_input,
                label="↳ *Use custom value*",
                placeholder="",
                help="Enter custom value.",
                key=f"{key}_custom",
                default_last=default_value,
            )
