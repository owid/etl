import os

import streamlit as st

from apps.wizard.utils import WIZARD_DIR
from etl.helpers import read_json_schema
from etl.paths import DAG_DIR, SCHEMAS_DIR
from etl.steps import load_dag

# Step icons
STEP_ICONS = {
    "meadow": ":material/nature:",
    "garden": ":material/deceased:",
    "grapher": ":material/database:",
}
STEP_NAME_PRESENT = {k: f"{v} {k.capitalize()}" for k, v in STEP_ICONS.items()}


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
COOKIE_SNAPSHOT = WIZARD_DIR / "etl_steps" / "cookiecutter" / "snapshot"
COOKIE_STEPS = {
    "snapshot": COOKIE_SNAPSHOT,
    "meadow": WIZARD_DIR / "etl_steps" / "cookiecutter" / "meadow",
    "garden": WIZARD_DIR / "etl_steps" / "cookiecutter" / "garden",
    "grapher": WIZARD_DIR / "etl_steps" / "cookiecutter" / "grapher",
}
# Paths to markdown templates
MD_SNAPSHOT = WIZARD_DIR / "etl_steps" / "markdown" / "snapshot.md"


# DAG dropdown options
dag_files = sorted([f for f in os.listdir(DAG_DIR) if f.endswith(".yml")])
dag_not_add_option = "(do not add to DAG)"
ADD_DAG_OPTIONS = [dag_not_add_option] + dag_files


# Read schema
SNAPSHOT_SCHEMA = read_json_schema(path=SCHEMAS_DIR / "snapshot-schema.json")
# Get properties for origin in schema
SCHEMA_ORIGIN = SNAPSHOT_SCHEMA["properties"]["meta"]["properties"]["origin"]["properties"]
