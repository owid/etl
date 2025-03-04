import os

import streamlit as st

from apps.wizard.utils import WIZARD_DIR
from etl.dag_utils import load_dag
from etl.files import read_json_schema
from etl.paths import DAG_DIR, SCHEMAS_DIR

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
# If can't connect to DB, and for some reason can't access the schema (set TAGS_DEFAULT)
TAGS_DEFAULT_FALLBACK = [
    "Access to Energy",
    "Age Structure",
    "Agricultural Production",
    "Air Pollution",
    "Alcohol Consumption",
    "Animal Welfare",
    "Antibiotics & Antibiotic Resistance",
    "Artificial Intelligence",
    "Biodiversity",
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
    "Fish & Overfishing",
    "Food Prices",
    "Food Supply",
    "Foreign Aid",
    "Forests & Deforestation",
    "Fossil Fuels",
    "Gender Ratio",
    "Global Education",
    "Global Health",
    "Government Spending",
    "HIV/AIDS",
    "Happiness & Life Satisfaction",
    "Healthcare Spending",
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
    "Metals & Minerals",
    "Micronutrient Deficiency",
    "Migration",
    "Military Personnel & Spending",
    "Mpox (monkeypox)",
    "Natural Disasters",
    "Neglected Tropical Diseases",
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
    "Tetanus",
    "Time Use",
    "Tourism",
    "Trade & Globalization",
    "Transport",
    "Trust",
    "Tuberculosis",
    "Uncategorized",
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
DATASET_SCHEMA = read_json_schema(path=SCHEMAS_DIR / "dataset-schema.json")
# Get properties for origin in schema
SCHEMA_ORIGIN = SNAPSHOT_SCHEMA["properties"]["meta"]["properties"]["origin"]["properties"]
# Tags
try:
    TAGS_DEFAULT = DATASET_SCHEMA["properties"]["tables"]["additionalProperties"]["properties"]["variables"][
        "additionalProperties"
    ]["properties"]["presentation"]["properties"]["topic_tags"]["items"]["enum"]
except Exception:
    TAGS_DEFAULT = TAGS_DEFAULT_FALLBACK
