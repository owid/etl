"""Helper tool to create map brackets for all indicators in an indicator-based explorer.

"""

from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import requests
import streamlit as st
from owid.catalog import find
from sqlalchemy.orm import Session
from structlog import get_logger

from apps.explorer_update.cli import extract_variable_ids_from_explorer_content
from apps.wizard.utils import chart_html
from apps.wizard.utils.env import OWID_ENV
from etl.data_helpers.misc import round_to_nearest_power_of_ten, round_to_sig_figs
from etl.grapher_model import Entity, Variable
from etl.paths import BASE_DIR

# TODO:
#  * Create another slider (from 0 to 10) for tolerance.
#  * Add "custom" to the list of radio buttons for bracket type.
#  * Consider categorical values.

# Logging
log = get_logger()

# Default path to the explorers folder.
EXPLORERS_DIR = BASE_DIR.parent / "owid-content/explorers"
EXPLORER_NAME_DEFAULT = "natural-disasters-temp"

# Maximum number of brackets allowed in a chart.
MAX_NUM_BRACKETS = 10

# Minimum number of brackets allowed in a chart.
MIN_NUM_BRACKETS = 4

# Smallest number (in absolute value) to consider.
SMALLEST_NUMBER_DEFAULT = 0.01

# Labels of the different bracket types.
LABEL_LINEAR = "linear"
LABEL_LOG_X2 = "log x2"
LABEL_LOG_X3 = "log x3"
LABEL_LOG_X10 = "log x10"


@st.cache_data
def load_mappable_regions_and_ids(df: pd.DataFrame) -> Dict[str, int]:
    # TODO: Is there a better way to obtain the list of mappable countries, and their entity ids?
    # Load the external regions dataset.
    regions = find(
        "regions", dataset="regions", namespace="owid_grapher", channels=["external"], version="latest"
    ).load()
    regions_mappable = regions[regions["is_mappable"]]["name"].tolist()

    # List all entity ids used by this variable.
    entity_ids = list(set(df["entities"]))

    # Fetch the mapping of entity ids to names.
    with Session(OWID_ENV.engine) as session:
        entities = Entity.load_entity_mapping(session=session, entity_ids=entity_ids)

    # Create a dictionary mapping regions (only the ones that show in grapher maps) to entity ids.
    region_to_id = {entity: entity_id for entity_id, entity in entities.items() if entity in regions_mappable}

    return region_to_id


@st.cache_data
def load_variable_from_id(variable_id: int):
    with Session(OWID_ENV.engine) as session:
        variable = Variable.load_variable(session=session, variable_id=variable_id)

    return variable


@st.cache_data
def load_variable_metadata(variable: Variable) -> Dict[str, Any]:
    metadata = requests.get(variable.s3_metadata_path(typ="http")).json()

    return metadata


@st.cache_data
def load_variable_data(variable: Variable) -> pd.DataFrame:
    data = requests.get(variable.s3_data_path(typ="http")).json()
    df = pd.DataFrame(data)

    return df


@st.cache_data
def create_default_chart_config_for_variable(metadata: Dict[str, Any]) -> Dict[str, Any]:
    """Create a default chart for a variable with id `variable_id`."""
    chart_config = {
        "hasMapTab": True,
        "hasChartTab": False,
        "tab": "map",
        "map": {
            # "timeTolerance": 0,
            "colorScale": {
                # "baseColorScheme": "BrBG",
                "binningStrategy": "manual",
                #     "customNumericMinValue": 0,
                #     "customNumericValues": [
                #         10,
                #         20,
                #     ]
            },
        },
        "dimensions": [{"property": "y", "variableId": metadata["id"]}],
        "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.004.json",
    }

    return chart_config


@st.cache_data
def load_explorer(explorer_path: Path) -> str:
    # Load explorer file as a string.
    with open(explorer_path, "r") as f:
        explorer = f.read()

    return explorer


########################################
# TITLE and DESCRIPTION
########################################

st.set_page_config(
    page_title="Wizard: ETL Map bracket generator",
    # layout="wide",
    page_icon="🪄",
    initial_sidebar_state="collapsed",
)
st.title("ETL Map bracket generator")
st.markdown("""🔨 WIP.""")

# Radio buttons to choose how to use this tool.
USE_TYPE_EXPLORERS = "by explorer"
USE_TYPE_CHART = "by chart"
USE_TYPE_ETL = "by etl path"
use_type = st.radio(
    "Select how to use this tool",
    options=[
        USE_TYPE_EXPLORERS,
        USE_TYPE_CHART,
        USE_TYPE_ETL,
    ],
    captions=[
        "NOT IMPLEMENTED: Select an indicator-based explorer, improve map brackets one by one, and update the explorer file.",
        "NOT IMPLEMENTED: Search for a chart by id or slug, improve its brackets, and save map configuration in the chart config to (staging) db.",
        "NOT IMPLEMENTED: Search for an ETL path to a dataset, improve indicator map brackets one by one, and save the configuration to a grapher yaml file.",
    ],
    index=0,
    horizontal=True,
)

if use_type in [USE_TYPE_CHART, USE_TYPE_ETL]:
    st.error(f"Use type '{use_type}' not yet implemented.")
    st.stop()
elif use_type == USE_TYPE_EXPLORERS:
    if not EXPLORERS_DIR.is_dir():
        st.error(f"Explorer directory not found: {EXPLORERS_DIR}")
        st.stop()
    # List all explorer names.
    explorer_names = [
        explorer_file.stem.replace(".explorer", "") for explorer_file in sorted(EXPLORERS_DIR.glob("*.explorer.tsv"))
    ]
    # Select an explorer name from a dropdown menu.
    explorer_name: str = st.selectbox(  # type: ignore
        label="Name of explorer file",
        options=explorer_names,
        index=[i for i, name in enumerate(explorer_names) if name == EXPLORER_NAME_DEFAULT][0],
        help="Name of .explorer.tsv file inside owid-content/explorers.",
    )
    # Define path to explorer file.
    explorer_path = (EXPLORERS_DIR / explorer_name).with_suffix(".explorer.tsv")

    # Load explorer content.
    explorer = load_explorer(explorer_path=explorer_path)
    if "yVariableIds" not in explorer:
        st.error("This tool can handle only indicator-based explorers!")
        st.stop()

    # Extract variable ids from explorer.
    variable_ids = extract_variable_ids_from_explorer_content(explorer=explorer)
    # TODO: Instead of using this function, create one that loads the main table and selects variables with a map tab.
    #  Also, exclude those that already have map brackets (and optionally show them too).
    #  We also need to figure out how to write back to the file afterwards with a given map bracket configuration.
    variable_id: int = st.selectbox(  # type: ignore
        label="Indicator id",
        options=variable_ids,
        index=0,
    )

variable = load_variable_from_id(variable_id=variable_id)  # type: ignore
df = load_variable_data(variable=variable)
regions_to_id = load_mappable_regions_and_ids(df=df)
metadata = load_variable_metadata(variable=variable)
chart_config = create_default_chart_config_for_variable(metadata=metadata)

# Select only regions that appear in grapher maps.
# And for now, focus on the latest year.
# TODO: These values are often used, sometimes as global variables. Consider creating a dictionary of important values.
values = df[(df["entities"].isin(regions_to_id.values())) & (df["years"] == df["years"].max())]["values"]
min_value = values.min()
max_value = values.max()
smallest_number = metadata["display"].get("numDecimalPlaces", SMALLEST_NUMBER_DEFAULT)


def get_all_possible_log_like_brackets(values) -> Dict[str, List[float]]:
    #  I think that's probably the case for log-like brackets that have both positive and negative values.
    #  Not sure about only positive cases. For now, assume so.

    # Find the minimum and maximum absolute nonzero values.
    values_nonzero = abs(values[abs(values) > smallest_number])
    # Find the closest power of 10 that is right below the minimum nonzero value.
    # That would be the minimum bracket possible.
    min_bracket_possible = round_to_nearest_power_of_ten(values_nonzero.min())
    # Find the closest power of 10 that is right above the maximum nonzero value.
    # That would be the maximum bracket possible.
    max_bracket_possible = round_to_nearest_power_of_ten(values_nonzero.max(), floor=False)

    # Initialize a dictionary of brackets.
    brackets_all = {}

    # Create the minimum number of brackets that would fully contain the values.
    # First, do it in powers of 10.
    brackets_x10 = 10 ** np.arange(np.log10(min_bracket_possible), np.log10(max_bracket_possible) + 1, 1)
    brackets_all[LABEL_LOG_X10] = brackets_x10

    # Now, do it following the sequence 0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 50, etc.
    brackets_all[LABEL_LOG_X2] = np.sort(np.hstack([brackets_x10, brackets_x10[:-1] * 2, brackets_x10[:-1] * 5]))

    # Now, do it following the sequence 0.1, 0.3, 1, 3, 10, 30, etc.
    brackets_all[LABEL_LOG_X3] = np.sort(np.hstack([brackets_x10, brackets_x10[:-1] * 3]))

    for bracket_type, brackets in brackets_all.items():
        if values.min() < -smallest_number:
            # If there is any negative value in the data, replicate the bracket to the left (to negative values).
            # NOTE: Instead of 0, we assume -0.001, in case there is some numeric noise.
            brackets = np.hstack([-brackets[::-1], [0], brackets])

        # Round numbers.
        brackets_all[bracket_type] = np.array([round_to_sig_figs(bracket) for bracket in brackets])  # .tolist()

    return brackets_all


def are_brackets_open(values):
    # If the minimum value in the data is negative, assume that the lower bracket is open.
    # Otherwise, the most common scenario is that the lower bracket is closed.
    lower_bracket_open = values.min() < -smallest_number
    # The upper bracket is most frequently open.
    upper_bracket_open = True

    # TODO: Consider creating some additional heuristics about the openness of the lower and upper brackets,
    # e.g. if the maximum (minimum) bracket is 100 (-100), close the upper (lower) bracket.

    return lower_bracket_open, upper_bracket_open


# Calculate the brackets for all possible log-like bracket types.
brackets_all = get_all_possible_log_like_brackets(values=values)

# Estimate whether the lower and upper brackets should be open.
lower_bracket_open_default, upper_bracket_open_default = are_brackets_open(values=values)

# Add a dropdown for color scheme.
# TODO: Add full list of color schemes.
color_scheme = st.selectbox(
    label="Color scheme (not fully implemented!)",
    options=["BuGn", "BinaryMapPaletteA"],
    help="Color scheme for the map.",
)

# Add toggles to control whether lower and upper brackets should be open.
lower_bracket_open = st.toggle("Lower bracket open", lower_bracket_open_default)
upper_bracket_open = st.toggle("Upper bracket open", upper_bracket_open_default)

bracket_type = st.radio(
    "Select linear or log-like",
    options=[LABEL_LOG_X2, LABEL_LOG_X3, LABEL_LOG_X10, LABEL_LINEAR],
    index=0,
    horizontal=True,
)
if bracket_type == LABEL_LINEAR:
    # TODO: Implement linear brackets.
    st.number_input("Increments", min_value=0, max_value=100, value=1, step=1)
    brackets = np.arange(min_value, max_value, 1).tolist()
else:
    brackets = brackets_all[bracket_type]  # type: ignore

# Create a slider for positive values.
# TODO: Maybe have a toggle to be able to manually select negative and positive values separately.
brackets_positive = brackets[brackets > 0]  # type: ignore
min_selected, max_selected = st.select_slider(  # type: ignore
    "Select a range of values:",
    options=brackets_positive,
    value=(min(brackets_positive), max(brackets_positive)),  # default range
)
# Filter the list to get the selected values
brackets_selected = [bracket for bracket in brackets if min_selected <= abs(bracket) <= max_selected or bracket == 0]
if len(brackets_selected) > MAX_NUM_BRACKETS:
    if min_value < -smallest_number:
        st.warning(
            f"WARNING: {len(brackets_selected)} is too many brackets. Taking only the first {MAX_NUM_BRACKETS} around zero."
        )
        brackets_selected = brackets_selected[
            int(len(brackets_selected) / 2) - int(MAX_NUM_BRACKETS / 2) : int(len(brackets_selected) / 2)
            + int(MAX_NUM_BRACKETS / 2)
        ]
    else:
        st.warning(f"WARNING: {len(brackets_selected)} is too many brackets. Taking only the first {MAX_NUM_BRACKETS}.")
        brackets_selected = brackets_selected[:MAX_NUM_BRACKETS]
elif len(brackets_selected) < MIN_NUM_BRACKETS:
    st.warning(f"WARNING: {len(brackets_selected)} is too few brackets. Increase the limits.")

# brackets = np.linspace(start=lower_limit, stop=upper_limit, num=10).tolist()
chart_config["map"]["colorScale"]["baseColorScheme"] = color_scheme
chart_config["map"]["colorScale"]["customNumericValues"] = brackets_selected
if lower_bracket_open:
    # To ensure the lower bracket is open, use a large value of "customNumericMinValue".
    chart_config["map"]["colorScale"]["customNumericMinValue"] = max_value
if upper_bracket_open:
    # To ensure the upper bracket is open, add an upper bracket with a value that is very small.
    chart_config["map"]["colorScale"]["customNumericValues"].append(min_value)
else:
    # To ensure the upper bracket is closed, ensure the upper bracket value is larger than any data value.
    if chart_config["map"]["colorScale"]["customNumericValues"][-1] < max_value:
        # Find the lowest bracket that is above the maximum value in the data, and use that as the upper bracket.
        chart_config["map"]["colorScale"]["customNumericValues"][-1] = brackets[brackets > max_value].min()

# Display the chart.
chart_html(chart_config, owid_env=OWID_ENV)
